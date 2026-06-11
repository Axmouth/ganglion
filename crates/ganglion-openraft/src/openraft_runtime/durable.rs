//! File-backed `RaftLogStorage` for [`GanglionRaftConfig`].
//!
//! A JSON-lines WAL holds vote, entry, truncate, and purge records. The full
//! file is folded into memory on open; appends are fsynced before the raft
//! flush callback fires. Purge compacts the WAL by rewriting it (purge is rare
//! and the metadata log is small). The state machine stays in-memory: openraft
//! re-commits/re-applies surviving log entries after restart, and snapshot
//! transfer covers nodes whose logs were purged.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use openraft::async_trait::async_trait;
use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{Entry, LogId, LogState, RaftLogReader, StorageError, StorageIOError, Vote};
use serde::{Deserialize, Serialize};

use super::{GanglionRaftConfig, StorageTelemetry, StorageTelemetrySnapshot};

type NodeId = u64;

#[derive(Serialize, Deserialize)]
enum WalRecord {
    Vote(Vote<NodeId>),
    Entry(Entry<GanglionRaftConfig>),
    /// Remove entries with `index >= since`.
    Truncate {
        since: u64,
    },
    /// Remove entries with `index <= upto.index` and remember the purge point.
    Purge {
        upto: LogId<NodeId>,
    },
}

#[derive(Debug, Default)]
struct DurableState {
    vote: Option<Vote<NodeId>>,
    log: BTreeMap<u64, Entry<GanglionRaftConfig>>,
    last_purged_log_id: Option<LogId<NodeId>>,
}

impl DurableState {
    fn apply_record(&mut self, record: WalRecord) {
        match record {
            WalRecord::Vote(vote) => self.vote = Some(vote),
            WalRecord::Entry(entry) => {
                self.log.insert(entry.log_id.index, entry);
            }
            WalRecord::Truncate { since } => {
                self.log.split_off(&since);
            }
            WalRecord::Purge { upto } => {
                // Mirrors the runtime guard: purge points never move backwards.
                if self
                    .last_purged_log_id
                    .is_none_or(|purged| upto.index > purged.index)
                {
                    self.last_purged_log_id = Some(upto);
                }
                let retained = self.log.split_off(&(upto.index + 1));
                self.log = retained;
            }
        }
    }
}

struct DurableInner {
    state: DurableState,
    file: File,
}

/// Durable raft log + vote store persisted as a JSON-lines WAL.
#[derive(Clone)]
pub struct FileRaftLogStore {
    path: PathBuf,
    inner: Arc<Mutex<DurableInner>>,
    telemetry: Arc<StorageTelemetry>,
}

impl FileRaftLogStore {
    /// Dead WAL records tolerated before an open-time compaction rewrite.
    const COMPACT_DEAD_RECORD_THRESHOLD: usize = 64;

    /// Open (or create) the WAL at `path` and replay it. Replay is strict:
    /// malformed records fail startup rather than risking divergent raft state.
    ///
    /// If replay encounters substantially more records than the live state
    /// retains (superseded votes, truncated/purged entries), the WAL is
    /// compacted immediately so the next startup replays a bounded file.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StorageError<NodeId>> {
        let path = path.into();
        let mut state = DurableState::default();
        let mut replayed_records = 0usize;

        if path.exists() {
            let file = File::open(&path).map_err(|error| StorageIOError::read_logs(&error))?;
            for (line_no, line) in BufReader::new(file).lines().enumerate() {
                let line = line.map_err(|error| StorageIOError::read_logs(&error))?;
                if line.trim().is_empty() {
                    continue;
                }
                let record: WalRecord = serde_json::from_str(&line).map_err(|error| {
                    StorageIOError::read_logs(&std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("malformed WAL record at line {}: {error}", line_no + 1),
                    ))
                })?;
                state.apply_record(record);
                replayed_records += 1;
            }
        }

        let created = !path.exists();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|error| StorageIOError::write_logs(&error))?;
        if created {
            // Make the new WAL's directory entry durable.
            super::fsync_parent_dir(&path).map_err(|error| StorageIOError::write_logs(&error))?;
        }

        let live_records = state.log.len()
            + usize::from(state.vote.is_some())
            + usize::from(state.last_purged_log_id.is_some());
        let needs_compaction =
            replayed_records > live_records + Self::COMPACT_DEAD_RECORD_THRESHOLD;

        let telemetry = Arc::new(StorageTelemetry::default());
        telemetry.replayed_records_last_open.store(
            replayed_records as u64,
            std::sync::atomic::Ordering::Relaxed,
        );

        let store = Self {
            path,
            inner: Arc::new(Mutex::new(DurableInner { state, file })),
            telemetry,
        };
        if needs_compaction {
            let mut inner = store.inner.lock().unwrap();
            store.rewrite(&mut inner)?;
        }
        Ok(store)
    }

    fn write_record(
        inner: &mut DurableInner,
        record: &WalRecord,
    ) -> Result<(), StorageError<NodeId>> {
        let mut line =
            serde_json::to_vec(record).map_err(|error| StorageIOError::write_logs(&error))?;
        line.push(b'\n');
        inner
            .file
            .write_all(&line)
            .and_then(|()| inner.file.sync_data())
            .map_err(|error| StorageIOError::write_logs(&error))?;
        Ok(())
    }

    /// Counter handle shared with `RaftMetadataNode::telemetry`.
    pub fn telemetry_handle(&self) -> Arc<StorageTelemetry> {
        Arc::clone(&self.telemetry)
    }

    /// Point-in-time durability counters.
    pub fn telemetry(&self) -> StorageTelemetrySnapshot {
        self.telemetry.snapshot()
    }

    /// Rewrite the WAL compacted to current state (used after purge).
    fn rewrite(&self, inner: &mut DurableInner) -> Result<(), StorageError<NodeId>> {
        let tmp_path = self.path.with_extension("tmp");
        let mut tmp =
            File::create(&tmp_path).map_err(|error| StorageIOError::write_logs(&error))?;

        let mut write_line = |record: &WalRecord| -> Result<(), StorageError<NodeId>> {
            let mut line =
                serde_json::to_vec(record).map_err(|error| StorageIOError::write_logs(&error))?;
            line.push(b'\n');
            tmp.write_all(&line)
                .map_err(|error| StorageIOError::write_logs(&error))?;
            Ok(())
        };

        if let Some(vote) = inner.state.vote {
            write_line(&WalRecord::Vote(vote))?;
        }
        if let Some(upto) = inner.state.last_purged_log_id {
            write_line(&WalRecord::Purge { upto })?;
        }
        for entry in inner.state.log.values() {
            write_line(&WalRecord::Entry(entry.clone()))?;
        }
        tmp.sync_data()
            .map_err(|error| StorageIOError::write_logs(&error))?;
        drop(tmp);

        std::fs::rename(&tmp_path, &self.path)
            .map_err(|error| StorageIOError::write_logs(&error))?;
        super::fsync_parent_dir(&self.path).map_err(|error| StorageIOError::write_logs(&error))?;
        inner.file = OpenOptions::new()
            .append(true)
            .open(&self.path)
            .map_err(|error| StorageIOError::write_logs(&error))?;
        use std::sync::atomic::Ordering::Relaxed;
        self.telemetry.compactions.fetch_add(1, Relaxed);
        self.telemetry.fsyncs.fetch_add(2, Relaxed); // tmp data + parent dir
        Ok(())
    }

    /// Persist and index a batch of entries with a single fsync
    /// (shared by the trait impl and tests).
    fn append_entries(
        &self,
        entries: impl IntoIterator<Item = Entry<GanglionRaftConfig>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().unwrap();
        let mut batch = Vec::new();
        let mut staged = Vec::new();
        for entry in entries {
            let line = serde_json::to_vec(&WalRecord::Entry(entry.clone()))
                .map_err(|error| StorageIOError::write_logs(&error))?;
            batch.extend_from_slice(&line);
            batch.push(b'\n');
            staged.push(entry);
        }
        if staged.is_empty() {
            return Ok(());
        }
        inner
            .file
            .write_all(&batch)
            .and_then(|()| inner.file.sync_data())
            .map_err(|error| StorageIOError::write_logs(&error))?;
        use std::sync::atomic::Ordering::Relaxed;
        self.telemetry
            .appended_records
            .fetch_add(staged.len() as u64, Relaxed);
        self.telemetry.appended_batches.fetch_add(1, Relaxed);
        self.telemetry.fsyncs.fetch_add(1, Relaxed);
        for entry in staged {
            inner.state.log.insert(entry.log_id.index, entry);
        }
        Ok(())
    }
}

#[async_trait]
impl RaftLogReader<GanglionRaftConfig> for FileRaftLogStore {
    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<GanglionRaftConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap();
        let last_purged_log_id = inner.state.last_purged_log_id;
        let last_log_id = inner
            .state
            .log
            .values()
            .next_back()
            .map(|entry| entry.log_id)
            .or(last_purged_log_id);
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send + Sync>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<GanglionRaftConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .state
            .log
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

#[async_trait]
impl RaftLogStorage<GanglionRaftConfig> for FileRaftLogStore {
    type LogReader = Self;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().unwrap();
        Self::write_record(&mut inner, &WalRecord::Vote(*vote))?;
        self.telemetry
            .fsyncs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        inner.state.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().unwrap().state.vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<NodeId>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<GanglionRaftConfig>> + Send,
        I::IntoIter: Send,
    {
        match self.append_entries(entries) {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(error) => {
                callback.log_io_completed(Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    error.to_string(),
                )));
                Err(error)
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().unwrap();
        Self::write_record(
            &mut inner,
            &WalRecord::Truncate {
                since: log_id.index,
            },
        )?;
        inner.state.log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().unwrap();
        // Purge points never move backwards.
        if inner
            .state
            .last_purged_log_id
            .is_none_or(|purged| log_id.index > purged.index)
        {
            inner.state.last_purged_log_id = Some(log_id);
        }
        let retained = inner.state.log.split_off(&(log_id.index + 1));
        inner.state.log = retained;
        self.rewrite(&mut inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::testing::{StoreBuilder, Suite};

    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_wal_path(tag: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "ganglion-raft-wal-{tag}-{}-{nanos}-{unique}.jsonl",
            std::process::id()
        ))
    }

    struct FileBuilder;

    #[async_trait]
    impl StoreBuilder<GanglionRaftConfig, FileRaftLogStore, super::super::GanglionStateMachine, ()>
        for FileBuilder
    {
        async fn build(
            &self,
        ) -> Result<((), FileRaftLogStore, super::super::GanglionStateMachine), StorageError<NodeId>>
        {
            let store = FileRaftLogStore::open(unique_wal_path("suite"))?;
            Ok(((), store, super::super::GanglionStateMachine::default()))
        }
    }

    #[test]
    fn file_store_passes_openraft_contract_suite() -> Result<(), StorageError<NodeId>> {
        Suite::test_all(FileBuilder)
    }

    #[test]
    fn file_store_survives_reopen_with_vote_log_and_purge() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        rt.block_on(async {
            use super::super::MetadataRaftCommand;
            use ganglion_core::CoordinationSnapshot;
            use openraft::{CommittedLeaderId, EntryPayload};

            let path = unique_wal_path("reopen");
            let leader = CommittedLeaderId::new(1, 0);
            let make_entry = |index: u64, generation: u64| Entry::<GanglionRaftConfig> {
                log_id: LogId::new(leader, index),
                payload: EntryPayload::Normal(MetadataRaftCommand::ApplySnapshot(
                    CoordinationSnapshot {
                        generation,
                        ..CoordinationSnapshot::default()
                    },
                )),
            };

            {
                let mut store = FileRaftLogStore::open(&path).expect("open fresh");
                store
                    .save_vote(&Vote::new(1, 7))
                    .await
                    .expect("vote should persist");

                store
                    .append_entries((1..=5).map(|index| make_entry(index, index)))
                    .expect("append should persist");

                store
                    .truncate(LogId::new(leader, 5))
                    .await
                    .expect("truncate should persist");
                store
                    .purge(LogId::new(leader, 2))
                    .await
                    .expect("purge should compact");
            }

            let mut reopened = FileRaftLogStore::open(&path).expect("reopen");
            assert_eq!(
                reopened.read_vote().await.expect("read vote"),
                Some(Vote::new(1, 7))
            );

            let log_state = reopened.get_log_state().await.expect("log state");
            assert_eq!(log_state.last_purged_log_id, Some(LogId::new(leader, 2)));
            assert_eq!(log_state.last_log_id, Some(LogId::new(leader, 4)));

            let entries = reopened
                .try_get_log_entries(..)
                .await
                .expect("entries readable");
            let indexes: Vec<u64> = entries.iter().map(|entry| entry.log_id.index).collect();
            assert_eq!(indexes, vec![3, 4]);
        });
    }

    /// Pins the v1 (pre-guarded-command) WAL record encoding: WALs written
    /// before `ApplySnapshotGuarded` existed must keep replaying. If this test
    /// breaks, the WAL format changed incompatibly — that needs a migration,
    /// not a fixture update.
    #[test]
    fn file_store_replays_pre_guarded_format_wal() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let path = unique_wal_path("v1-fixture");
            let fixture = concat!(
                r#"{"Vote":{"leader_id":{"term":1,"node_id":7},"committed":false}}"#,
                "\n",
                r#"{"Entry":{"log_id":{"leader_id":{"term":1,"node_id":0},"index":1},"payload":{"Normal":{"ApplySnapshot":{"nodes":{},"assignments":{},"generation":3}}}}}"#,
                "\n",
            );
            std::fs::write(&path, fixture).expect("write fixture WAL");

            let mut store = FileRaftLogStore::open(&path).expect("v1 WAL must replay");
            assert_eq!(
                store.read_vote().await.expect("vote"),
                Some(Vote::new(1, 7))
            );
            let entries = store.try_get_log_entries(..).await.expect("entries");
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].log_id.index, 1);
        });
    }

    #[test]
    fn file_store_rejects_malformed_wal() {
        let path = unique_wal_path("malformed");
        std::fs::write(&path, b"{not-a-record}\n").expect("write bad WAL");
        let result = FileRaftLogStore::open(&path);
        assert!(result.is_err(), "malformed WAL must fail strict replay");
    }

    mod fuzz {
        use super::*;
        use proptest::prelude::*;

        #[derive(Debug, Clone)]
        enum WalOp {
            /// Append `count` entries continuing from the model's next index.
            Append {
                count: u8,
            },
            /// Truncate at `last_index - back` (skipped when the log is empty).
            Truncate {
                back: u8,
            },
            /// Purge up to `last_index - back` (skipped when the log is empty).
            Purge {
                back: u8,
            },
            Vote {
                term: u64,
            },
        }

        fn wal_op() -> impl Strategy<Value = WalOp> {
            prop_oneof![
                4 => (1u8..6).prop_map(|count| WalOp::Append { count }),
                1 => (0u8..4).prop_map(|back| WalOp::Truncate { back }),
                1 => (0u8..4).prop_map(|back| WalOp::Purge { back }),
                1 => (1u64..6).prop_map(|term| WalOp::Vote { term }),
            ]
        }

        fn make_entry(index: u64) -> Entry<GanglionRaftConfig> {
            use super::super::super::MetadataRaftCommand;
            use ganglion_core::CoordinationSnapshot;
            use openraft::{CommittedLeaderId, EntryPayload};
            Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 0), index),
                payload: EntryPayload::Normal(MetadataRaftCommand::ApplySnapshot(
                    CoordinationSnapshot {
                        generation: index,
                        ..CoordinationSnapshot::default()
                    },
                )),
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(48))]

            /// Any interleaving of append/truncate/purge/vote must survive a
            /// reopen: the reopened store equals the in-memory model.
            #[test]
            fn fuzz_wal_reopen_matches_model(ops in proptest::collection::vec(wal_op(), 1..25)) {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .expect("runtime");
                rt.block_on(async {
                    let path = unique_wal_path("fuzz");
                    let mut store = FileRaftLogStore::open(&path).expect("open");
                    let leader = openraft::CommittedLeaderId::new(1, 0);

                    // Model state.
                    let mut model_log: Vec<u64> = Vec::new();
                    let mut model_vote: Option<Vote<NodeId>> = None;
                    let mut model_purged: Option<u64> = None;
                    let mut next_index = 1u64;
                    let mut model_appended = 0u64;
                    let mut model_batches = 0u64;
                    let mut model_compactions = 0u64;

                    for op in &ops {
                        match op {
                            WalOp::Append { count } => {
                                let entries: Vec<_> = (0..*count)
                                    .map(|_| {
                                        let entry = make_entry(next_index);
                                        model_log.push(next_index);
                                        next_index += 1;
                                        entry
                                    })
                                    .collect();
                                model_appended += entries.len() as u64;
                                model_batches += 1;
                                store.append_entries(entries).expect("append");
                            }
                            WalOp::Truncate { back } => {
                                if let Some(last) = model_log.last().copied() {
                                    let since = last.saturating_sub(*back as u64).max(1);
                                    store
                                        .truncate(LogId::new(leader, since))
                                        .await
                                        .expect("truncate");
                                    model_log.retain(|index| *index < since);
                                }
                            }
                            WalOp::Purge { back } => {
                                if let Some(last) = model_log.last().copied() {
                                    let upto = last.saturating_sub(*back as u64).max(1);
                                    store
                                        .purge(LogId::new(leader, upto))
                                        .await
                                        .expect("purge");
                                    model_compactions += 1;
                                    model_log.retain(|index| *index > upto);
                                    model_purged =
                                        Some(model_purged.unwrap_or(0).max(upto));
                                }
                            }
                            WalOp::Vote { term } => {
                                let vote = Vote::new(*term, 1);
                                store.save_vote(&vote).await.expect("vote");
                                model_vote = Some(vote);
                            }
                        }
                    }
                    // Telemetry must match the model exactly.
                    let telemetry = store.telemetry();
                    prop_assert_eq!(telemetry.appended_records, model_appended);
                    prop_assert_eq!(telemetry.appended_batches, model_batches);
                    prop_assert_eq!(telemetry.compactions, model_compactions);
                    drop(store);

                    let mut reopened = FileRaftLogStore::open(&path).expect("reopen");
                    prop_assert_eq!(
                        reopened.read_vote().await.expect("read vote"),
                        model_vote
                    );

                    let state = reopened.get_log_state().await.expect("log state");
                    let expected_purged =
                        model_purged.map(|index| LogId::new(leader, index));
                    let expected_last = model_log
                        .last()
                        .map(|index| LogId::new(leader, *index))
                        .or(expected_purged);
                    prop_assert_eq!(state.last_purged_log_id, expected_purged);
                    prop_assert_eq!(state.last_log_id, expected_last);

                    let entries = reopened
                        .try_get_log_entries(..)
                        .await
                        .expect("read entries");
                    let indexes: Vec<u64> =
                        entries.iter().map(|entry| entry.log_id.index).collect();
                    prop_assert_eq!(indexes, model_log);
                    Ok(())
                })?;
            }
        }
    }
}
