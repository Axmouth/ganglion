//! Reproduce an empty replication read caused by leader step-down.
//!
//! OpenRaft runs each replication stream as its own task. When a leader
//! learns of a newer leader, it can truncate its uncommitted tail before an
//! old stream reads the range it was told to send. The read then returns no
//! entries for a nonempty range. A gate in the log reader holds the old read
//! open so the interleaving is deterministic.
//!
//! In stock OpenRaft 0.9.25 the empty read becomes a heartbeat whose
//! progress report carries the retired session and is dropped by RaftCore,
//! so the progress debug assertion is not reached on this path. The old
//! stream repeats that heartbeat every 10 ms while the peer still accepts the
//! old vote. In 0.9.24 the same read panics the replication task. The test
//! runs in a subprocess to catch background panics that would not fail the
//! caller.
use super::*;
use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{CommittedLeaderId, Entry, LogId, LogState, RaftLogReader, StorageError, Vote};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::{collections::BTreeMap, fmt::Debug, ops::RangeBounds, sync::Arc, time::Duration};

#[derive(Debug, Clone, Copy)]
struct EmptyRead {
    start: u64,
    end: u64,
    truncates_before: usize,
    purges_before: usize,
}

#[derive(Default)]
struct ReadGate {
    armed: AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    held_range: Mutex<Option<(u64, u64)>>,
    truncates: AtomicUsize,
    purges: AtomicUsize,
    empty_reads: Mutex<Vec<EmptyRead>>,
}

/// Log store that can hold one replication read open and records reads that
/// return nothing for a nonempty range.
#[derive(Clone, Default)]
struct GatedReads {
    inner: GanglionLogStore,
    gate: Arc<ReadGate>,
}

impl RaftLogReader<GanglionRaftConfig> for GatedReads {
    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + Debug + openraft::OptionalSend>(
        &mut self,
        range: R,
    ) -> Result<Vec<Entry<GanglionRaftConfig>>, StorageError<u64>> {
        self.inner.try_get_log_entries(range).await
    }

    // Only replication streams call this, so the gate never stalls RaftCore.
    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<Entry<GanglionRaftConfig>>, StorageError<u64>> {
        if start < end && self.gate.armed.swap(false, Ordering::SeqCst) {
            *self.gate.held_range.lock().unwrap() = Some((start, end));
            self.gate.entered.notify_one();
            self.gate.release.notified().await;
        }
        let entries = self.inner.try_get_log_entries(start..end).await?;
        if start < end && entries.is_empty() {
            self.gate.empty_reads.lock().unwrap().push(EmptyRead {
                start,
                end,
                truncates_before: self.gate.truncates.load(Ordering::SeqCst),
                purges_before: self.gate.purges.load(Ordering::SeqCst),
            });
        }
        Ok(entries)
    }
}

impl RaftLogStorage<GanglionRaftConfig> for GatedReads {
    type LogReader = Self;
    async fn get_log_state(&mut self) -> Result<LogState<GanglionRaftConfig>, StorageError<u64>> {
        self.inner.get_log_state().await
    }
    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }
    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.inner.save_vote(vote).await
    }
    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        self.inner.read_vote().await
    }
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<GanglionRaftConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<GanglionRaftConfig>> + Send,
        I::IntoIter: Send,
    {
        self.inner.append(entries, callback).await
    }
    async fn truncate(&mut self, id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.inner.truncate(id).await?;
        self.gate.truncates.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn purge(&mut self, id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.inner.purge(id).await?;
        self.gate.purges.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn step_down_truncate_empties_an_old_replication_read() {
    const CHILD: &str = "GANGLION_STEP_DOWN_READ_RACE_CHILD";
    const MARKER: &str = "old replication stream read an empty truncated range";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "openraft_runtime::step_down_read_race::step_down_truncate_empties_an_old_replication_read",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success() && !stderr.contains("panicked at"),
            "{stdout}\n{stderr}"
        );
        assert!(stdout.contains(MARKER), "{stdout}\n{stderr}");
        return;
    }
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(20), async {
                let router = InProcessRouter::new();
                let store = GatedReads::default();
                let gate = store.gate.clone();
                let mut inspect = store.inner.clone();
                let leader = RaftMetadataNode::start_with_storage(
                    1,
                    default_raft_config().unwrap(),
                    &router,
                    store,
                    GanglionStateMachine::default(),
                )
                .await
                .unwrap();
                let follower = RaftMetadataNode::start(2, default_raft_config().unwrap(), &router)
                    .await
                    .unwrap();
                leader
                    .initialize(BTreeMap::from([(1, openraft::BasicNode::new("one"))]))
                    .await
                    .unwrap();
                leader
                    .wait_for_any_leader(Duration::from_secs(5))
                    .await
                    .unwrap();
                leader
                    .add_learner(2, openraft::BasicNode::new("two"), true)
                    .await
                    .unwrap();
                leader.change_membership([1, 2], false).await.unwrap();
                // Both nodes stay put so only the injected leader change happens.
                follower.raft().runtime_config().elect(false);
                leader
                    .write_snapshot(CoordinationSnapshot {
                        generation: 1,
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                while follower.committed_snapshot().generation != 1 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }

                // Hold the next data read of the stream to node 2. The write
                // stays uncommitted because node 2 is needed for a quorum.
                gate.armed.store(true, Ordering::SeqCst);
                let pending = {
                    let raft = leader.raft().clone();
                    tokio::spawn(async move {
                        raft.client_write(MetadataRaftCommand::ApplySnapshot(
                            CoordinationSnapshot {
                                generation: 2,
                                ..Default::default()
                            },
                        ))
                        .await
                    })
                };
                gate.entered.notified().await;
                let (start, end) = gate.held_range.lock().unwrap().unwrap();
                let tail = inspect.get_log_state().await.unwrap().last_log_id.unwrap();
                assert!(
                    start <= tail.index && tail.index < end,
                    "{start}..{end} {tail}"
                );

                // A newer leader probes with its own entry at the same index.
                // Node 1 steps down and truncates the conflicting tail while
                // the old stream is still waiting to read it.
                leader.raft().runtime_config().elect(false);
                let term = inspect.read_vote().await.unwrap().unwrap().leader_id().term + 1;
                let response = leader
                    .raft()
                    .append_entries(openraft::raft::AppendEntriesRequest {
                        vote: Vote::new_committed(term, 2),
                        prev_log_id: Some(LogId::new(CommittedLeaderId::new(term, 2), tail.index)),
                        entries: vec![],
                        leader_commit: None,
                    })
                    .await
                    .unwrap();
                assert!(
                    matches!(response, openraft::raft::AppendEntriesResponse::Conflict),
                    "{response:?}"
                );
                assert_eq!(gate.truncates.load(Ordering::SeqCst), 1);
                assert!(inspect
                    .try_get_log_entries(start..end)
                    .await
                    .unwrap()
                    .is_empty());

                gate.release.notify_one();
                while gate.empty_reads.lock().unwrap().is_empty() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                let empty = gate.empty_reads.lock().unwrap()[0];
                assert_eq!((empty.start, empty.end), (start, end));
                assert_eq!(empty.truncates_before, 1);
                assert_eq!(empty.purges_before, 0);
                println!("{MARKER}: {empty:?}");

                // Give the old stream time to report on the retired session.
                tokio::time::sleep(Duration::from_millis(200)).await;
                assert!(pending.await.unwrap().is_err());
                assert!(leader.raft().current_leader().await != Some(1));
                tokio::time::timeout(Duration::from_secs(2), leader.shutdown())
                    .await
                    .unwrap()
                    .unwrap();
                follower.shutdown().await.unwrap();
            })
            .await
            .unwrap();
        });
}
