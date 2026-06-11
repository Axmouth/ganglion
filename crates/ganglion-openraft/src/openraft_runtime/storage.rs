//! In-memory `RaftLogStorage` / `RaftStateMachine` implementations for
//! [`GanglionRaftConfig`], validated against openraft's storage contract suite.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

use ganglion_core::CoordinationSnapshot;
use tokio::sync::watch;

use openraft::async_trait::async_trait;
use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, LogState, RaftLogReader, RaftSnapshotBuilder, Snapshot,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};

use super::{GanglionRaftConfig, MetadataRaftCommand, MetadataRaftResponse};

type NodeId = u64;

/// Shared in-memory raft log + vote store.
#[derive(Debug, Clone, Default)]
pub struct GanglionLogStore {
    inner: Arc<Mutex<LogStoreInner>>,
}

#[derive(Debug, Default)]
struct LogStoreInner {
    vote: Option<Vote<NodeId>>,
    log: BTreeMap<u64, Entry<GanglionRaftConfig>>,
    last_purged_log_id: Option<LogId<NodeId>>,
}

#[async_trait]
impl RaftLogReader<GanglionRaftConfig> for GanglionLogStore {
    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<GanglionRaftConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap();
        let last_purged_log_id = inner.last_purged_log_id;
        let last_log_id = inner
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
        Ok(inner.log.range(range).map(|(_, entry)| entry.clone()).collect())
    }
}

#[async_trait]
impl RaftLogStorage<GanglionRaftConfig> for GanglionLogStore {
    type LogReader = Self;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().unwrap().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().unwrap().vote)
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
        {
            let mut inner = self.inner.lock().unwrap();
            for entry in entries {
                inner.log.insert(entry.log_id.index, entry);
            }
        }
        // The in-memory store is "persisted" as soon as the map insert lands.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().unwrap();
        // Keep entries strictly before `log_id.index`.
        inner.log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().unwrap();
        inner.last_purged_log_id = Some(log_id);
        let retained = inner.log.split_off(&(log_id.index + 1));
        inner.log = retained;
        Ok(())
    }
}

/// Shared in-memory state machine holding the committed [`CoordinationSnapshot`].
///
/// Every committed change is also published on a `watch` channel so sync
/// consumers (fibril's `Coordination::watch()` shape) can observe the
/// committed state without touching raft.
#[derive(Debug, Clone)]
pub struct GanglionStateMachine {
    inner: Arc<Mutex<StateMachineInner>>,
    committed_tx: Arc<watch::Sender<CoordinationSnapshot>>,
}

impl Default for GanglionStateMachine {
    fn default() -> Self {
        let (committed_tx, _rx) = watch::channel(CoordinationSnapshot::default());
        Self {
            inner: Arc::default(),
            committed_tx: Arc::new(committed_tx),
        }
    }
}

#[derive(Debug, Default)]
struct StateMachineInner {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    state: CoordinationSnapshot,
    snapshot_idx: u64,
    current_snapshot: Option<StoredSnapshot>,
}

#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

impl GanglionStateMachine {
    /// Current committed coordination snapshot.
    pub fn committed_snapshot(&self) -> CoordinationSnapshot {
        self.inner.lock().unwrap().state.clone()
    }

    /// Last applied raft log id, if any.
    pub fn last_applied(&self) -> Option<LogId<NodeId>> {
        self.inner.lock().unwrap().last_applied
    }

    /// Subscribe to committed snapshot updates.
    ///
    /// The receiver always holds the latest committed snapshot; only accepted
    /// state changes are published (rejected stale writes are not).
    pub fn watch_committed(&self) -> watch::Receiver<CoordinationSnapshot> {
        self.committed_tx.subscribe()
    }
}

#[async_trait]
impl RaftSnapshotBuilder<GanglionRaftConfig> for GanglionStateMachine {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<GanglionRaftConfig>, StorageError<NodeId>> {
        let mut inner = self.inner.lock().unwrap();
        let data = serde_json::to_vec(&inner.state)
            .map_err(|error| StorageIOError::read_state_machine(&error))?;

        inner.snapshot_idx += 1;
        let snapshot_id = match inner.last_applied {
            Some(last) => format!("{}-{}-{}", last.leader_id, last.index, inner.snapshot_idx),
            None => format!("--{}", inner.snapshot_idx),
        };
        let meta = SnapshotMeta {
            last_log_id: inner.last_applied,
            last_membership: inner.last_membership.clone(),
            snapshot_id,
        };
        inner.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[async_trait]
impl RaftStateMachine<GanglionRaftConfig> for GanglionStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let inner = self.inner.lock().unwrap();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<MetadataRaftResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<GanglionRaftConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut inner = self.inner.lock().unwrap();
        let mut replies = Vec::new();
        let mut state_changed = false;

        for entry in entries {
            inner.last_applied = Some(entry.log_id);
            let accepted = match entry.payload {
                EntryPayload::Blank => true,
                EntryPayload::Normal(MetadataRaftCommand::ApplySnapshot(snapshot)) => {
                    // Deterministic stale-generation rejection: every replica
                    // sees the same command order, so this check is replicated
                    // state-machine safe.
                    if snapshot.generation < inner.state.generation {
                        false
                    } else {
                        inner.state = snapshot;
                        state_changed = true;
                        true
                    }
                }
                EntryPayload::Membership(membership) => {
                    inner.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    true
                }
            };
            replies.push(MetadataRaftResponse {
                accepted,
                snapshot: inner.state.clone(),
            });
        }

        if state_changed {
            self.committed_tx.send_replace(inner.state.clone());
        }

        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let state: CoordinationSnapshot = if data.is_empty() {
            CoordinationSnapshot::default()
        } else {
            serde_json::from_slice(&data).map_err(|error| {
                StorageIOError::read_snapshot(Some(meta.signature()), &error)
            })?
        };

        let mut inner = self.inner.lock().unwrap();
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.state = state;
        inner.current_snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
        self.committed_tx.send_replace(inner.state.clone());
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<GanglionRaftConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.current_snapshot.as_ref().map(|stored| Snapshot {
            meta: stored.meta.clone(),
            snapshot: Box::new(Cursor::new(stored.data.clone())),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::testing::{StoreBuilder, Suite};

    struct InMemoryBuilder;

    #[async_trait]
    impl StoreBuilder<GanglionRaftConfig, GanglionLogStore, GanglionStateMachine, ()>
        for InMemoryBuilder
    {
        async fn build(
            &self,
        ) -> Result<((), GanglionLogStore, GanglionStateMachine), StorageError<NodeId>> {
            Ok(((), GanglionLogStore::default(), GanglionStateMachine::default()))
        }
    }

    #[test]
    fn openraft_storage_contract_suite() -> Result<(), StorageError<NodeId>> {
        Suite::test_all(InMemoryBuilder)
    }

    #[test]
    fn state_machine_rejects_stale_generation_deterministically() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let mut sm = GanglionStateMachine::default();

            let fresh = CoordinationSnapshot {
                generation: 3,
                ..CoordinationSnapshot::default()
            };
            let stale = CoordinationSnapshot {
                generation: 2,
                ..CoordinationSnapshot::default()
            };

            let make_entry = |index: u64, snapshot: CoordinationSnapshot| Entry::<
                GanglionRaftConfig,
            > {
                log_id: LogId::new(openraft::CommittedLeaderId::new(1, 0), index),
                payload: EntryPayload::Normal(MetadataRaftCommand::ApplySnapshot(snapshot)),
            };

            let replies = sm
                .apply(vec![make_entry(1, fresh.clone()), make_entry(2, stale)])
                .await
                .expect("apply should succeed");

            assert!(replies[0].accepted);
            assert!(!replies[1].accepted, "stale generation must be rejected");
            assert_eq!(replies[1].snapshot.generation, 3);
            assert_eq!(sm.committed_snapshot(), fresh);
            assert_eq!(sm.last_applied().map(|id| id.index), Some(2));
        });
    }
}
