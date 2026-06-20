//! In-memory `RaftLogStorage` / `RaftStateMachine` implementations for
//! [`GanglionRaftConfig`], validated against openraft's storage contract suite.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

use ganglion_core::CoordinationSnapshot;
use tokio::sync::watch;

use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, LogState, RaftLogReader, RaftSnapshotBuilder, Snapshot,
    SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};

use super::{
    GanglionRaftConfig, MetadataRaftCommand, MetadataRaftResponse, MetadataRejection,
    StorageTelemetry, StorageTelemetrySnapshot,
};

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

impl RaftLogReader<GanglionRaftConfig> for GanglionLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + openraft::OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<GanglionRaftConfig>>, StorageError<NodeId>> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(inner
            .log
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftLogStorage<GanglionRaftConfig> for GanglionLogStore {
    type LogReader = Self;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<GanglionRaftConfig>, StorageError<NodeId>> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<GanglionRaftConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<GanglionRaftConfig>> + Send,
        I::IntoIter: Send,
    {
        {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for entry in entries {
                inner.log.insert(entry.log_id.index, entry);
            }
        }
        // The in-memory store is "persisted" as soon as the map insert lands.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Keep entries strictly before `log_id.index`.
        inner.log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Purge points never move backwards.
        if inner
            .last_purged_log_id
            .is_none_or(|purged| log_id.index > purged.index)
        {
            inner.last_purged_log_id = Some(log_id);
        }
        let retained = inner.log.split_off(&(log_id.index + 1));
        inner.log = retained;
        Ok(())
    }
}

/// Shared state machine holding the committed [`CoordinationSnapshot`].
///
/// Every committed change is also published on a `watch` channel so sync
/// consumers (fibril's `Coordination::watch()` shape) can observe the
/// committed state without touching raft.
///
/// With [`GanglionStateMachine::persistent`], built/installed snapshots are
/// also written to disk, which bounds recovery: a restarted node loads the
/// snapshot and only re-applies the short log tail behind it, and state
/// survives log purges across full-cluster restarts.
#[derive(Debug, Clone)]
pub struct GanglionStateMachine {
    inner: Arc<Mutex<StateMachineInner>>,
    committed_tx: Arc<watch::Sender<CoordinationSnapshot>>,
    snapshot_path: Option<Arc<std::path::PathBuf>>,
    telemetry: Arc<StorageTelemetry>,
}

impl Default for GanglionStateMachine {
    fn default() -> Self {
        let (committed_tx, _rx) = watch::channel(CoordinationSnapshot::default());
        Self {
            inner: Arc::default(),
            committed_tx: Arc::new(committed_tx),
            snapshot_path: None,
            telemetry: Arc::default(),
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    data: Vec<u8>,
}

impl GanglionStateMachine {
    /// Open a state machine that persists snapshots to `path`.
    ///
    /// If the file exists, committed state, last-applied id, and membership are
    /// restored from it immediately.
    pub fn persistent(path: impl Into<std::path::PathBuf>) -> Result<Self, StorageError<NodeId>> {
        let path = path.into();
        let machine = Self {
            snapshot_path: Some(Arc::new(path.clone())),
            ..Self::default()
        };

        if path.exists() {
            let bytes = std::fs::read(&path)
                .map_err(|error| StorageIOError::read_snapshot(None, &error))?;
            let stored: StoredSnapshot = serde_json::from_slice(&bytes)
                .map_err(|error| StorageIOError::read_snapshot(None, &error))?;
            let state: CoordinationSnapshot = if stored.data.is_empty() {
                CoordinationSnapshot::default()
            } else {
                serde_json::from_slice(&stored.data)
                    .map_err(|error| StorageIOError::read_snapshot(None, &error))?
            };

            let mut inner = machine
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            inner.last_applied = stored.meta.last_log_id;
            inner.last_membership = stored.meta.last_membership.clone();
            inner.state = state.clone();
            inner.current_snapshot = Some(stored);
            drop(inner);
            machine.committed_tx.send_replace(state);
            machine
                .telemetry
                .snapshot_loads
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        Ok(machine)
    }

    /// Counter handle shared with `RaftMetadataNode::telemetry`.
    pub fn telemetry_handle(&self) -> Arc<StorageTelemetry> {
        Arc::clone(&self.telemetry)
    }

    /// Point-in-time snapshot persistence counters.
    pub fn telemetry(&self) -> StorageTelemetrySnapshot {
        self.telemetry.snapshot()
    }

    /// Atomically write the snapshot file: tmp + fsync + rename + parent-dir
    /// fsync, so a crash leaves either the old or the new snapshot — never a
    /// torn one — and the rename itself is durable.
    fn persist_snapshot(&self, stored: &StoredSnapshot) -> Result<(), StorageError<NodeId>> {
        let Some(path) = &self.snapshot_path else {
            return Ok(());
        };
        let bytes = serde_json::to_vec(stored).map_err(|error| {
            StorageIOError::write_snapshot(Some(stored.meta.signature()), &error)
        })?;
        let tmp_path = path.with_extension("tmp");
        let write = || -> std::io::Result<()> {
            use std::io::Write as _;
            let mut file = std::fs::File::create(&tmp_path)?;
            file.write_all(&bytes)?;
            file.sync_data()?;
            drop(file);
            std::fs::rename(&tmp_path, path.as_ref())?;
            super::fsync_parent_dir(path)
        };
        write().map_err(|error| {
            StorageIOError::write_snapshot(Some(stored.meta.signature()), &error)
        })?;
        self.telemetry
            .snapshot_persists
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Current committed coordination snapshot.
    pub fn committed_snapshot(&self) -> CoordinationSnapshot {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .state
            .clone()
    }

    /// Last applied raft log id, if any.
    pub fn last_applied(&self) -> Option<LogId<NodeId>> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .last_applied
    }

    /// Subscribe to committed snapshot updates.
    ///
    /// The receiver always holds the latest committed snapshot; only accepted
    /// state changes are published (rejected stale writes are not).
    pub fn watch_committed(&self) -> watch::Receiver<CoordinationSnapshot> {
        self.committed_tx.subscribe()
    }
}

impl RaftSnapshotBuilder<GanglionRaftConfig> for GanglionStateMachine {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<GanglionRaftConfig>, StorageError<NodeId>> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        self.persist_snapshot(&stored)?;
        inner.current_snapshot = Some(stored);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<GanglionRaftConfig> for GanglionStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<MetadataRaftResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<GanglionRaftConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut replies = Vec::new();
        let mut state_changed = false;

        // All rejections are decided here, inside the replicated apply: every
        // replica sees the same command order, so the outcome is deterministic.
        let mut apply_command = |inner: &mut StateMachineInner,
                                 command: MetadataRaftCommand|
         -> Option<MetadataRejection> {
            let snapshot = match command {
                MetadataRaftCommand::ApplySnapshot(snapshot) => snapshot,
                MetadataRaftCommand::ApplySnapshotGuarded {
                    expected_generation,
                    snapshot,
                } => {
                    if inner.state.generation != expected_generation {
                        return Some(MetadataRejection::GenerationMismatch {
                            expected: expected_generation,
                            actual: inner.state.generation,
                        });
                    }
                    snapshot
                }
                // Merge commands: cannot clobber concurrent updates, so no
                // CAS/staleness checks apply.
                MetadataRaftCommand::RegisterNode { node } => {
                    // Liveness refreshes must not churn the cluster version:
                    // a label-only change (heartbeat timestamps, applied
                    // tails) updates silently — no generation bump, no watch
                    // wake-up — so guarded CAS writers never race heartbeats
                    // and watchers only see identity/topology changes.
                    // Liveness readers consume the committed snapshot
                    // directly, so freshness is unaffected.
                    match inner.state.nodes.get(&node.node_id) {
                        Some(existing) if *existing == node => {}
                        Some(existing)
                            if existing.endpoint == node.endpoint
                                && existing.admin_endpoint == node.admin_endpoint =>
                        {
                            inner.state.nodes.insert(node.node_id.clone(), node);
                        }
                        _ => {
                            inner.state.nodes.insert(node.node_id.clone(), node);
                            inner.state.generation += 1;
                            state_changed = true;
                        }
                    }
                    return None;
                }
                MetadataRaftCommand::DeregisterNode { node_id } => {
                    if inner.state.nodes.remove(&node_id).is_some() {
                        inner.state.generation += 1;
                        state_changed = true;
                    }
                    return None;
                }
                MetadataRaftCommand::RegisterResource { resource } => {
                    if inner.state.resources.insert(resource) {
                        inner.state.generation += 1;
                        state_changed = true;
                    }
                    return None;
                }
                MetadataRaftCommand::DeregisterResource { resource } => {
                    if inner.state.resources.remove(&resource) {
                        inner.state.generation += 1;
                        state_changed = true;
                    }
                    return None;
                }
                MetadataRaftCommand::SetAttribute { key, value } => {
                    if inner.state.attributes.get(&key) != Some(&value) {
                        inner.state.attributes.insert(key, value);
                        inner.state.generation += 1;
                        state_changed = true;
                    }
                    return None;
                }
                MetadataRaftCommand::RemoveAttribute { key } => {
                    if inner.state.attributes.remove(&key).is_some() {
                        inner.state.generation += 1;
                        state_changed = true;
                    }
                    return None;
                }
                MetadataRaftCommand::CompareAndSetAttribute {
                    key,
                    expected,
                    value,
                } => {
                    let actual = inner.state.attributes.get(&key).cloned();
                    if actual != expected {
                        return Some(MetadataRejection::AttributeMismatch { key, actual });
                    }
                    if actual.as_ref() != Some(&value) {
                        inner.state.attributes.insert(key, value);
                        inner.state.generation += 1;
                        state_changed = true;
                    }
                    return None;
                }
            };

            if snapshot.generation < inner.state.generation {
                return Some(MetadataRejection::StaleGeneration);
            }
            inner.state = snapshot;
            state_changed = true;
            None
        };

        for entry in entries {
            inner.last_applied = Some(entry.log_id);
            let rejection = match entry.payload {
                EntryPayload::Blank => None,
                EntryPayload::Normal(command) => apply_command(&mut inner, command),
                EntryPayload::Membership(membership) => {
                    inner.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    None
                }
            };
            replies.push(MetadataRaftResponse {
                accepted: rejection.is_none(),
                rejection,
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
            serde_json::from_slice(&data)
                .map_err(|error| StorageIOError::read_snapshot(Some(meta.signature()), &error))?
        };

        let stored = StoredSnapshot {
            meta: meta.clone(),
            data,
        };
        self.persist_snapshot(&stored)?;

        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.state = state;
        inner.current_snapshot = Some(stored);
        self.committed_tx.send_replace(inner.state.clone());
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<GanglionRaftConfig>>, StorageError<NodeId>> {
        let inner = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
    use proptest::prelude::*;

    struct InMemoryBuilder;

    impl StoreBuilder<GanglionRaftConfig, GanglionLogStore, GanglionStateMachine, ()>
        for InMemoryBuilder
    {
        async fn build(
            &self,
        ) -> Result<((), GanglionLogStore, GanglionStateMachine), StorageError<NodeId>> {
            Ok((
                (),
                GanglionLogStore::default(),
                GanglionStateMachine::default(),
            ))
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

            let make_entry =
                |index: u64, snapshot: CoordinationSnapshot| Entry::<GanglionRaftConfig> {
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

    /// Catalogue + attribute merge commands: idempotent, generation bumps
    /// only on real changes, never touch assignments.
    #[test]
    fn merge_commands_update_catalogue_and_attributes() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let mut sm = GanglionStateMachine::default();
            let leader = openraft::CommittedLeaderId::new(1, 0);
            let mut index = 0u64;
            let mut entry = |command: MetadataRaftCommand| {
                index += 1;
                vec![Entry::<GanglionRaftConfig> {
                    log_id: LogId::new(leader, index),
                    payload: EntryPayload::Normal(command),
                }]
            };
            let resource =
                ganglion_core::ResourceIdentity::new("fibril/queue", "orders", 0, None::<String>);

            let replies = sm
                .apply(entry(MetadataRaftCommand::RegisterResource {
                    resource: resource.clone(),
                }))
                .await
                .expect("apply");
            assert!(replies[0].accepted);
            assert_eq!(sm.committed_snapshot().generation, 1);
            assert!(sm.committed_snapshot().resources.contains(&resource));

            // Idempotent re-register: no generation bump.
            sm.apply(entry(MetadataRaftCommand::RegisterResource {
                resource: resource.clone(),
            }))
            .await
            .expect("apply");
            assert_eq!(sm.committed_snapshot().generation, 1);

            // Attributes: set, same-value no-op, change, remove.
            sm.apply(entry(MetadataRaftCommand::SetAttribute {
                key: "runtime_settings".into(),
                value: "v1".into(),
            }))
            .await
            .expect("apply");
            assert_eq!(sm.committed_snapshot().generation, 2);
            sm.apply(entry(MetadataRaftCommand::SetAttribute {
                key: "runtime_settings".into(),
                value: "v1".into(),
            }))
            .await
            .expect("apply");
            assert_eq!(
                sm.committed_snapshot().generation,
                2,
                "same-value set is a no-op"
            );
            sm.apply(entry(MetadataRaftCommand::SetAttribute {
                key: "runtime_settings".into(),
                value: "v2".into(),
            }))
            .await
            .expect("apply");
            assert_eq!(sm.committed_snapshot().generation, 3);
            assert_eq!(
                sm.committed_snapshot().attributes.get("runtime_settings"),
                Some(&"v2".to_string())
            );

            sm.apply(entry(MetadataRaftCommand::DeregisterResource { resource }))
                .await
                .expect("apply");
            assert_eq!(sm.committed_snapshot().generation, 4);
            assert!(sm.committed_snapshot().resources.is_empty());
            sm.apply(entry(MetadataRaftCommand::RemoveAttribute {
                key: "runtime_settings".into(),
            }))
            .await
            .expect("apply");
            assert_eq!(sm.committed_snapshot().generation, 5);
            assert!(sm.committed_snapshot().attributes.is_empty());

            // CAS attribute: create-if-absent, wrong-expected rejection with
            // the actual value reported, matching-expected succeeds.
            let replies = sm
                .apply(entry(MetadataRaftCommand::CompareAndSetAttribute {
                    key: "settings".into(),
                    expected: None,
                    value: "v1".into(),
                }))
                .await
                .expect("apply");
            assert!(replies[0].accepted);
            assert_eq!(sm.committed_snapshot().generation, 6);

            let replies = sm
                .apply(entry(MetadataRaftCommand::CompareAndSetAttribute {
                    key: "settings".into(),
                    expected: Some("stale".into()),
                    value: "v2".into(),
                }))
                .await
                .expect("apply");
            assert_eq!(
                replies[0].rejection,
                Some(MetadataRejection::AttributeMismatch {
                    key: "settings".into(),
                    actual: Some("v1".into()),
                })
            );
            assert_eq!(
                sm.committed_snapshot().generation,
                6,
                "rejected CAS writes nothing"
            );

            let replies = sm
                .apply(entry(MetadataRaftCommand::CompareAndSetAttribute {
                    key: "settings".into(),
                    expected: Some("v1".into()),
                    value: "v2".into(),
                }))
                .await
                .expect("apply");
            assert!(replies[0].accepted);
            assert_eq!(
                sm.committed_snapshot().attributes.get("settings"),
                Some(&"v2".to_string())
            );
        });
    }

    /// CAS attribute guarantees relied on by replicated topic metadata /
    /// runtime settings: create-once (expected=None fails if the key exists),
    /// compare-update against the current value, idempotent same-value writes
    /// (no generation bump), and expected-but-absent rejection. These are what
    /// make concurrent writers race-safe.
    #[test]
    fn cas_attribute_preserves_create_once_and_idempotency() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let mut sm = GanglionStateMachine::default();
            let leader = openraft::CommittedLeaderId::new(1, 0);
            let mut index = 0u64;
            let mut entry = |command: MetadataRaftCommand| {
                index += 1;
                vec![Entry::<GanglionRaftConfig> {
                    log_id: LogId::new(leader, index),
                    payload: EntryPayload::Normal(command),
                }]
            };
            let cas =
                |expected: Option<&str>, value: &str| MetadataRaftCommand::CompareAndSetAttribute {
                    key: "topic/orders".into(),
                    expected: expected.map(str::to_string),
                    value: value.into(),
                };

            // Create-once: first create-if-absent wins.
            let replies = sm.apply(entry(cas(None, "n=4"))).await.expect("apply");
            assert!(replies[0].accepted);
            assert_eq!(sm.committed_snapshot().generation, 1);

            // Create-once: a SECOND create-if-absent must be rejected (the key
            // already exists) — this is what stops two declarers both "creating"
            // the same topic and clobbering each other.
            let replies = sm.apply(entry(cas(None, "n=8"))).await.expect("apply");
            assert_eq!(
                replies[0].rejection,
                Some(MetadataRejection::AttributeMismatch {
                    key: "topic/orders".into(),
                    actual: Some("n=4".into()),
                })
            );
            assert_eq!(
                sm.committed_snapshot().generation,
                1,
                "rejected create-once writes nothing"
            );

            // Idempotent CAS: expected==actual==value -> accepted, NO bump.
            let replies = sm
                .apply(entry(cas(Some("n=4"), "n=4")))
                .await
                .expect("apply");
            assert!(replies[0].accepted);
            assert_eq!(
                sm.committed_snapshot().generation,
                1,
                "no-op CAS must not bump the generation"
            );

            // Compare-update against the current value succeeds.
            let replies = sm
                .apply(entry(cas(Some("n=4"), "n=8")))
                .await
                .expect("apply");
            assert!(replies[0].accepted);
            assert_eq!(sm.committed_snapshot().generation, 2);
            assert_eq!(
                sm.committed_snapshot().attributes.get("topic/orders"),
                Some(&"n=8".to_string())
            );

            // Expected-but-absent: CAS expecting a value on a removed key is
            // rejected with actual=None (never silently creates).
            sm.apply(entry(MetadataRaftCommand::RemoveAttribute {
                key: "topic/orders".into(),
            }))
            .await
            .expect("apply");
            let gen_after_remove = sm.committed_snapshot().generation;
            let replies = sm
                .apply(entry(cas(Some("n=8"), "n=9")))
                .await
                .expect("apply");
            assert_eq!(
                replies[0].rejection,
                Some(MetadataRejection::AttributeMismatch {
                    key: "topic/orders".into(),
                    actual: None,
                })
            );
            assert_eq!(sm.committed_snapshot().generation, gen_after_remove);

            // RemoveAttribute on an absent key is a no-op (no bump).
            let before = sm.committed_snapshot().generation;
            sm.apply(entry(MetadataRaftCommand::RemoveAttribute {
                key: "topic/orders".into(),
            }))
            .await
            .expect("apply");
            assert_eq!(
                sm.committed_snapshot().generation,
                before,
                "removing an absent key writes nothing"
            );
        });
    }

    /// Liveness refreshes are version-silent: label-only re-registration
    /// neither bumps the generation nor wakes watchers (heartbeats must never
    /// race guarded CAS writers); identity changes still do both.
    #[test]
    fn label_only_node_refresh_is_version_silent() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let mut sm = GanglionStateMachine::default();
            let leader = openraft::CommittedLeaderId::new(1, 0);
            let mut index = 0u64;
            let mut entry = |command: MetadataRaftCommand| {
                index += 1;
                vec![Entry::<GanglionRaftConfig> {
                    log_id: LogId::new(leader, index),
                    payload: EntryPayload::Normal(command),
                }]
            };
            let node = |beat: &str| {
                let mut node =
                    ganglion_core::NodeInfo::new("broker-a", "127.0.0.1:9000", None::<String>);
                node.labels.insert("heartbeat_unix_ms".into(), beat.into());
                node
            };

            sm.apply(entry(MetadataRaftCommand::RegisterNode { node: node("1") }))
                .await
                .expect("apply");
            assert_eq!(sm.committed_snapshot().generation, 1);
            let mut watch = sm.watch_committed();
            watch.borrow_and_update();

            // Heartbeat refresh: labels change, identity does not.
            sm.apply(entry(MetadataRaftCommand::RegisterNode { node: node("2") }))
                .await
                .expect("apply");
            assert_eq!(
                sm.committed_snapshot().generation,
                1,
                "label-only refresh must not bump the generation"
            );
            assert!(
                !watch.has_changed().expect("watch open"),
                "label-only refresh must not wake watchers"
            );
            // ...but the labels themselves ARE fresh for liveness readers.
            assert_eq!(
                sm.committed_snapshot().nodes["broker-a"].labels["heartbeat_unix_ms"],
                "2"
            );

            // Identity change (new address): bump + wake.
            let mut moved = node("3");
            moved.endpoint = "127.0.0.1:9999".into();
            sm.apply(entry(MetadataRaftCommand::RegisterNode { node: moved }))
                .await
                .expect("apply");
            assert_eq!(sm.committed_snapshot().generation, 2);
            assert!(watch.has_changed().expect("watch open"));
        });
    }

    /// FAILURE_MODES §3.3: a corrupt snapshot file fails startup loudly —
    /// never silently degrades to default state.
    #[test]
    fn persistent_state_machine_rejects_corrupt_snapshot_file() {
        let path = std::env::temp_dir().join(format!(
            "ganglion-corrupt-snap-{}-{:?}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::write(&path, b"{definitely not a snapshot").expect("write corrupt file");
        assert!(
            GanglionStateMachine::persistent(&path).is_err(),
            "corrupt snapshot must fail startup"
        );
    }

    /// FAILURE_MODES §1.3: a leftover `.tmp` from a crashed persist is
    /// ignored on load and harmlessly overwritten by the next persist.
    #[test]
    fn persistent_state_machine_ignores_leftover_tmp_file() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let path = std::env::temp_dir().join(format!(
                "ganglion-tmp-leftover-{}-{:?}.json",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            ));
            // Simulate a crash mid-persist: torn tmp present, no real file.
            std::fs::write(path.with_extension("tmp"), b"{torn").expect("write torn tmp");

            let mut sm =
                GanglionStateMachine::persistent(&path).expect("tmp leftovers must not block");
            sm.apply(vec![Entry::<GanglionRaftConfig> {
                log_id: LogId::new(openraft::CommittedLeaderId::new(1, 0), 1),
                payload: EntryPayload::Normal(MetadataRaftCommand::ApplySnapshot(
                    CoordinationSnapshot {
                        generation: 1,
                        ..CoordinationSnapshot::default()
                    },
                )),
            }])
            .await
            .expect("apply");
            sm.build_snapshot().await.expect("persist overwrites tmp");

            let reloaded = GanglionStateMachine::persistent(&path).expect("reload");
            assert_eq!(reloaded.committed_snapshot().generation, 1);
        });
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// The state machine must behave exactly like a running-max model with
        /// CAS guards: plain commands are accepted iff `generation >= current`;
        /// guarded commands additionally require `expected == current`
        /// generation at apply time. Batching must not change outcomes.
        ///
        /// Op encoding: `mode` 0 = plain, 1 = guarded with the correct
        /// expectation (resolved at apply time), 2 = guarded with a wrong
        /// expectation (off by `1 + offset`).
        #[test]
        fn fuzz_state_machine_matches_running_max_model(
            ops in proptest::collection::vec((0u8..3, 0u64..8, 0u64..3), 1..40),
            batch_split in 1usize..8,
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("runtime");
            rt.block_on(async {
                let mut sm = GanglionStateMachine::default();
                let mut model_generation = 0u64;
                let mut index = 0u64;
                let leader = openraft::CommittedLeaderId::new(1, 0);

                for chunk in ops.chunks(batch_split) {
                    // Resolve expectations against the model BEFORE applying the
                    // chunk, exactly like a controller that read committed state
                    // and then proposed: earlier commands in the same batch can
                    // invalidate later guards.
                    let read_generation = model_generation;
                    let entries: Vec<Entry<GanglionRaftConfig>> = chunk
                        .iter()
                        .map(|(mode, generation, offset)| {
                            index += 1;
                            let snapshot = CoordinationSnapshot {
                                generation: *generation,
                                ..CoordinationSnapshot::default()
                            };
                            let command = match mode {
                                0 => MetadataRaftCommand::ApplySnapshot(snapshot),
                                1 => MetadataRaftCommand::ApplySnapshotGuarded {
                                    expected_generation: read_generation,
                                    snapshot,
                                },
                                _ => MetadataRaftCommand::ApplySnapshotGuarded {
                                    expected_generation: read_generation + 1 + offset,
                                    snapshot,
                                },
                            };
                            Entry {
                                log_id: LogId::new(leader, index),
                                payload: EntryPayload::Normal(command),
                            }
                        })
                        .collect();

                    let replies = sm.apply(entries).await.expect("apply never errors");
                    for ((mode, generation, offset), reply) in chunk.iter().zip(replies) {
                        // True CAS semantics: a guard only matters if its
                        // expectation differs from the generation at apply
                        // time — a "wrong" guess can become right when earlier
                        // ops in the batch advanced the generation.
                        let guard = match mode {
                            0 => None,
                            1 => Some(read_generation),
                            _ => Some(read_generation + 1 + offset),
                        };
                        let expected_rejection = match guard {
                            Some(expected) if expected != model_generation => {
                                Some(MetadataRejection::GenerationMismatch {
                                    expected,
                                    actual: model_generation,
                                })
                            }
                            _ if *generation < model_generation => {
                                Some(MetadataRejection::StaleGeneration)
                            }
                            _ => None,
                        };

                        let expect_accept = expected_rejection.is_none();
                        prop_assert_eq!(reply.accepted, expect_accept);
                        prop_assert_eq!(reply.rejection, expected_rejection);
                        if expect_accept {
                            model_generation = *generation;
                        }
                        prop_assert_eq!(reply.snapshot.generation, model_generation);
                    }
                }

                prop_assert_eq!(sm.committed_snapshot().generation, model_generation);
                prop_assert_eq!(sm.last_applied().map(|id| id.index), Some(index));
                prop_assert_eq!(
                    sm.watch_committed().borrow().generation,
                    model_generation,
                    "watch channel must hold the latest committed state"
                );
                Ok(())
            })?;
        }

        /// A persistent state machine reloaded from disk must equal the state
        /// at the last persisted snapshot (build or install), for any
        /// interleaving of applies, snapshot builds, and snapshot installs.
        ///
        /// Op encoding: 0 = apply (advancing generation), 1 = build_snapshot,
        /// 2 = install leader-shipped snapshot (also advancing generation).
        #[test]
        fn fuzz_persistent_state_machine_reload_matches_last_persisted(
            ops in proptest::collection::vec((0u8..3, 1u64..50), 1..25),
        ) {
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("runtime");
            rt.block_on(async {
                let nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos();
                let path = std::env::temp_dir().join(format!(
                    "ganglion-sm-fuzz-{}-{nanos}.json",
                    std::process::id()
                ));

                let mut sm = GanglionStateMachine::persistent(&path)
                    .expect("fresh persistent state machine");
                let leader = openraft::CommittedLeaderId::new(1, 0);
                let mut model_generation = 0u64;
                let mut persisted_generation: Option<u64> = None;
                let mut index = 0u64;

                for (mode, generation_step) in &ops {
                    match mode {
                        0 => {
                            index += 1;
                            let generation = model_generation + generation_step;
                            let replies = sm
                                .apply(vec![Entry::<GanglionRaftConfig> {
                                    log_id: LogId::new(leader, index),
                                    payload: EntryPayload::Normal(
                                        MetadataRaftCommand::ApplySnapshot(
                                            CoordinationSnapshot {
                                                generation,
                                                ..CoordinationSnapshot::default()
                                            },
                                        ),
                                    ),
                                }])
                                .await
                                .expect("apply");
                            prop_assert!(replies[0].accepted);
                            model_generation = generation;
                        }
                        1 => {
                            sm.build_snapshot().await.expect("build snapshot");
                            persisted_generation = Some(model_generation);
                        }
                        _ => {
                            index += 1;
                            let generation = model_generation + generation_step;
                            let state = CoordinationSnapshot {
                                generation,
                                ..CoordinationSnapshot::default()
                            };
                            let data =
                                serde_json::to_vec(&state).expect("serialize install data");
                            let meta = SnapshotMeta {
                                last_log_id: Some(LogId::new(leader, index)),
                                last_membership: StoredMembership::default(),
                                snapshot_id: format!("fuzz-install-{index}"),
                            };
                            sm.install_snapshot(&meta, Box::new(Cursor::new(data)))
                                .await
                                .expect("install snapshot");
                            model_generation = generation;
                            persisted_generation = Some(generation);
                        }
                    }
                }

                // Reload from disk: state equals the last persisted point
                // (or default if nothing was ever persisted).
                let reloaded =
                    GanglionStateMachine::persistent(&path).expect("reload persistent SM");
                prop_assert_eq!(
                    reloaded.committed_snapshot().generation,
                    persisted_generation.unwrap_or(0)
                );
                prop_assert_eq!(
                    reloaded.watch_committed().borrow().generation,
                    persisted_generation.unwrap_or(0)
                );
                Ok(())
            })?;
        }
    }
}
