//! Runtime metadata node backed by a real `Raft<GanglionRaftConfig>` instance.
//!
//! `RaftMetadataNode` mirrors `MetadataConsensus` semantics (leader-only writes,
//! stale-generation rejection) over actual raft consensus. The API is async
//! because raft itself is; the sync `MetadataConsensus` trait remains served by
//! the in-memory/persisted nodes until a bridging decision is made.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use ganglion_core::CoordinationSnapshot;
use openraft::storage::RaftLogStorage;
use openraft::{BasicNode, Config, Raft};

use crate::OpenraftAdapterError;

use super::{
    GanglionLogStore, GanglionRaftConfig, GanglionStateMachine, InProcessRouter,
    MetadataRaftCommand, MetadataRaftResponse,
};

type NodeId = u64;

/// Map raft client-write/membership errors onto `MetadataConsensus` semantics.
fn map_membership_error(
    error: openraft::error::RaftError<NodeId, openraft::error::ClientWriteError<NodeId, BasicNode>>,
) -> OpenraftAdapterError {
    if error.forward_to_leader().is_some() {
        OpenraftAdapterError::NotLeader
    } else {
        OpenraftAdapterError::Storage(error.to_string())
    }
}

/// Serializable view of the raft group as seen by one node.
///
/// This is the JSON contract consumed by topology CLIs and admin diagrams.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RaftTopology {
    pub local_id: NodeId,
    pub leader: Option<NodeId>,
    pub voters: Vec<NodeId>,
    pub learners: Vec<NodeId>,
    /// Raft id → address (from `BasicNode.addr`).
    pub nodes: BTreeMap<NodeId, String>,
    pub last_applied_index: Option<u64>,
    pub snapshot_index: Option<u64>,
    pub committed_generation: u64,
}

/// One raft-backed metadata node.
///
/// Generic over the raft log store (`GanglionLogStore` in-memory default,
/// `FileRaftLogStore` durable WAL) and the network factory (`InProcessRouter`
/// default for same-process clusters, `TcpNetworkFactory` for real
/// multi-process clusters).
pub struct RaftMetadataNode<LS = GanglionLogStore, NF = InProcessRouter<LS>>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
    NF: openraft::RaftNetworkFactory<GanglionRaftConfig>,
{
    id: NodeId,
    raft: Raft<GanglionRaftConfig, NF, LS, GanglionStateMachine>,
    state_machine: GanglionStateMachine,
    log_telemetry: Option<Arc<super::StorageTelemetry>>,
}

impl RaftMetadataNode<GanglionLogStore> {
    /// Start an in-memory node and register it on the router.
    pub async fn start(
        id: NodeId,
        config: Arc<Config>,
        router: &InProcessRouter,
    ) -> Result<Self, OpenraftAdapterError> {
        Self::start_with_store(id, config, router, GanglionLogStore::default()).await
    }
}

impl RaftMetadataNode<super::FileRaftLogStore> {
    /// Start a fully durable node storing its WAL and snapshot under `dir`.
    ///
    /// Recovery is bounded: restart loads `snapshot.json` and replays only the
    /// short WAL tail behind it (see `default_raft_config` thresholds), and
    /// committed state survives log purges across full restarts.
    pub async fn start_durable(
        id: NodeId,
        config: Arc<Config>,
        router: &InProcessRouter<super::FileRaftLogStore>,
        dir: impl AsRef<std::path::Path>,
    ) -> Result<Self, OpenraftAdapterError> {
        let (log_store, state_machine) = open_durable_storage(dir)?;
        let log_telemetry = log_store.telemetry_handle();
        let mut node =
            Self::start_with_storage(id, config, router, log_store, state_machine).await?;
        node.log_telemetry = Some(log_telemetry);
        Ok(node)
    }
}

impl RaftMetadataNode<super::FileRaftLogStore, super::TcpNetworkFactory> {
    /// Start a durable node reachable over TCP: storage under `dir`, raft RPCs
    /// served on `listen_addr` (the address other members must carry in their
    /// `BasicNode.addr` for this node).
    ///
    /// Returns the node plus the listener handle; dropping the handle stops
    /// serving. Peers are dialed from membership addresses — no static peer
    /// table.
    pub async fn start_durable_tcp(
        id: NodeId,
        config: Arc<Config>,
        listen_addr: impl tokio::net::ToSocketAddrs,
        dir: impl AsRef<std::path::Path>,
    ) -> Result<(Self, super::TcpRaftServer), OpenraftAdapterError> {
        Self::start_durable_tcp_with_format(
            id,
            config,
            listen_addr,
            dir,
            super::WireFormat::default(),
        )
        .await
    }

    /// `start_durable_tcp` with an explicit wire format for outbound frames
    /// (both RPCs to peers and replies on the listener). Pass this from
    /// startup configuration.
    pub async fn start_durable_tcp_with_format(
        id: NodeId,
        config: Arc<Config>,
        listen_addr: impl tokio::net::ToSocketAddrs,
        dir: impl AsRef<std::path::Path>,
        wire_format: super::WireFormat,
    ) -> Result<(Self, super::TcpRaftServer), OpenraftAdapterError> {
        let (log_store, state_machine) = open_durable_storage(dir)?;
        let log_telemetry = log_store.telemetry_handle();
        let mut node = Self::start_with_network(
            id,
            config,
            super::TcpNetworkFactory::with_format(wire_format),
            log_store,
            state_machine,
        )
        .await?;
        node.log_telemetry = Some(log_telemetry);

        let server = super::TcpRaftServer::bind(listen_addr, node.raft.clone(), wire_format)
            .await
            .map_err(|error| OpenraftAdapterError::Storage(error.to_string()))?;
        Ok((node, server))
    }
}

fn open_durable_storage(
    dir: impl AsRef<std::path::Path>,
) -> Result<(super::FileRaftLogStore, GanglionStateMachine), OpenraftAdapterError> {
    let dir = dir.as_ref();
    std::fs::create_dir_all(dir)
        .map_err(|error| OpenraftAdapterError::Storage(error.to_string()))?;
    let log_store = super::FileRaftLogStore::open(dir.join("raft-wal.jsonl"))
        .map_err(|error| OpenraftAdapterError::Storage(error.to_string()))?;
    let state_machine = GanglionStateMachine::persistent(dir.join("snapshot.json"))
        .map_err(|error| OpenraftAdapterError::Storage(error.to_string()))?;
    Ok((log_store, state_machine))
}

impl<LS> RaftMetadataNode<LS>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
{
    /// Start a node over the given log store and register it on the router.
    pub async fn start_with_store(
        id: NodeId,
        config: Arc<Config>,
        router: &InProcessRouter<LS>,
        log_store: LS,
    ) -> Result<Self, OpenraftAdapterError> {
        Self::start_with_storage(
            id,
            config,
            router,
            log_store,
            GanglionStateMachine::default(),
        )
        .await
    }

    /// Start a node over explicit log store and state machine instances.
    pub async fn start_with_storage(
        id: NodeId,
        config: Arc<Config>,
        router: &InProcessRouter<LS>,
        log_store: LS,
        state_machine: GanglionStateMachine,
    ) -> Result<Self, OpenraftAdapterError> {
        let node =
            Self::start_with_network(id, config, router.clone(), log_store, state_machine).await?;
        router.register(id, node.raft.clone());
        Ok(node)
    }
}

impl<LS, NF> RaftMetadataNode<LS, NF>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
    NF: openraft::RaftNetworkFactory<GanglionRaftConfig>,
{
    /// Start a node over an arbitrary network factory (TCP, in-process, ...).
    pub async fn start_with_network(
        id: NodeId,
        config: Arc<Config>,
        network: NF,
        log_store: LS,
        state_machine: GanglionStateMachine,
    ) -> Result<Self, OpenraftAdapterError> {
        let raft = Raft::new(id, config, network, log_store, state_machine.clone())
            .await
            .map_err(|error| OpenraftAdapterError::Storage(error.to_string()))?;
        Ok(Self {
            id,
            raft,
            state_machine,
            log_telemetry: None,
        })
    }

    /// Bootstrap cluster membership. Call once, on one node, with blank state.
    pub async fn initialize(
        &self,
        members: BTreeMap<NodeId, BasicNode>,
    ) -> Result<(), OpenraftAdapterError> {
        self.raft
            .initialize(members)
            .await
            .map_err(|error| OpenraftAdapterError::Config(error.to_string()))
    }

    /// Propose a snapshot through raft consensus.
    ///
    /// Errors map onto `MetadataConsensus` semantics: proposals on a non-leader
    /// return `NotLeader`, committed-but-rejected stale generations return
    /// `StaleGeneration`.
    pub async fn write_snapshot(
        &self,
        snapshot: CoordinationSnapshot,
    ) -> Result<MetadataRaftResponse, OpenraftAdapterError> {
        self.submit_command(MetadataRaftCommand::ApplySnapshot(snapshot))
            .await
    }

    /// CAS write: commits only if the committed generation still equals
    /// `expected_generation`, otherwise fails with `GenerationMismatch`.
    /// Controller loops should re-read, re-plan, and retry on mismatch
    /// (or use [`Self::plan_and_propose_guarded`]).
    pub async fn write_snapshot_guarded(
        &self,
        expected_generation: u64,
        snapshot: CoordinationSnapshot,
    ) -> Result<MetadataRaftResponse, OpenraftAdapterError> {
        self.submit_command(MetadataRaftCommand::ApplySnapshotGuarded {
            expected_generation,
            snapshot,
        })
        .await
    }

    /// One race-safe controller iteration: read committed state, produce the
    /// desired snapshot via `plan` (pure!), bump the generation, stamp fencing
    /// epochs, and propose guarded. Retries up to `max_retries` times when
    /// another proposal won the CAS race in between.
    pub async fn plan_and_propose_guarded<F>(
        &self,
        plan: F,
        max_retries: usize,
    ) -> Result<MetadataRaftResponse, OpenraftAdapterError>
    where
        F: Fn(&CoordinationSnapshot) -> CoordinationSnapshot,
    {
        let mut attempts = 0;
        loop {
            let committed = self.committed_snapshot();
            let expected = committed.generation;
            let mut desired = plan(&committed);
            desired.generation = expected + 1;
            ganglion_core::stamp_assignment_epochs(&committed, &mut desired);

            match self.write_snapshot_guarded(expected, desired).await {
                Err(OpenraftAdapterError::GenerationMismatch { .. }) if attempts < max_retries => {
                    attempts += 1;
                }
                other => return other,
            }
        }
    }

    async fn submit_command(
        &self,
        command: MetadataRaftCommand,
    ) -> Result<MetadataRaftResponse, OpenraftAdapterError> {
        let response = match self.raft.client_write(command).await {
            Ok(response) => response.data,
            Err(error) => return Err(map_membership_error(error)),
        };

        match response.rejection {
            None => Ok(response),
            Some(super::MetadataRejection::StaleGeneration) => {
                Err(OpenraftAdapterError::StaleGeneration)
            }
            Some(super::MetadataRejection::GenerationMismatch { expected, actual }) => {
                Err(OpenraftAdapterError::GenerationMismatch { expected, actual })
            }
            Some(super::MetadataRejection::AttributeMismatch { key, actual }) => {
                Err(OpenraftAdapterError::AttributeMismatch { key, actual })
            }
        }
    }

    /// Merge one node record into the committed snapshot (leader-only).
    ///
    /// Unlike `write_snapshot`, this cannot clobber concurrent updates;
    /// brokers use it to register and heartbeat themselves. Non-leaders should
    /// forward via `client_write_remote` to the leader's raft address.
    pub async fn register_node(
        &self,
        node: ganglion_core::NodeInfo,
    ) -> Result<MetadataRaftResponse, OpenraftAdapterError> {
        self.submit_command(MetadataRaftCommand::RegisterNode { node })
            .await
    }

    /// Remove one node record from the committed snapshot (leader-only).
    pub async fn deregister_node(
        &self,
        node_id: impl Into<String>,
    ) -> Result<MetadataRaftResponse, OpenraftAdapterError> {
        self.submit_command(MetadataRaftCommand::DeregisterNode {
            node_id: node_id.into(),
        })
        .await
    }

    /// Submit any merge command (leader-only). Provider layers that already
    /// hold a `MetadataRaftCommand` use this instead of per-command wrappers.
    pub async fn submit_merge(
        &self,
        command: MetadataRaftCommand,
    ) -> Result<MetadataRaftResponse, OpenraftAdapterError> {
        self.submit_command(command).await
    }

    /// Add one resource to the cluster catalogue (leader-only merge).
    pub async fn register_resource(
        &self,
        resource: ganglion_core::ResourceIdentity,
    ) -> Result<MetadataRaftResponse, OpenraftAdapterError> {
        self.submit_command(MetadataRaftCommand::RegisterResource { resource })
            .await
    }

    /// Remove one resource from the catalogue (leader-only merge).
    pub async fn deregister_resource(
        &self,
        resource: ganglion_core::ResourceIdentity,
    ) -> Result<MetadataRaftResponse, OpenraftAdapterError> {
        self.submit_command(MetadataRaftCommand::DeregisterResource { resource })
            .await
    }

    /// Set one cluster attribute (leader-only merge; same-value writes no-op).
    pub async fn set_attribute(
        &self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<MetadataRaftResponse, OpenraftAdapterError> {
        self.submit_command(MetadataRaftCommand::SetAttribute {
            key: key.into(),
            value: value.into(),
        })
        .await
    }

    /// Attribute CAS (leader-only): set `key` to `value` only if its current
    /// value equals `expected` (`None` = absent). Lost races surface as
    /// `OpenraftAdapterError::AttributeMismatch` carrying the actual value.
    pub async fn compare_and_set_attribute(
        &self,
        key: impl Into<String>,
        expected: Option<String>,
        value: impl Into<String>,
    ) -> Result<MetadataRaftResponse, OpenraftAdapterError> {
        self.submit_command(MetadataRaftCommand::CompareAndSetAttribute {
            key: key.into(),
            expected,
            value: value.into(),
        })
        .await
    }

    /// Remove one cluster attribute (leader-only merge).
    pub async fn remove_attribute(
        &self,
        key: impl Into<String>,
    ) -> Result<MetadataRaftResponse, OpenraftAdapterError> {
        self.submit_command(MetadataRaftCommand::RemoveAttribute { key: key.into() })
            .await
    }

    /// Committed coordination snapshot as applied on this node.
    pub fn committed_snapshot(&self) -> CoordinationSnapshot {
        self.state_machine.committed_snapshot()
    }

    /// Watch stream of committed snapshots, updated as raft applies entries.
    ///
    /// This is the sync consumption surface: callers can read or await changes
    /// without touching raft. Matches the `watch::Receiver<CoordinationSnapshot>`
    /// shape fibril's `Coordination` trait expects.
    pub fn watch_committed(&self) -> tokio::sync::watch::Receiver<CoordinationSnapshot> {
        self.state_machine.watch_committed()
    }

    pub fn node_id(&self) -> NodeId {
        self.id
    }

    pub async fn current_leader(&self) -> Option<NodeId> {
        self.raft.current_leader().await
    }

    pub async fn is_leader(&self) -> bool {
        self.raft.is_leader().await.is_ok()
    }

    /// Wait until this node observes the given leader (test/bootstrap helper).
    pub async fn wait_for_leader(
        &self,
        leader: NodeId,
        timeout: Duration,
    ) -> Result<(), OpenraftAdapterError> {
        self.raft
            .wait(Some(timeout))
            .current_leader(leader, "wait_for_leader")
            .await
            .map(|_| ())
            .map_err(|error| OpenraftAdapterError::Config(error.to_string()))
    }

    /// Wait until any leader is elected, returning its id.
    pub async fn wait_for_any_leader(
        &self,
        timeout: Duration,
    ) -> Result<NodeId, OpenraftAdapterError> {
        let metrics = self
            .raft
            .wait(Some(timeout))
            .metrics(
                |metrics| metrics.current_leader.is_some(),
                "wait_for_any_leader",
            )
            .await
            .map_err(|error| OpenraftAdapterError::Config(error.to_string()))?;
        metrics
            .current_leader
            .ok_or_else(|| OpenraftAdapterError::Config("leader vanished after wait".to_string()))
    }

    /// Wait until this node has applied at least `index` (replication checkpoint).
    pub async fn wait_for_applied_index(
        &self,
        index: u64,
        timeout: Duration,
    ) -> Result<(), OpenraftAdapterError> {
        self.raft
            .wait(Some(timeout))
            .metrics(
                move |metrics| metrics.last_applied.map(|id| id.index) >= Some(index),
                "wait_for_applied_index",
            )
            .await
            .map(|_| ())
            .map_err(|error| OpenraftAdapterError::Config(error.to_string()))
    }

    /// Add a learner node: it receives replication but does not vote.
    ///
    /// `blocking` waits until the learner has caught up to the leader's log.
    /// Leader-only; non-leaders surface `NotLeader`.
    pub async fn add_learner(
        &self,
        id: NodeId,
        node: BasicNode,
        blocking: bool,
    ) -> Result<(), OpenraftAdapterError> {
        self.raft
            .add_learner(id, node, blocking)
            .await
            .map(|_| ())
            .map_err(map_membership_error)
    }

    /// Change the voter set. Members must already be learners (or voters).
    ///
    /// With `retain`, demoted voters stay on as learners; otherwise they are
    /// removed from the cluster entirely. Leader-only.
    pub async fn change_membership(
        &self,
        voters: impl IntoIterator<Item = NodeId>,
        retain: bool,
    ) -> Result<(), OpenraftAdapterError> {
        let voters: std::collections::BTreeSet<NodeId> = voters.into_iter().collect();
        self.raft
            .change_membership(voters, retain)
            .await
            .map(|_| ())
            .map_err(map_membership_error)
    }

    /// Serializable view of the raft group from this node's metrics.
    ///
    /// Sync and cheap: reads the metrics watch channel and the committed
    /// snapshot. This JSON is the contract topology CLIs / admin UIs consume.
    pub fn topology(&self) -> RaftTopology {
        let metrics = self.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership().clone();
        let voters: Vec<NodeId> = membership.voter_ids().collect();
        let learners: Vec<NodeId> = membership.learner_ids().collect();
        let nodes = membership
            .nodes()
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect();

        RaftTopology {
            local_id: self.id,
            leader: metrics.current_leader,
            voters,
            learners,
            nodes,
            last_applied_index: metrics.last_applied.map(|id| id.index),
            snapshot_index: metrics.snapshot.map(|id| id.index),
            committed_generation: self.state_machine.committed_snapshot().generation,
        }
    }

    /// Combined durability counters: state-machine snapshot persistence plus
    /// (for durable nodes) WAL append/fsync/compaction/replay counts.
    pub fn telemetry(&self) -> super::StorageTelemetrySnapshot {
        let sm = self.state_machine.telemetry();
        let Some(log) = &self.log_telemetry else {
            return sm;
        };
        let log = log.snapshot();
        super::StorageTelemetrySnapshot {
            appended_records: log.appended_records,
            appended_batches: log.appended_batches,
            fsyncs: log.fsyncs,
            compactions: log.compactions,
            replayed_records_last_open: log.replayed_records_last_open,
            snapshot_persists: sm.snapshot_persists,
            snapshot_loads: sm.snapshot_loads,
        }
    }

    /// Access the raw raft handle for membership changes and metrics.
    pub fn raft(&self) -> &Raft<GanglionRaftConfig, NF, LS, GanglionStateMachine> {
        &self.raft
    }

    pub async fn shutdown(&self) -> Result<(), OpenraftAdapterError> {
        self.raft
            .shutdown()
            .await
            .map_err(|error| OpenraftAdapterError::Storage(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> Arc<Config> {
        // Tight timeouts keep the election fast in tests.
        let config = Config {
            heartbeat_interval: 50,
            election_timeout_min: 150,
            election_timeout_max: 300,
            ..Default::default()
        };
        Arc::new(config.validate().expect("test config should validate"))
    }

    fn basic_members(ids: &[NodeId]) -> BTreeMap<NodeId, BasicNode> {
        ids.iter()
            .map(|id| (*id, BasicNode::new(format!("node-{id}"))))
            .collect()
    }

    async fn start_cluster(router: &InProcessRouter, ids: &[NodeId]) -> Vec<RaftMetadataNode> {
        let config = test_config();
        let mut nodes = Vec::new();
        for id in ids {
            nodes.push(
                RaftMetadataNode::start(*id, config.clone(), router)
                    .await
                    .expect("node should start"),
            );
        }
        nodes[0]
            .initialize(basic_members(ids))
            .await
            .expect("cluster should initialize");
        nodes
    }

    #[test]
    fn three_node_cluster_elects_replicates_and_rejects_stale() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");

        rt.block_on(async {
            let router = InProcessRouter::new();
            let nodes = start_cluster(&router, &[1, 2, 3]).await;

            let timeout = Duration::from_secs(10);
            let leader_id = nodes[0]
                .wait_for_any_leader(timeout)
                .await
                .expect("a leader should be elected");
            let leader = nodes
                .iter()
                .find(|node| node.node_id() == leader_id)
                .expect("leader handle");

            // Leader write commits and reports acceptance.
            let snapshot = CoordinationSnapshot {
                generation: 1,
                ..CoordinationSnapshot::default()
            };
            let response = leader
                .write_snapshot(snapshot.clone())
                .await
                .expect("leader write should commit");
            assert!(response.accepted);
            assert_eq!(response.snapshot.generation, 1);

            // A follower watch subscriber observes the committed snapshot.
            let mut follower_watch = nodes
                .iter()
                .find(|node| node.node_id() != leader_id)
                .expect("cluster has followers")
                .watch_committed();
            tokio::time::timeout(timeout, async {
                while follower_watch.borrow_and_update().generation < 1 {
                    follower_watch
                        .changed()
                        .await
                        .expect("watch should stay open");
                }
            })
            .await
            .expect("follower watch should observe the committed write");
            assert_eq!(follower_watch.borrow().clone(), snapshot);

            // All nodes converge to the committed snapshot.
            let applied = leader
                .state_machine
                .last_applied()
                .expect("leader applied the write")
                .index;
            for node in &nodes {
                node.wait_for_applied_index(applied, timeout)
                    .await
                    .expect("follower should catch up");
                assert_eq!(node.committed_snapshot(), snapshot);
            }

            // Topology agreement: every node reports the same leader, voters,
            // and committed generation.
            for node in &nodes {
                let topology = node.topology();
                assert_eq!(topology.local_id, node.node_id());
                assert_eq!(topology.leader, Some(leader_id));
                assert_eq!(topology.voters, vec![1, 2, 3]);
                assert!(topology.learners.is_empty());
                assert_eq!(topology.nodes.len(), 3);
                assert_eq!(topology.committed_generation, 1);
            }

            // Stale generation is rejected after going through consensus.
            let stale = CoordinationSnapshot {
                generation: 0,
                ..CoordinationSnapshot::default()
            };
            let err = leader
                .write_snapshot(stale)
                .await
                .expect_err("stale generation should be rejected");
            assert!(matches!(err, OpenraftAdapterError::StaleGeneration));

            // Non-leader writes report NotLeader.
            if let Some(follower) = nodes.iter().find(|node| node.node_id() != leader_id) {
                let err = follower
                    .write_snapshot(CoordinationSnapshot {
                        generation: 2,
                        ..CoordinationSnapshot::default()
                    })
                    .await
                    .expect_err("follower write should be refused");
                assert!(matches!(err, OpenraftAdapterError::NotLeader));
            }

            for node in &nodes {
                node.shutdown().await.expect("shutdown");
            }
        });
    }

    #[test]
    fn leader_loss_triggers_reelection_and_writes_continue() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");

        rt.block_on(async {
            let router = InProcessRouter::new();
            let nodes = start_cluster(&router, &[1, 2, 3]).await;
            let timeout = Duration::from_secs(10);

            let first_leader = nodes[0]
                .wait_for_any_leader(timeout)
                .await
                .expect("initial election");
            let leader = nodes
                .iter()
                .find(|node| node.node_id() == first_leader)
                .expect("leader handle");
            leader
                .write_snapshot(CoordinationSnapshot {
                    generation: 1,
                    ..CoordinationSnapshot::default()
                })
                .await
                .expect("initial write");

            // Kill the leader: unreachable to peers and shut down.
            router.deregister(first_leader);
            leader.shutdown().await.expect("leader shutdown");

            let survivors: Vec<_> = nodes
                .iter()
                .filter(|node| node.node_id() != first_leader)
                .collect();

            // The two survivors hold quorum and elect a new leader.
            let new_leader_id = tokio::time::timeout(timeout, async {
                loop {
                    for node in &survivors {
                        if node.is_leader().await {
                            return node.node_id();
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await
            .expect("survivors should elect a new leader");
            assert_ne!(new_leader_id, first_leader);

            // Writes continue under the new leader and reach both survivors.
            let new_leader = survivors
                .iter()
                .find(|node| node.node_id() == new_leader_id)
                .expect("new leader handle");
            new_leader
                .write_snapshot(CoordinationSnapshot {
                    generation: 2,
                    ..CoordinationSnapshot::default()
                })
                .await
                .expect("post-failover write should commit");

            for node in &survivors {
                let mut watch = node.watch_committed();
                tokio::time::timeout(timeout, async {
                    while watch.borrow_and_update().generation < 2 {
                        watch.changed().await.expect("watch open");
                    }
                })
                .await
                .expect("survivor should observe post-failover write");
            }

            for node in &survivors {
                node.shutdown().await.expect("shutdown");
            }
        });
    }

    #[test]
    fn partitioned_follower_rejoins_and_catches_up() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");

        rt.block_on(async {
            let router = InProcessRouter::new();
            let nodes = start_cluster(&router, &[1, 2, 3]).await;
            let timeout = Duration::from_secs(10);

            let leader_id = nodes[0]
                .wait_for_any_leader(timeout)
                .await
                .expect("initial election");

            let partitioned = nodes
                .iter()
                .find(|node| node.node_id() != leader_id)
                .expect("a follower to partition");
            let partitioned_id = partitioned.node_id();

            // Partition the follower (inbound RPCs fail as Unreachable).
            router.deregister(partitioned_id);

            // Quorum of 2/3 still commits. The partitioned node may force term
            // churn via vote requests, so retry on transient NotLeader.
            let write = CoordinationSnapshot {
                generation: 1,
                ..CoordinationSnapshot::default()
            };
            tokio::time::timeout(timeout, async {
                loop {
                    let current = nodes[0]
                        .wait_for_any_leader(timeout)
                        .await
                        .expect("quorum holds a leader");
                    if current == partitioned_id {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    let node = nodes
                        .iter()
                        .find(|node| node.node_id() == current)
                        .expect("leader handle");
                    match node.write_snapshot(write.clone()).await {
                        Ok(_) => break,
                        Err(OpenraftAdapterError::NotLeader) => {
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                        Err(other) => panic!("unexpected write failure: {other}"),
                    }
                }
            })
            .await
            .expect("write should commit during partition");

            // Heal the partition; the follower catches up.
            router.register(partitioned_id, partitioned.raft().clone());
            let mut watch = partitioned.watch_committed();
            tokio::time::timeout(timeout, async {
                while watch.borrow_and_update().generation < 1 {
                    watch.changed().await.expect("watch open");
                }
            })
            .await
            .expect("rejoined follower should catch up");
            assert_eq!(partitioned.committed_snapshot().generation, 1);

            for node in &nodes {
                node.shutdown().await.expect("shutdown");
            }
        });
    }

    #[test]
    fn racing_guarded_controllers_never_lose_updates() {
        use ganglion_core::{NodeInfo as GNodeInfo, PartitionAssignment, ResourceIdentity};

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("runtime");

        rt.block_on(async {
            let router = InProcessRouter::new();
            let nodes = start_cluster(&router, &[1, 2, 3]).await;
            let timeout = Duration::from_secs(10);

            let leader_id = nodes[0]
                .wait_for_any_leader(timeout)
                .await
                .expect("election");
            let leader = nodes
                .iter()
                .find(|node| node.node_id() == leader_id)
                .expect("leader handle");

            // Watch sampler: any observed sequence must be monotonic in
            // generation and per-resource epoch (watch may coalesce; that's
            // fine — monotonicity must hold on every sampled subsequence).
            let resource = ResourceIdentity::new("ns", "contended", 0, None::<String>);
            let mut sampler = leader.watch_committed();
            let sampler_resource = resource.clone();
            let sampler_task = tokio::spawn(async move {
                let mut last_generation = 0u64;
                let mut last_epoch = 0u64;
                while sampler.changed().await.is_ok() {
                    let snapshot = sampler.borrow_and_update().clone();
                    assert!(
                        snapshot.generation >= last_generation,
                        "generation regressed: {} -> {}",
                        last_generation,
                        snapshot.generation
                    );
                    last_generation = snapshot.generation;
                    if let Some(assignment) = snapshot.assignments.get(&sampler_resource) {
                        assert!(
                            assignment.epoch >= last_epoch,
                            "epoch regressed: {} -> {}",
                            last_epoch,
                            assignment.epoch
                        );
                        last_epoch = assignment.epoch;
                    }
                }
            });

            // Two controllers race: each wants the contended resource owned by
            // its own broker. Every accepted proposal flips ownership, so the
            // epoch must bump nearly every round — and CAS retries must absorb
            // all interleaving without lost updates.
            const ROUNDS: usize = 20;
            let controller = |owner: &'static str| {
                let node = leader;
                let resource = resource.clone();
                async move {
                    let mut accepted = 0u64;
                    for _ in 0..ROUNDS {
                        let resource = resource.clone();
                        let response = node
                            .plan_and_propose_guarded(
                                move |committed| {
                                    let mut desired = committed.clone();
                                    desired.nodes.insert(
                                        owner.to_string(),
                                        GNodeInfo::new(owner, "127.0.0.1:0", None::<String>),
                                    );
                                    desired.assignments.insert(
                                        resource.clone(),
                                        PartitionAssignment::new(
                                            resource.clone(),
                                            owner,
                                            vec![],
                                            0, // stamped by the helper
                                        ),
                                    );
                                    desired
                                },
                                64,
                            )
                            .await
                            .expect("guarded proposal should eventually win");
                        assert!(response.accepted);
                        accepted += 1;
                    }
                    accepted
                }
            };

            let (a_accepted, b_accepted) =
                tokio::join!(controller("broker-a"), controller("broker-b"));

            // No lost updates: every accepted proposal advanced the generation
            // by exactly one.
            let final_snapshot = leader.committed_snapshot();
            assert_eq!(final_snapshot.generation, a_accepted + b_accepted);
            let final_assignment = &final_snapshot.assignments[&resource];
            assert!(final_assignment.epoch >= 1);
            assert!(final_assignment.owner == "broker-a" || final_assignment.owner == "broker-b");

            drop(sampler_task);
            for node in &nodes {
                node.shutdown().await.expect("shutdown");
            }
        });
    }

    #[test]
    fn learner_joins_catches_up_and_gets_promoted() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");

        rt.block_on(async {
            let router = InProcessRouter::new();
            let nodes = start_cluster(&router, &[1, 2, 3]).await;
            let timeout = Duration::from_secs(10);

            let leader_id = nodes[0]
                .wait_for_any_leader(timeout)
                .await
                .expect("initial election");
            let leader = nodes
                .iter()
                .find(|node| node.node_id() == leader_id)
                .expect("leader handle");

            leader
                .write_snapshot(CoordinationSnapshot {
                    generation: 1,
                    ..CoordinationSnapshot::default()
                })
                .await
                .expect("pre-join write");

            // Node 4 joins as a learner and catches up (blocking add).
            let joiner = RaftMetadataNode::start(4, test_config(), &router)
                .await
                .expect("joiner should start");
            leader
                .add_learner(4, BasicNode::new("node-4"), true)
                .await
                .expect("learner should be added and caught up");
            assert_eq!(joiner.committed_snapshot().generation, 1);

            // Followers cannot drive membership changes.
            let follower = nodes
                .iter()
                .find(|node| node.node_id() != leader_id)
                .expect("a follower");
            let err = follower
                .add_learner(5, BasicNode::new("node-5"), false)
                .await
                .expect_err("follower add_learner must be refused");
            assert!(matches!(err, OpenraftAdapterError::NotLeader));

            // Promote the learner into the voter set (keep all four voters).
            leader
                .change_membership([1, 2, 3, 4], false)
                .await
                .expect("promotion should commit");

            // New voter observes subsequent writes.
            leader
                .write_snapshot(CoordinationSnapshot {
                    generation: 2,
                    ..CoordinationSnapshot::default()
                })
                .await
                .expect("post-promotion write");
            let mut watch = joiner.watch_committed();
            tokio::time::timeout(timeout, async {
                while watch.borrow_and_update().generation < 2 {
                    watch.changed().await.expect("watch open");
                }
            })
            .await
            .expect("promoted voter should observe new writes");

            // Shrink back to three voters, dropping node 1 entirely.
            leader
                .change_membership([2, 3, 4], false)
                .await
                .expect("removal should commit");
            let topology = leader.topology();
            assert_eq!(topology.voters, vec![2, 3, 4]);
            assert!(topology.learners.is_empty());
            assert!(topology.nodes.contains_key(&4));

            for node in nodes.iter().chain(std::iter::once(&joiner)) {
                node.shutdown().await.expect("shutdown");
            }
        });
    }

    #[test]
    fn durable_node_bounded_recovery_survives_purge_across_restart() {
        use super::super::FileRaftLogStore;

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");

        rt.block_on(async {
            let dir = std::env::temp_dir().join(format!(
                "ganglion-bounded-recovery-{}-{:?}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos(),
            ));
            let timeout = Duration::from_secs(10);

            // Aggressive snapshot/purge so the test exercises the bound cheaply.
            let config = Arc::new(
                Config {
                    heartbeat_interval: 50,
                    election_timeout_min: 150,
                    election_timeout_max: 300,
                    snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(16),
                    max_in_snapshot_log_to_keep: 4,
                    ..Default::default()
                }
                .validate()
                .expect("config"),
            );

            const WRITES: u64 = 100;
            {
                let router = InProcessRouter::<FileRaftLogStore>::new();
                let node = RaftMetadataNode::start_durable(1, config.clone(), &router, &dir)
                    .await
                    .expect("durable node should start");
                node.initialize(basic_members(&[1]))
                    .await
                    .expect("initialize");
                node.wait_for_leader(1, timeout).await.expect("election");

                for generation in 1..=WRITES {
                    node.write_snapshot(CoordinationSnapshot {
                        generation,
                        ..CoordinationSnapshot::default()
                    })
                    .await
                    .expect("write");
                }

                // Wait for snapshot+purge to actually trim the log.
                node.raft()
                    .wait(Some(timeout))
                    .metrics(
                        |metrics| metrics.snapshot.map(|id| id.index).unwrap_or(0) > WRITES / 2,
                        "snapshot built",
                    )
                    .await
                    .expect("snapshot should be built under aggressive policy");

                // Durability telemetry moved with the workload.
                let telemetry = node.telemetry();
                assert!(telemetry.appended_records >= WRITES);
                assert!(telemetry.fsyncs >= WRITES);
                assert!(telemetry.compactions >= 1, "purge should compact the WAL");
                assert!(telemetry.snapshot_persists >= 1);

                node.shutdown().await.expect("shutdown");
                router.deregister(1);
            }

            // The WAL must be bounded: far fewer surviving records than writes.
            let wal_lines = std::fs::read_to_string(dir.join("raft-wal.jsonl"))
                .expect("WAL exists")
                .lines()
                .count();
            assert!(
                wal_lines < 60,
                "WAL should stay bounded after purge, found {wal_lines} records"
            );
            assert!(
                dir.join("snapshot.json").exists(),
                "persisted snapshot must exist"
            );

            // Full restart: state must come back even though the log was purged.
            let router = InProcessRouter::<FileRaftLogStore>::new();
            let restarted = RaftMetadataNode::start_durable(1, config, &router, &dir)
                .await
                .expect("restart");
            // Restart telemetry: bounded replay and a snapshot load.
            let restart_telemetry = restarted.telemetry();
            assert!(restart_telemetry.replayed_records_last_open < 60);
            assert_eq!(restart_telemetry.snapshot_loads, 1);
            // Pre-election state equals the persisted snapshot — at most the
            // short WAL tail (snapshot threshold + keep) behind the last write.
            let pre_election = restarted.committed_snapshot().generation;
            assert!(
                pre_election >= WRITES - 16 - 4,
                "snapshot restore must be within the configured tail bound, got {pre_election}"
            );
            restarted
                .wait_for_leader(1, timeout)
                .await
                .expect("re-election");
            // Re-committing the WAL tail recovers the full committed state.
            let mut watch = restarted.watch_committed();
            tokio::time::timeout(timeout, async {
                while watch.borrow_and_update().generation < WRITES {
                    watch.changed().await.expect("watch open");
                }
            })
            .await
            .expect("WAL tail replay should recover the final generation");
            assert_eq!(restarted.committed_snapshot().generation, WRITES);

            restarted.shutdown().await.expect("shutdown");
        });
    }

    #[test]
    fn durable_node_recovers_committed_state_after_restart() {
        use super::super::FileRaftLogStore;

        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");

        rt.block_on(async {
            let wal_path = std::env::temp_dir().join(format!(
                "ganglion-durable-node-{}-{:?}.jsonl",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos(),
            ));
            let timeout = Duration::from_secs(10);

            {
                let router = InProcessRouter::<FileRaftLogStore>::new();
                let store = FileRaftLogStore::open(&wal_path).expect("open WAL");
                let node = RaftMetadataNode::start_with_store(1, test_config(), &router, store)
                    .await
                    .expect("durable node should start");
                node.initialize(basic_members(&[1]))
                    .await
                    .expect("single node should initialize");
                node.wait_for_leader(1, timeout)
                    .await
                    .expect("single node should elect itself");

                for generation in 1..=3 {
                    node.write_snapshot(CoordinationSnapshot {
                        generation,
                        ..CoordinationSnapshot::default()
                    })
                    .await
                    .expect("write should commit");
                }
                assert_eq!(node.committed_snapshot().generation, 3);

                node.shutdown().await.expect("shutdown");
                router.deregister(1);
            }

            // Restart from the same WAL: no initialize — membership, vote, and
            // log come from disk; the fresh in-memory state machine catches up
            // once the node re-elects itself and re-commits the log.
            let router = InProcessRouter::<FileRaftLogStore>::new();
            let store = FileRaftLogStore::open(&wal_path).expect("reopen WAL");
            let restarted = RaftMetadataNode::start_with_store(1, test_config(), &router, store)
                .await
                .expect("restarted node should start");
            restarted
                .wait_for_leader(1, timeout)
                .await
                .expect("restarted node should re-elect itself from durable state");

            let mut watch = restarted.watch_committed();
            tokio::time::timeout(timeout, async {
                while watch.borrow_and_update().generation < 3 {
                    watch.changed().await.expect("watch open");
                }
            })
            .await
            .expect("restarted node should recover generation 3");
            assert_eq!(restarted.committed_snapshot().generation, 3);

            restarted.shutdown().await.expect("shutdown restarted");
        });
    }
}
