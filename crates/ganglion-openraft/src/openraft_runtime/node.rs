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
use openraft::{BasicNode, Config, Raft};

use crate::OpenraftAdapterError;

use super::{
    GanglionLogStore, GanglionRaft, GanglionStateMachine, InProcessRouter, MetadataRaftCommand,
    MetadataRaftResponse,
};

type NodeId = u64;

/// One raft-backed metadata node living inside a process-local cluster.
pub struct RaftMetadataNode {
    id: NodeId,
    raft: GanglionRaft,
    state_machine: GanglionStateMachine,
}

impl RaftMetadataNode {
    /// Start a node and register it on the router so peers can reach it.
    pub async fn start(
        id: NodeId,
        config: Arc<Config>,
        router: &InProcessRouter,
    ) -> Result<Self, OpenraftAdapterError> {
        let state_machine = GanglionStateMachine::default();
        let raft = Raft::new(
            id,
            config,
            router.clone(),
            GanglionLogStore::default(),
            state_machine.clone(),
        )
        .await
        .map_err(|error| OpenraftAdapterError::Storage(error.to_string()))?;
        router.register(id, raft.clone());
        Ok(Self {
            id,
            raft,
            state_machine,
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
        let result = self
            .raft
            .client_write(MetadataRaftCommand::ApplySnapshot(snapshot))
            .await;

        let response = match result {
            Ok(response) => response.data,
            Err(error) => {
                return if error.forward_to_leader().is_some() {
                    Err(OpenraftAdapterError::NotLeader)
                } else {
                    Err(OpenraftAdapterError::Storage(error.to_string()))
                };
            }
        };

        if !response.accepted {
            return Err(OpenraftAdapterError::StaleGeneration);
        }
        Ok(response)
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

    /// Access the raw raft handle for membership changes and metrics.
    pub fn raft(&self) -> &GanglionRaft {
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

    async fn start_cluster(
        router: &InProcessRouter,
        ids: &[NodeId],
    ) -> Vec<RaftMetadataNode> {
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
                    follower_watch.changed().await.expect("watch should stay open");
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
}
