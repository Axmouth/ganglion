//! In-process `RaftNetwork`/`RaftNetworkFactory` router.
//!
//! Routes RPCs between `Raft<GanglionRaftConfig>` instances living in the same
//! process by calling the target's `Raft` handle directly. Generic over the log
//! store so in-memory and durable nodes use the same transport. A wire
//! transport can implement the same traits later without touching storage or
//! runtime-node code.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Raft, RaftNetwork, RaftNetworkFactory};

use super::GanglionRaftConfig;

type NodeId = u64;

/// Raft handle type for the ganglion metadata group.
///
/// Since openraft 0.9 the `Raft` handle is type-erased over storage and network
/// (`Raft<C>`), so a single handle type covers in-memory and durable nodes.
pub type GanglionRaft = Raft<GanglionRaftConfig>;

#[derive(Debug)]
struct UnknownTarget(NodeId);

impl std::fmt::Display for UnknownTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "node {} is not registered in the in-process router",
            self.0
        )
    }
}

impl std::error::Error for UnknownTarget {}

/// Shared registry of in-process raft handles. Cheap to clone.
///
/// The stored handles are type-erased `Raft<C>`, so a single router can serve
/// in-memory and durable nodes without a storage type parameter.
#[derive(Default)]
pub struct InProcessRouter {
    targets: Arc<RwLock<BTreeMap<NodeId, GanglionRaft>>>,
}

impl Clone for InProcessRouter {
    fn clone(&self) -> Self {
        Self {
            targets: Arc::clone(&self.targets),
        }
    }
}

impl InProcessRouter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a node's raft handle so peers can reach it.
    pub fn register(&self, id: NodeId, raft: GanglionRaft) {
        self.targets.write().unwrap().insert(id, raft);
    }

    /// Remove a node from the router, simulating an unreachable peer.
    pub fn deregister(&self, id: NodeId) {
        self.targets.write().unwrap().remove(&id);
    }

    fn lookup(&self, id: NodeId) -> Option<GanglionRaft> {
        self.targets.read().unwrap().get(&id).cloned()
    }
}

impl RaftNetworkFactory<GanglionRaftConfig> for InProcessRouter {
    type Network = InProcessConnection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        InProcessConnection {
            router: self.clone(),
            target,
        }
    }
}

/// A connection to one target node, resolved through the router on every call
/// so restarts/replacements are picked up.
pub struct InProcessConnection {
    router: InProcessRouter,
    target: NodeId,
}

impl InProcessConnection {
    fn target_raft(&self) -> Result<GanglionRaft, Unreachable> {
        self.router
            .lookup(self.target)
            .ok_or_else(|| Unreachable::new(&UnknownTarget(self.target)))
    }
}

impl RaftNetwork<GanglionRaftConfig> for InProcessConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<GanglionRaftConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let raft = self.target_raft()?;
        raft.append_entries(rpc)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<GanglionRaftConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let raft = self.target_raft()?;
        raft.install_snapshot(rpc)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let raft = self.target_raft()?;
        raft.vote(rpc)
            .await
            .map_err(|error| RPCError::RemoteError(RemoteError::new(self.target, error)))
    }
}
