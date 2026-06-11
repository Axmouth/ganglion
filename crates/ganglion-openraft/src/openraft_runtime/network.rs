//! In-process `RaftNetwork`/`RaftNetworkFactory` router.
//!
//! Routes RPCs between `Raft<GanglionRaftConfig>` instances living in the same
//! process by calling the target's `Raft` handle directly. Generic over the log
//! store so in-memory and durable nodes use the same transport. A wire
//! transport can implement the same traits later without touching storage or
//! runtime-node code.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use openraft::async_trait::async_trait;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::RaftLogStorage;
use openraft::{BasicNode, Raft, RaftNetwork, RaftNetworkFactory};

use super::{GanglionLogStore, GanglionRaftConfig, GanglionStateMachine};

type NodeId = u64;

/// Raft handle type for an in-process cluster over log store `LS`.
pub type GanglionRaftOf<LS> =
    Raft<GanglionRaftConfig, InProcessRouter<LS>, LS, GanglionStateMachine>;

/// Raft handle type for the default in-memory log store.
pub type GanglionRaft = GanglionRaftOf<GanglionLogStore>;

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
pub struct InProcessRouter<LS = GanglionLogStore>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
{
    targets: Arc<RwLock<BTreeMap<NodeId, GanglionRaftOf<LS>>>>,
}

impl<LS> Clone for InProcessRouter<LS>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
{
    fn clone(&self) -> Self {
        Self {
            targets: Arc::clone(&self.targets),
        }
    }
}

impl<LS> Default for InProcessRouter<LS>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
{
    fn default() -> Self {
        Self {
            targets: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
}

impl<LS> InProcessRouter<LS>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
{
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a node's raft handle so peers can reach it.
    pub fn register(&self, id: NodeId, raft: GanglionRaftOf<LS>) {
        self.targets.write().unwrap().insert(id, raft);
    }

    /// Remove a node from the router, simulating an unreachable peer.
    pub fn deregister(&self, id: NodeId) {
        self.targets.write().unwrap().remove(&id);
    }

    fn lookup(&self, id: NodeId) -> Option<GanglionRaftOf<LS>> {
        self.targets.read().unwrap().get(&id).cloned()
    }
}

#[async_trait]
impl<LS> RaftNetworkFactory<GanglionRaftConfig> for InProcessRouter<LS>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
{
    type Network = InProcessConnection<LS>;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        InProcessConnection {
            router: self.clone(),
            target,
        }
    }
}

/// A connection to one target node, resolved through the router on every call
/// so restarts/replacements are picked up.
pub struct InProcessConnection<LS = GanglionLogStore>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
{
    router: InProcessRouter<LS>,
    target: NodeId,
}

impl<LS> InProcessConnection<LS>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
{
    fn target_raft(&self) -> Result<GanglionRaftOf<LS>, Unreachable> {
        self.router
            .lookup(self.target)
            .ok_or_else(|| Unreachable::new(&UnknownTarget(self.target)))
    }
}

#[async_trait]
impl<LS> RaftNetwork<GanglionRaftConfig> for InProcessConnection<LS>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
{
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
