//! TCP wire transport for `RaftNetwork`.
//!
//! Frame layout: `[1-byte format tag][4-byte BE body length][body]`.
//! The tag selects the body encoding — receivers always accept both formats,
//! senders choose via [`WireFormat`] (MessagePack by default; JSON for
//! debugging, e.g. `GANGLION_WIRE_FORMAT=json`). This makes format transitions
//! and mixed-version clusters a non-event.
//!
//! Peer addresses travel in raft membership (`BasicNode.addr`), so the network
//! factory needs no static peer table. Connections are lazy and re-established
//! per call after IO failures; failures surface as `Unreachable` so openraft
//! applies its backoff policy.

use std::io;

use openraft::async_trait::async_trait;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::RaftLogStorage;
use openraft::{BasicNode, Raft, RaftNetwork, RaftNetworkFactory};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::{GanglionRaftConfig, GanglionStateMachine};

type NodeId = u64;

/// Body encoding, carried per frame as a single-byte tag.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Default)]
pub enum WireFormat {
    /// Compact binary (default): native byte handling for snapshot chunks.
    #[default]
    MessagePack,
    /// Human-readable; handy with tcpdump/wireshark while debugging.
    Json,
}

impl WireFormat {
    const TAG_MSGPACK: u8 = 0x01;
    const TAG_JSON: u8 = 0x02;

    /// Convenience for binaries/examples: honor `GANGLION_WIRE_FORMAT`
    /// (json|msgpack, default msgpack). Libraries must not call this — wire
    /// formats flow through startup configuration.
    pub fn from_env() -> Self {
        match std::env::var("GANGLION_WIRE_FORMAT").as_deref() {
            Ok("json") => Self::Json,
            _ => Self::MessagePack,
        }
    }

    fn tag(self) -> u8 {
        match self {
            Self::MessagePack => Self::TAG_MSGPACK,
            Self::Json => Self::TAG_JSON,
        }
    }

    fn from_tag(tag: u8) -> io::Result<Self> {
        match tag {
            Self::TAG_MSGPACK => Ok(Self::MessagePack),
            Self::TAG_JSON => Ok(Self::Json),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown wire format tag {other:#04x}"),
            )),
        }
    }

    fn encode<T: Serialize>(self, value: &T) -> io::Result<Vec<u8>> {
        match self {
            Self::MessagePack => rmp_serde::to_vec(value)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
            Self::Json => serde_json::to_vec(value)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
        }
    }

    fn decode<T: for<'de> Deserialize<'de>>(self, bytes: &[u8]) -> io::Result<T> {
        match self {
            Self::MessagePack => rmp_serde::from_slice(bytes)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
            Self::Json => serde_json::from_slice(bytes)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error)),
        }
    }
}

impl std::str::FromStr for WireFormat {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "msgpack" | "messagepack" => Ok(Self::MessagePack),
            "json" => Ok(Self::Json),
            other => Err(format!("unknown wire format `{other}` (msgpack|json)")),
        }
    }
}

/// Upper bound on a single frame body; metadata RPCs are small, snapshots are
/// bounded by the coordination state size.
const MAX_FRAME_BYTES: u32 = 64 * 1024 * 1024;

async fn write_frame<T: Serialize>(
    stream: &mut TcpStream,
    format: WireFormat,
    value: &T,
) -> io::Result<()> {
    let body = format.encode(value)?;
    let length = u32::try_from(body.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "frame too large"))?;
    if length > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    stream.write_all(&[format.tag()]).await?;
    stream.write_all(&length.to_be_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await
}

async fn read_frame<T: for<'de> Deserialize<'de>>(stream: &mut TcpStream) -> io::Result<T> {
    let mut tag = [0u8; 1];
    stream.read_exact(&mut tag).await?;
    let format = WireFormat::from_tag(tag[0])?;

    let mut length = [0u8; 4];
    stream.read_exact(&mut length).await?;
    let length = u32::from_be_bytes(length);
    if length > MAX_FRAME_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }

    let mut body = vec![0u8; length as usize];
    stream.read_exact(&mut body).await?;
    format.decode(&body)
}

#[derive(Serialize, Deserialize)]
enum WireRequest {
    AppendEntries(AppendEntriesRequest<GanglionRaftConfig>),
    Vote(VoteRequest<NodeId>),
    InstallSnapshot(InstallSnapshotRequest<GanglionRaftConfig>),
}

#[derive(Serialize, Deserialize)]
enum WireResponse {
    AppendEntries(Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>),
    Vote(Result<VoteResponse<NodeId>, RaftError<NodeId>>),
    InstallSnapshot(
        Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
    ),
}

/// Listener task serving raft RPCs for one local node.
pub struct TcpRaftServer {
    local_addr: std::net::SocketAddr,
    handle: tokio::task::JoinHandle<()>,
}

impl TcpRaftServer {
    /// Bind `listen_addr` (use port 0 for ephemeral) and serve the raft handle.
    ///
    /// `format` selects the encoding of this node's replies; inbound frames
    /// are decoded by their own tag regardless. Settings policy: callers pass
    /// the format from their startup configuration — the library reads no
    /// environment variables.
    pub async fn bind<NF, LS>(
        listen_addr: impl tokio::net::ToSocketAddrs,
        raft: Raft<GanglionRaftConfig, NF, LS, GanglionStateMachine>,
        format: WireFormat,
    ) -> io::Result<Self>
    where
        NF: RaftNetworkFactory<GanglionRaftConfig>,
        LS: RaftLogStorage<GanglionRaftConfig>,
    {
        let listener = TcpListener::bind(listen_addr).await?;
        let local_addr = listener.local_addr()?;

        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _peer)) = listener.accept().await else {
                    break;
                };
                let raft = raft.clone();
                tokio::spawn(async move {
                    let _ = serve_connection(stream, raft, format).await;
                });
            }
        });

        Ok(Self { local_addr, handle })
    }

    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.local_addr
    }

    /// Stop accepting; in-flight connections finish on their own tasks.
    pub fn shutdown(&self) {
        self.handle.abort();
    }
}

impl Drop for TcpRaftServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn serve_connection<NF, LS>(
    mut stream: TcpStream,
    raft: Raft<GanglionRaftConfig, NF, LS, GanglionStateMachine>,
    format: WireFormat,
) -> io::Result<()>
where
    NF: RaftNetworkFactory<GanglionRaftConfig>,
    LS: RaftLogStorage<GanglionRaftConfig>,
{
    loop {
        let request: WireRequest = match read_frame(&mut stream).await {
            Ok(request) => request,
            // Peer closed between requests: normal connection end.
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error),
        };
        let response = match request {
            WireRequest::AppendEntries(rpc) => {
                WireResponse::AppendEntries(raft.append_entries(rpc).await)
            }
            WireRequest::Vote(rpc) => WireResponse::Vote(raft.vote(rpc).await),
            WireRequest::InstallSnapshot(rpc) => {
                WireResponse::InstallSnapshot(raft.install_snapshot(rpc).await)
            }
        };
        write_frame(&mut stream, format, &response).await?;
    }
}

/// `RaftNetworkFactory` resolving peers from membership (`BasicNode.addr`).
#[derive(Debug, Clone, Default)]
pub struct TcpNetworkFactory {
    format: WireFormat,
}

impl TcpNetworkFactory {
    /// Factory sending MessagePack frames (the default format).
    pub fn new() -> Self {
        Self::default()
    }

    /// Factory sending the given format. Settings policy: pass this from
    /// startup configuration, not from environment reads inside libraries.
    pub fn with_format(format: WireFormat) -> Self {
        Self { format }
    }
}

#[async_trait]
impl RaftNetworkFactory<GanglionRaftConfig> for TcpNetworkFactory {
    type Network = TcpRaftConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        TcpRaftConnection {
            target,
            addr: node.addr.clone(),
            format: self.format,
            stream: None,
        }
    }
}

/// Lazy, self-healing connection to one raft peer.
pub struct TcpRaftConnection {
    target: NodeId,
    addr: String,
    format: WireFormat,
    stream: Option<TcpStream>,
}

impl TcpRaftConnection {
    async fn call(&mut self, request: WireRequest) -> Result<WireResponse, Unreachable> {
        let result = self.try_call(&request).await;
        if result.is_err() {
            // Drop the broken connection; the next call reconnects.
            self.stream = None;
        }
        result.map_err(|error| Unreachable::new(&error))
    }

    async fn try_call(&mut self, request: &WireRequest) -> io::Result<WireResponse> {
        if self.stream.is_none() {
            self.stream = Some(TcpStream::connect(&self.addr).await?);
        }
        let stream = self.stream.as_mut().expect("stream just ensured");
        write_frame(stream, self.format, request).await?;
        read_frame(stream).await
    }
}

#[async_trait]
impl RaftNetwork<GanglionRaftConfig> for TcpRaftConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<GanglionRaftConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        match self.call(WireRequest::AppendEntries(rpc)).await? {
            WireResponse::AppendEntries(Ok(response)) => Ok(response),
            WireResponse::AppendEntries(Err(error)) => {
                Err(RPCError::RemoteError(RemoteError::new(self.target, error)))
            }
            _ => Err(mismatched_response(self.target)),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<GanglionRaftConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        match self.call(WireRequest::InstallSnapshot(rpc)).await? {
            WireResponse::InstallSnapshot(Ok(response)) => Ok(response),
            WireResponse::InstallSnapshot(Err(error)) => {
                Err(RPCError::RemoteError(RemoteError::new(self.target, error)))
            }
            _ => Err(mismatched_response(self.target)),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        match self.call(WireRequest::Vote(rpc)).await? {
            WireResponse::Vote(Ok(response)) => Ok(response),
            WireResponse::Vote(Err(error)) => {
                Err(RPCError::RemoteError(RemoteError::new(self.target, error)))
            }
            _ => Err(mismatched_response(self.target)),
        }
    }
}

fn mismatched_response<E: std::error::Error>(target: NodeId) -> RPCError<NodeId, BasicNode, E> {
    #[derive(Debug)]
    struct Mismatch(NodeId);
    impl std::fmt::Display for Mismatch {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "peer {} answered with a mismatched response variant",
                self.0
            )
        }
    }
    impl std::error::Error for Mismatch {}
    RPCError::Unreachable(Unreachable::new(&Mismatch(target)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::openraft_runtime::{default_raft_config, RaftMetadataNode};
    use ganglion_core::CoordinationSnapshot;
    use std::collections::BTreeMap;
    use std::time::Duration;

    fn unique_dir(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("ganglion-tcp-{tag}-{}-{nanos}", std::process::id()))
    }

    /// Both wire formats roundtrip a request frame, and a JSON sender talks to
    /// the same decoder a msgpack sender uses (mixed setups are a non-event).
    #[tokio::test]
    async fn frames_roundtrip_in_both_formats() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");

        let echo = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let request: WireRequest = read_frame(&mut stream).await.expect("read");
                // Echo back a vote response regardless of request kind.
                let WireRequest::Vote(vote) = request else {
                    panic!("expected vote request");
                };
                let response = WireResponse::Vote(Ok(VoteResponse {
                    vote: vote.vote,
                    vote_granted: true,
                    last_log_id: None,
                }));
                // Reply in msgpack always: the client must decode it even when
                // it sent JSON.
                write_frame(&mut stream, WireFormat::MessagePack, &response)
                    .await
                    .expect("write");
            }
        });

        for format in [WireFormat::MessagePack, WireFormat::Json] {
            let mut stream = TcpStream::connect(addr).await.expect("connect");
            let request = WireRequest::Vote(VoteRequest {
                vote: openraft::Vote::new(1, 1),
                last_log_id: None,
            });
            write_frame(&mut stream, format, &request)
                .await
                .expect("send");
            let response: WireResponse = read_frame(&mut stream).await.expect("recv");
            let WireResponse::Vote(Ok(response)) = response else {
                panic!("unexpected response variant");
            };
            assert!(response.vote_granted);
        }
        echo.await.expect("echo server");
    }

    /// Real multi-node cluster over actual TCP sockets: election, replication,
    /// leader kill + survivor re-election, durable restart and rejoin.
    #[test]
    fn tcp_cluster_elects_replicates_and_survives_leader_loss() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("runtime");

        rt.block_on(async {
            let timeout = Duration::from_secs(15);
            let config = std::sync::Arc::new(
                openraft::Config {
                    heartbeat_interval: 50,
                    election_timeout_min: 200,
                    election_timeout_max: 400,
                    ..default_raft_config().expect("config").as_ref().clone()
                }
                .validate()
                .expect("config"),
            );

            // Start three durable TCP nodes on ephemeral ports.
            let mut nodes = Vec::new();
            let mut servers = Vec::new();
            let mut dirs = Vec::new();
            for id in 1..=3u64 {
                let dir = unique_dir(&format!("node-{id}"));
                let (node, server) =
                    RaftMetadataNode::start_durable_tcp(id, config.clone(), "127.0.0.1:0", &dir)
                        .await
                        .expect("tcp node should start");
                dirs.push(dir);
                nodes.push(node);
                servers.push(server);
            }

            // Membership carries the real socket addresses.
            let members: BTreeMap<u64, BasicNode> = servers
                .iter()
                .enumerate()
                .map(|(index, server)| {
                    (
                        index as u64 + 1,
                        BasicNode::new(server.local_addr().to_string()),
                    )
                })
                .collect();
            nodes[0].initialize(members).await.expect("initialize");

            let leader_id = nodes[0]
                .wait_for_any_leader(timeout)
                .await
                .expect("election over TCP");
            let leader_index = (leader_id - 1) as usize;

            // Replicate a write to every node over the wire.
            nodes[leader_index]
                .write_snapshot(CoordinationSnapshot {
                    generation: 1,
                    ..CoordinationSnapshot::default()
                })
                .await
                .expect("write commits over TCP");
            for node in &nodes {
                let mut watch = node.watch_committed();
                tokio::time::timeout(timeout, async {
                    while watch.borrow_and_update().generation < 1 {
                        watch.changed().await.expect("watch open");
                    }
                })
                .await
                .expect("every node observes the write");
            }

            // Kill the leader process-equivalent: stop its server + raft.
            servers[leader_index].shutdown();
            nodes[leader_index]
                .shutdown()
                .await
                .expect("leader shutdown");

            // Survivors re-elect and keep committing.
            let survivor_index = (0..3)
                .find(|index| *index != leader_index)
                .expect("survivor");
            let new_leader_id = tokio::time::timeout(timeout, async {
                loop {
                    for (index, node) in nodes.iter().enumerate() {
                        if index != leader_index && node.is_leader().await {
                            return node.node_id();
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .expect("survivors re-elect over TCP");
            assert_ne!(new_leader_id, leader_id);

            let new_leader_index = (new_leader_id - 1) as usize;
            nodes[new_leader_index]
                .write_snapshot(CoordinationSnapshot {
                    generation: 2,
                    ..CoordinationSnapshot::default()
                })
                .await
                .expect("post-failover write commits");

            // Restart the killed node from its data dir on a fresh port? No —
            // its old address is baked into membership, so restart on the SAME
            // address (real deployments pin listen addresses).
            let old_addr = servers[leader_index].local_addr();
            let (revived, revived_server) = RaftMetadataNode::start_durable_tcp(
                leader_id,
                config.clone(),
                old_addr,
                &dirs[leader_index],
            )
            .await
            .expect("revived node restarts from its WAL");

            let mut watch = revived.watch_committed();
            tokio::time::timeout(timeout, async {
                while watch.borrow_and_update().generation < 2 {
                    watch.changed().await.expect("watch open");
                }
            })
            .await
            .expect("revived node catches up over TCP");

            // Topology agreement across the wire.
            let survivor_topology = nodes[survivor_index].topology();
            assert_eq!(survivor_topology.leader, Some(new_leader_id));
            assert_eq!(survivor_topology.voters, vec![1, 2, 3]);

            revived.shutdown().await.expect("shutdown revived");
            drop(revived_server);
            for (index, node) in nodes.iter().enumerate() {
                if index != leader_index {
                    node.shutdown().await.expect("shutdown");
                }
            }
        });
    }
}
