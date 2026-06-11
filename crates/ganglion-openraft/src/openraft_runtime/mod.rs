//! Feature-gated openraft (0.8.x) runtime integration.
//!
//! Declares the [`openraft::RaftTypeConfig`] surface, the application
//! command/response payloads, and the storage adapters used by the
//! transport-backed `MetadataConsensus` implementation. Network and runtime
//! node wiring are added incrementally; see `OPENRAFT_SURVIVAL_CONTEXT.md`.

use std::io::Cursor;

use ganglion_core::CoordinationSnapshot;
use serde::{Deserialize, Serialize};

use crate::OpenraftAdapterError;

mod durable;
mod network;
mod node;
mod storage;

pub use durable::FileRaftLogStore;
pub use network::{GanglionRaft, GanglionRaftOf, InProcessConnection, InProcessRouter};
pub use node::RaftMetadataNode;
pub use storage::{GanglionLogStore, GanglionStateMachine};

/// Application-level write submitted through `Raft::client_write`.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum MetadataRaftCommand {
    /// Replace the committed coordination snapshot.
    ApplySnapshot(CoordinationSnapshot),
}

/// Application-level response returned from the state machine.
///
/// `accepted` is `false` when the command was deterministically rejected
/// (stale generation); `snapshot` always carries the post-apply committed state.
#[derive(Debug, Clone, Eq, PartialEq, Default, Serialize, Deserialize)]
pub struct MetadataRaftResponse {
    pub accepted: bool,
    pub snapshot: CoordinationSnapshot,
}

openraft::declare_raft_types!(
    /// Type configuration for the ganglion metadata raft group.
    pub GanglionRaftConfig:
        D = MetadataRaftCommand,
        R = MetadataRaftResponse,
        NodeId = u64,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<GanglionRaftConfig>,
        SnapshotData = Cursor<Vec<u8>>
);

/// Fsync the parent directory of `path` so a preceding rename is durable.
///
/// Crash-consistency: writing tmp + fsync + rename only guarantees the new
/// file content; the directory entry swap itself needs a directory fsync to
/// survive power loss on most filesystems.
pub(crate) fn fsync_parent_dir(path: &std::path::Path) -> std::io::Result<()> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => std::path::Path::new("."),
    };
    std::fs::File::open(parent)?.sync_all()
}

/// Log entries between snapshots before a new snapshot is built.
pub const SNAPSHOT_LOGS_SINCE_LAST: u64 = 256;
/// In-snapshot log entries retained after a purge.
pub const MAX_IN_SNAPSHOT_LOG_TO_KEEP: u64 = 64;

/// Build a validated openraft runtime config tuned for the metadata workload.
///
/// Snapshot/purge thresholds are kept small so the durable WAL — and therefore
/// startup replay — stays bounded at roughly
/// `SNAPSHOT_LOGS_SINCE_LAST + MAX_IN_SNAPSHOT_LOG_TO_KEEP` entries: recovery
/// loads the persisted snapshot and only re-applies the short log tail.
///
/// Returns [`OpenraftAdapterError::Config`] if the resulting configuration fails
/// openraft's own validation (e.g. inconsistent timeout ordering).
pub fn default_raft_config() -> Result<std::sync::Arc<openraft::Config>, OpenraftAdapterError> {
    openraft::Config {
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(SNAPSHOT_LOGS_SINCE_LAST),
        max_in_snapshot_log_to_keep: MAX_IN_SNAPSHOT_LOG_TO_KEEP,
        ..openraft::Config::default()
    }
    .validate()
    .map(std::sync::Arc::new)
    .map_err(|error| OpenraftAdapterError::Config(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_raft_config_validates() {
        let config = default_raft_config().expect("default config should validate");
        assert!(config.heartbeat_interval > 0);
    }

    #[test]
    fn metadata_raft_command_roundtrips_through_json() {
        let command = MetadataRaftCommand::ApplySnapshot(CoordinationSnapshot::default());
        let json = serde_json::to_string(&command).expect("command should serialize");
        let decoded: MetadataRaftCommand =
            serde_json::from_str(&json).expect("command should deserialize");
        assert_eq!(command, decoded);
    }
}
