use std::collections::BTreeSet;
use std::sync::RwLock;

use ganglion_core::{CoordinationSnapshot, ResourceIdentity};
use tokio::sync::watch;

/// Snapshot stream used by brokers or control loops that need reactive metadata updates.
pub type SnapshotWatch = watch::Receiver<CoordinationSnapshot>;

/// Runtime-facing view of ownership/follower metadata.
pub trait CoordinationProvider: Send + Sync {
    /// Returns the currently known snapshot.
    fn snapshot(&self) -> CoordinationSnapshot;

    /// Returns whether the local node currently owns the resource.
    fn owns_resource(&self, node_id: &str, resource: &ResourceIdentity) -> bool;

    /// Returns whether the local node currently follows the resource.
    fn follows_resource(&self, node_id: &str, resource: &ResourceIdentity) -> bool;

    /// Returns a watcher that is updated whenever the snapshot changes.
    fn watch(&self) -> SnapshotWatch;
}

/// Minimal in-process implementation with explicit update API.
#[derive(Debug)]
pub struct InMemoryCoordination {
    snapshot: RwLock<CoordinationSnapshot>,
    sender: watch::Sender<CoordinationSnapshot>,
}

impl InMemoryCoordination {
    pub fn new(initial: CoordinationSnapshot) -> Self {
        let (sender, _receiver) = watch::channel(initial.clone());
        Self {
            snapshot: RwLock::new(initial),
            sender,
        }
    }

    pub fn update_snapshot(&self, next: CoordinationSnapshot) {
        let mut current = self
            .snapshot
            .write()
            .unwrap_or_else(|error| error.into_inner());
        *current = next.clone();
        let _ = self.sender.send(next);
    }

    pub fn set_generation(&self, generation: u64) {
        let mut current = self
            .snapshot
            .write()
            .unwrap_or_else(|error| error.into_inner());
        current.generation = generation;
        let next = current.clone();
        let _ = self.sender.send(next);
    }
}

impl CoordinationProvider for InMemoryCoordination {
    fn snapshot(&self) -> CoordinationSnapshot {
        self.snapshot
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    fn owns_resource(&self, node_id: &str, resource: &ResourceIdentity) -> bool {
        self.snapshot
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .assignment_for(resource)
            .is_some_and(|assignment| assignment.owner == node_id)
    }

    fn follows_resource(&self, node_id: &str, resource: &ResourceIdentity) -> bool {
        self.snapshot
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .assignment_for(resource)
            .is_some_and(|assignment| {
                assignment
                    .followers
                    .iter()
                    .any(|follower| follower == node_id)
            })
    }

    fn watch(&self) -> SnapshotWatch {
        self.sender.subscribe()
    }
}

/// Static immutable provider for tests and bootstrap scenarios.
#[derive(Debug, Clone)]
pub struct StaticCoordination {
    snapshot: CoordinationSnapshot,
}

impl StaticCoordination {
    pub fn new(_local_node: impl Into<String>, snapshot: CoordinationSnapshot) -> Self {
        Self { snapshot }
    }
}

impl CoordinationProvider for StaticCoordination {
    fn snapshot(&self) -> CoordinationSnapshot {
        self.snapshot.clone()
    }

    fn owns_resource(&self, node_id: &str, resource: &ResourceIdentity) -> bool {
        self.snapshot
            .assignment_for(resource)
            .is_some_and(|assignment| assignment.is_owned_by(node_id))
    }

    fn follows_resource(&self, node_id: &str, resource: &ResourceIdentity) -> bool {
        self.snapshot
            .assignment_for(resource)
            .is_some_and(|assignment| assignment.is_followed_by(node_id))
    }

    fn watch(&self) -> SnapshotWatch {
        let (_sender, receiver) = watch::channel(self.snapshot.clone());
        receiver
    }
}

/// Convenience set utilities for controller-like code.
pub fn owned_resources(
    provider: &dyn CoordinationProvider,
    node_id: &str,
) -> Vec<ResourceIdentity> {
    provider
        .snapshot()
        .assignments
        .into_iter()
        .filter_map(|(resource, assignment)| {
            let local_owned = assignment.owner == node_id;
            if local_owned {
                Some(resource)
            } else {
                None
            }
        })
        .collect()
}

/// Convenience set utilities for controller-like code.
pub fn followed_resources(
    provider: &dyn CoordinationProvider,
    node_id: &str,
) -> Vec<ResourceIdentity> {
    provider
        .snapshot()
        .assignments
        .into_iter()
        .filter_map(|(resource, assignment)| {
            if assignment
                .followers
                .iter()
                .any(|follower| follower == node_id)
            {
                Some(resource)
            } else {
                None
            }
        })
        .collect()
}

/// Extract resource identities from a snapshot using a set semantics.
pub fn owned_by_snapshot(
    snapshot: &CoordinationSnapshot,
    node_id: &str,
) -> BTreeSet<ResourceIdentity> {
    snapshot
        .assignments
        .iter()
        .filter_map(|(resource, assignment)| {
            if assignment.owner == node_id {
                Some(resource.clone())
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ganglion_core::NodeInfo;
    use std::collections::BTreeMap;

    fn sample_snapshot() -> CoordinationSnapshot {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            "node-a".to_string(),
            NodeInfo::new("node-a", "127.0.0.1:1", None::<String>),
        );
        nodes.insert(
            "node-b".to_string(),
            NodeInfo::new("node-b", "127.0.0.1:2", None::<String>),
        );

        let mut assignments = BTreeMap::new();
        assignments.insert(
            ResourceIdentity::new("ns", "topic-a", 0, None::<String>),
            ganglion_core::PartitionAssignment::new(
                ResourceIdentity::new("ns", "topic-a", 0, None::<String>),
                "node-a",
                vec!["node-b".to_string()],
                1,
            ),
        );

        CoordinationSnapshot {
            nodes,
            assignments,
            generation: 7,
            ..CoordinationSnapshot::default()
        }
    }

    #[test]
    fn static_coordination_reports_expected_roles() {
        let snapshot = sample_snapshot();
        let coordination = StaticCoordination::new("node-a", snapshot);

        let resource = ResourceIdentity::new("ns", "topic-a", 0, None::<String>);
        assert!(coordination.owns_resource("node-a", &resource));
        assert!(coordination.follows_resource("node-b", &resource));
    }

    #[test]
    fn in_memory_coordination_can_publish_updates() {
        let coordination = InMemoryCoordination::new(sample_snapshot());

        let mut updated = sample_snapshot();
        updated.generation = 8;
        coordination.update_snapshot(updated.clone());

        let snapshot = coordination.snapshot();
        assert_eq!(snapshot.generation, 8);
    }

    #[test]
    fn helpers_filter_owned_followed_resources() {
        let snapshot = sample_snapshot();
        let coordination = StaticCoordination::new("node-a", snapshot.clone());
        let owned = owned_resources(&coordination, "node-a");
        let followed = followed_resources(&coordination, "node-b");

        assert_eq!(owned.len(), 1);
        assert_eq!(followed.len(), 1);
        assert_eq!(
            owned[0],
            ResourceIdentity::new("ns", "topic-a", 0, None::<String>)
        );
        assert_eq!(
            followed[0],
            ResourceIdentity::new("ns", "topic-a", 0, None::<String>)
        );
    }
}
