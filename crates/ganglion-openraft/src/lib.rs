use std::fmt;
use std::sync::RwLock;

use ganglion_core::{
    CoordinationSnapshot, PartitionPlacementPolicy, PlacementError, PlacementInput as PlannerInput,
};

/// A narrow error surface for the initial adapter scaffold.
#[derive(Debug, Clone)]
pub enum OpenraftAdapterError {
    NotLeader,
    StaleGeneration,
    PoisonedState,
    StaleTerm,
    Planner(PlacementError),
}

impl fmt::Display for OpenraftAdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader => f.write_str("metadata write attempted by non-leader"),
            Self::StaleGeneration => {
                f.write_str("snapshot generation is older than current generation")
            }
            Self::PoisonedState => f.write_str("consensus state lock was poisoned"),
            Self::StaleTerm => f.write_str("proposal term is older than current leader term"),
            Self::Planner(error) => write!(f, "planner error: {:?}", error),
        }
    }
}

/// Trait contract for control-plane engines used by ganglion.
pub trait MetadataConsensus {
    fn local_node_id(&self) -> &str;
    fn leader_id(&self) -> Option<&str>;
    fn is_leader(&self) -> bool;

    fn snapshot(&self) -> CoordinationSnapshot;
    fn apply_snapshot(
        &self,
        proposer: &str,
        snapshot: CoordinationSnapshot,
        term: Option<u64>,
    ) -> Result<(), OpenraftAdapterError>;
}

/// One iteration of a control loop:
/// 1) compute a snapshot with a planner,
/// 2) append/commit it to consensus,
/// 3) publish the committed snapshot to the caller's observer.
///
/// This keeps the planner/consensus/watch integration explicit and testable.
pub fn plan_and_publish(
    consensus: &dyn MetadataConsensus,
    proposer: &str,
    planner: &dyn PartitionPlacementPolicy,
    input: PlannerInput,
    publish: impl FnOnce(CoordinationSnapshot),
) -> Result<CoordinationSnapshot, OpenraftAdapterError> {
    let plan = planner.plan(input).map_err(OpenraftAdapterError::Planner)?;
    consensus.apply_snapshot(proposer, plan.snapshot.clone(), None)?;
    publish(plan.snapshot.clone());
    Ok(plan.snapshot)
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct MetadataLogEntry {
    term: u64,
    index: u64,
    generation: u64,
}

#[derive(Debug)]
struct OpenraftLikeStore {
    current_term: u64,
    leader: Option<&'static str>,
    snapshot: CoordinationSnapshot,
    log: Vec<MetadataLogEntry>,
}

impl OpenraftLikeStore {
    fn new(initial_snapshot: CoordinationSnapshot) -> Self {
        Self {
            current_term: 1,
            leader: None,
            snapshot: initial_snapshot,
            log: Vec::new(),
        }
    }

    fn is_leader(&self, node_id: &str) -> bool {
        self.leader.is_some_and(|leader| leader == node_id)
    }

    fn append_entry(&mut self, term: u64, generation: u64) -> Result<u64, OpenraftAdapterError> {
        if term < self.current_term {
            return Err(OpenraftAdapterError::StaleTerm);
        }

        if term > self.current_term {
            self.current_term = term;
            self.log.clear();
        }

        let next_index = self.log.len() as u64 + 1;
        self.log.push(MetadataLogEntry {
            term,
            index: next_index,
            generation,
        });

        Ok(next_index)
    }

    fn replace_snapshot(&mut self, next: CoordinationSnapshot) -> Result<(), OpenraftAdapterError> {
        if next.generation < self.snapshot.generation {
            return Err(OpenraftAdapterError::StaleGeneration);
        }

        self.snapshot = next;
        Ok(())
    }

    fn last_index(&self) -> u64 {
        self.log.last().map_or(0, |entry| entry.index)
    }

    fn last_term(&self) -> u64 {
        self.log.last().map_or(0, |entry| entry.term)
    }
}

fn into_static_str(value: String) -> &'static str {
    Box::leak(value.into_boxed_str())
}

/// In-process adapter using raft-like semantics (term + leader + durable-style log append).
/// This keeps the current integration stable while the true openraft transport is added.
#[derive(Debug)]
pub struct InMemoryMetadataNode {
    local_node_id: &'static str,
    store: RwLock<OpenraftLikeStore>,
}

impl InMemoryMetadataNode {
    pub fn new(node_id: impl Into<String>, initial_snapshot: CoordinationSnapshot) -> Self {
        Self {
            local_node_id: into_static_str(node_id.into()),
            store: RwLock::new(OpenraftLikeStore::new(initial_snapshot)),
        }
    }

    fn with_store_write<F, T>(&self, op: F) -> Result<T, OpenraftAdapterError>
    where
        F: FnOnce(&mut OpenraftLikeStore) -> Result<T, OpenraftAdapterError>,
    {
        let mut store = self
            .store
            .write()
            .map_err(|_| OpenraftAdapterError::PoisonedState)?;
        op(&mut store)
    }

    fn with_store_read<F, T>(&self, op: F) -> Result<T, OpenraftAdapterError>
    where
        F: FnOnce(&OpenraftLikeStore) -> T,
    {
        let store = self
            .store
            .read()
            .map_err(|_| OpenraftAdapterError::PoisonedState)?;
        Ok(op(&store))
    }

    pub fn set_leader_term(&self, node_id: impl Into<String>, term: u64) {
        let node_id = into_static_str(node_id.into());
        let _ = self.with_store_write(|store| {
            if term >= store.current_term {
                store.current_term = term;
            }

            store.leader = Some(node_id);
            Ok(())
        });
    }

    pub fn set_leader(&self, leader_id: impl Into<String>) {
        self.set_leader_term(leader_id, self.current_term());
    }

    pub fn clear_leader(&self) {
        let _ = self.with_store_write(|store| {
            store.leader = None;
            Ok(())
        });
    }

    /// Convenience wrapper for a local plan + apply in one operation.
    pub fn plan_and_apply(
        &self,
        proposer: &str,
        planner: &dyn PartitionPlacementPolicy,
        input: PlannerInput,
    ) -> Result<CoordinationSnapshot, OpenraftAdapterError> {
        let plan = planner.plan(input).map_err(OpenraftAdapterError::Planner)?;

        self.apply_snapshot(proposer, plan.snapshot.clone(), None)?;

        Ok(plan.snapshot)
    }

    pub fn log_len(&self) -> usize {
        self.with_store_read(|store| store.log.len())
            .unwrap_or_default()
    }

    pub fn current_term(&self) -> u64 {
        self.with_store_read(|store| store.current_term)
            .unwrap_or_default()
    }

    pub fn last_index(&self) -> u64 {
        self.with_store_read(OpenraftLikeStore::last_index)
            .unwrap_or_default()
    }

    pub fn last_term(&self) -> u64 {
        self.with_store_read(OpenraftLikeStore::last_term)
            .unwrap_or_default()
    }
}

impl MetadataConsensus for InMemoryMetadataNode {
    fn local_node_id(&self) -> &str {
        self.local_node_id
    }

    fn leader_id(&self) -> Option<&str> {
        self.store
            .read()
            .ok()
            .and_then(|store| store.leader)
            .map(|leader| leader as &str)
    }

    fn is_leader(&self) -> bool {
        self.store
            .read()
            .ok()
            .is_some_and(|store| store.is_leader(self.local_node_id))
    }

    fn snapshot(&self) -> CoordinationSnapshot {
        match self.store.read() {
            Ok(snapshot) => snapshot.snapshot.clone(),
            Err(_) => CoordinationSnapshot::default(),
        }
    }

    fn apply_snapshot(
        &self,
        proposer: &str,
        snapshot: CoordinationSnapshot,
        term: Option<u64>,
    ) -> Result<(), OpenraftAdapterError> {
        let term = term.unwrap_or_else(|| self.current_term());
        self.with_store_write(|store| {
            if !store.is_leader(proposer) {
                return Err(OpenraftAdapterError::NotLeader);
            }

            store.append_entry(term, snapshot.generation)?;
            store.replace_snapshot(snapshot)
        })
    }
}

/// A compatibility constructor that stores local node identity explicitly.
pub struct InMemoryMetadataNodeBuilder {
    node_id: String,
}

impl InMemoryMetadataNodeBuilder {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
        }
    }

    pub fn initial_snapshot(self, initial: CoordinationSnapshot) -> InMemoryMetadataNode {
        InMemoryMetadataNode::new(self.node_id, initial)
    }
}

impl Default for InMemoryMetadataNode {
    fn default() -> Self {
        Self::new("node-unknown", CoordinationSnapshot::default())
    }
}

/// Simple planner export for convenience.
pub use ganglion_core::DeterministicPartitionPlacement;

#[cfg(test)]
mod tests {
    use super::*;
    use ganglion_coordination::{CoordinationProvider, InMemoryCoordination};
    use ganglion_core::ResourceIdentity;
    use std::collections::BTreeMap;

    #[test]
    fn in_memory_metadata_node_rejects_non_leader_updates() {
        let core = InMemoryMetadataNode::new("node-a", CoordinationSnapshot::default());
        core.set_leader("node-b".to_string());

        let result = core.apply_snapshot(
            "node-a",
            CoordinationSnapshot {
                generation: 1,
                ..Default::default()
            },
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn in_memory_metadata_node_plans_and_applies() {
        let initial = CoordinationSnapshot::default();
        let node = InMemoryMetadataNode::new("node-a", initial);
        node.set_leader("node-a");

        let mut nodes = BTreeMap::new();
        nodes.insert(
            "node-a".to_string(),
            ganglion_core::NodeInfo::new("node-a", "127.0.0.1:1", None::<String>),
        );
        nodes.insert(
            "node-b".to_string(),
            ganglion_core::NodeInfo::new("node-b", "127.0.0.1:2", None::<String>),
        );

        let input = PlannerInput {
            nodes,
            resources: vec![ResourceIdentity::new("svc", "orders", 0, None::<String>)],
            existing: BTreeMap::new(),
            target_followers: 1,
            generation: 1,
        };

        let result = node
            .plan_and_apply("node-a", &DeterministicPartitionPlacement, input)
            .expect("plan should apply");
        assert_eq!(result.generation, 1);
        assert_eq!(result.assignments.len(), 1);
        assert_eq!(node.log_len(), 1);
    }

    #[test]
    fn in_memory_metadata_node_rejects_stale_generation() {
        let mut snapshot = CoordinationSnapshot::default();
        snapshot.generation = 5;
        let node = InMemoryMetadataNode::new("node-a", snapshot);
        node.set_leader("node-a");

        let stale = CoordinationSnapshot {
            generation: 4,
            ..Default::default()
        };
        let err = node
            .apply_snapshot("node-a", stale, None)
            .expect_err("stale update is rejected");
        assert!(matches!(err, OpenraftAdapterError::StaleGeneration));
    }

    #[test]
    fn in_memory_metadata_node_updates_generation() {
        let node = InMemoryMetadataNode::new(
            "node-a",
            CoordinationSnapshot {
                generation: 2,
                ..Default::default()
            },
        );
        node.set_leader("node-a");

        let next = CoordinationSnapshot {
            generation: 3,
            ..Default::default()
        };

        node.apply_snapshot("node-a", next.clone(), None)
            .expect("generation 3 should apply");
        assert_eq!(node.snapshot().generation, 3);
        assert_eq!(node.log_len(), 1);
    }

    #[test]
    fn in_memory_metadata_node_rejects_stale_term() {
        let node = InMemoryMetadataNode::new("node-a", CoordinationSnapshot::default());
        node.set_leader_term("node-a", 5);

        let snapshot = CoordinationSnapshot {
            generation: 1,
            ..Default::default()
        };
        let err = node
            .apply_snapshot("node-a", snapshot, Some(4))
            .expect_err("stale term is rejected");
        assert!(matches!(err, OpenraftAdapterError::StaleTerm));
    }

    #[test]
    fn in_memory_metadata_node_advances_term_and_resets_log() {
        let node = InMemoryMetadataNode::new("node-a", CoordinationSnapshot::default());
        node.set_leader_term("node-a", 1);
        node.apply_snapshot(
            "node-a",
            CoordinationSnapshot {
                generation: 1,
                ..Default::default()
            },
            Some(1),
        )
        .expect("first entry applied");

        assert_eq!(node.current_term(), 1);
        assert_eq!(node.log_len(), 1);
        assert_eq!(node.last_term(), 1);

        node.apply_snapshot(
            "node-a",
            CoordinationSnapshot {
                generation: 2,
                ..Default::default()
            },
            Some(3),
        )
        .expect("higher term entry applied");

        assert_eq!(node.current_term(), 3);
        assert_eq!(node.log_len(), 1);
        assert_eq!(node.last_term(), 3);
    }

    #[test]
    fn in_memory_metadata_node_exposes_ids() {
        let node = InMemoryMetadataNode::new("node-local", CoordinationSnapshot::default());
        assert_eq!(node.local_node_id(), "node-local");

        node.set_leader("node-remote");
        assert_eq!(node.leader_id(), Some("node-remote"));
        assert!(!node.is_leader());
    }

    #[test]
    fn control_loop_publishes_planned_snapshot_to_watchers() {
        let node = InMemoryMetadataNode::new("node-a", CoordinationSnapshot::default());
        node.set_leader("node-a");

        let mut nodes = BTreeMap::new();
        nodes.insert(
            "node-a".to_string(),
            ganglion_core::NodeInfo::new("node-a", "127.0.0.1:1", None::<String>),
        );
        nodes.insert(
            "node-b".to_string(),
            ganglion_core::NodeInfo::new("node-b", "127.0.0.1:2", None::<String>),
        );

        let input = PlannerInput {
            nodes,
            resources: vec![ResourceIdentity::new("svc", "orders", 0, None::<String>)],
            existing: BTreeMap::new(),
            target_followers: 1,
            generation: 1,
        };

        let coordination = InMemoryCoordination::new(CoordinationSnapshot::default());
        let watch = coordination.watch();

        let published = plan_and_publish(
            &node,
            "node-a",
            &DeterministicPartitionPlacement,
            input,
            |snapshot| coordination.update_snapshot(snapshot),
        )
        .expect("control loop should publish");

        assert_eq!(published.generation, 1);
        assert_eq!(coordination.snapshot(), published);
        assert_eq!(watch.borrow().clone(), published);
    }

    #[test]
    fn control_loop_does_not_publish_on_consensus_reject() {
        let node = InMemoryMetadataNode::new("node-a", CoordinationSnapshot::default());
        node.set_leader("node-a");
        let mut nodes = BTreeMap::new();
        nodes.insert(
            "node-a".to_string(),
            ganglion_core::NodeInfo::new("node-a", "127.0.0.1:1", None::<String>),
        );

        let input = PlannerInput {
            nodes,
            resources: vec![ResourceIdentity::new("svc", "orders", 0, None::<String>)],
            existing: BTreeMap::new(),
            target_followers: 1,
            generation: 1,
        };

        let mut published = false;
        let result = plan_and_publish(
            &node,
            "node-b",
            &DeterministicPartitionPlacement,
            input,
            |_| {
                published = true;
            },
        );

        assert!(matches!(result, Err(OpenraftAdapterError::NotLeader)));
        assert!(!published);
    }
}
