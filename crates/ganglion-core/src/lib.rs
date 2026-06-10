use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Stable identifier for a sharded logical resource (queue, partition, etc.).
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub struct ResourceIdentity {
    pub namespace: String,
    pub name: String,
    pub partition: u64,
    pub group: Option<String>,
}

impl ResourceIdentity {
    pub fn new(
        namespace: impl Into<String>,
        name: impl Into<String>,
        partition: u64,
        group: impl Into<Option<String>>,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
            partition,
            group: group.into(),
        }
    }

    pub fn with_namespace_part(self, namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            ..self
        }
    }
}

/// Runtime-visible metadata for one node in the control plane.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub endpoint: String,
    pub admin_endpoint: Option<String>,
    pub labels: BTreeMap<String, String>,
}

impl NodeInfo {
    pub fn new(
        node_id: impl Into<String>,
        endpoint: impl Into<String>,
        admin_endpoint: impl Into<Option<String>>,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            endpoint: endpoint.into(),
            admin_endpoint: admin_endpoint.into(),
            labels: BTreeMap::new(),
        }
    }
}

impl Default for NodeInfo {
    fn default() -> Self {
        Self::new("node-unknown", "127.0.0.1:0", None::<String>)
    }
}

/// Durability options for metadata writes.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReplicationDurabilityPolicy {
    /// Require only local durable write.
    LocalDurable,
    /// Require this many assigned replicas to accept a write (including owner).
    ReplicaAccepted { nodes: usize },
    /// Require this many assigned replicas to durably persist a write (including owner).
    ReplicaDurable { nodes: usize },
    /// Require a majority of assigned replicas to durably persist a write.
    MajorityDurable,
}

impl Default for ReplicationDurabilityPolicy {
    fn default() -> Self {
        Self::LocalDurable
    }
}

impl ReplicationDurabilityPolicy {
    pub fn resolve(
        self,
        assigned_nodes: usize,
    ) -> Result<ReplicationDurabilityRequirement, ReplicationDurabilityError> {
        let requirement = match self {
            Self::LocalDurable => ReplicationDurabilityRequirement {
                nodes: 1,
                acknowledgement: ReplicationAcknowledgement::Durable,
            },
            Self::ReplicaAccepted { nodes } => ReplicationDurabilityRequirement {
                nodes,
                acknowledgement: ReplicationAcknowledgement::Accepted,
            },
            Self::ReplicaDurable { nodes } => ReplicationDurabilityRequirement {
                nodes,
                acknowledgement: ReplicationAcknowledgement::Durable,
            },
            Self::MajorityDurable => ReplicationDurabilityRequirement {
                nodes: (assigned_nodes / 2) + 1,
                acknowledgement: ReplicationAcknowledgement::Durable,
            },
        };

        requirement.validate(assigned_nodes)?;
        Ok(requirement)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplicationDurabilityRequirement {
    pub nodes: usize,
    pub acknowledgement: ReplicationAcknowledgement,
}

impl ReplicationDurabilityRequirement {
    fn validate(self, assigned_nodes: usize) -> Result<(), ReplicationDurabilityError> {
        if self.nodes == 0 {
            return Err(ReplicationDurabilityError::ZeroNodes);
        }

        if self.nodes > assigned_nodes {
            return Err(ReplicationDurabilityError::NotEnoughAssignedNodes {
                required: self.nodes,
                available: assigned_nodes,
            });
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReplicationAcknowledgement {
    Accepted,
    Durable,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum ReplicationDurabilityError {
    ZeroNodes,
    NotEnoughAssignedNodes { required: usize, available: usize },
}

/// Owner/follower assignment for a single resource.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PartitionAssignment {
    pub resource: ResourceIdentity,
    pub owner: String,
    pub followers: Vec<String>,
    pub epoch: u64,
    pub durability: ReplicationDurabilityPolicy,
}

impl PartitionAssignment {
    pub fn new(
        resource: ResourceIdentity,
        owner: impl Into<String>,
        followers: Vec<String>,
        epoch: u64,
    ) -> Self {
        Self {
            resource,
            owner: owner.into(),
            followers,
            epoch,
            durability: ReplicationDurabilityPolicy::LocalDurable,
        }
    }

    pub fn is_owned_by(&self, node_id: &str) -> bool {
        self.owner == node_id
    }

    pub fn is_followed_by(&self, node_id: &str) -> bool {
        self.followers.iter().any(|follower| follower == node_id)
    }

    pub fn replica_set_size(&self) -> usize {
        1 + self.followers.len()
    }

    pub fn durability_requirement(
        &self,
    ) -> Result<ReplicationDurabilityRequirement, ReplicationDurabilityError> {
        self.durability.resolve(self.replica_set_size())
    }
}

/// Full control-plane snapshot, including all known node and assignment state.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CoordinationSnapshot {
    pub nodes: BTreeMap<String, NodeInfo>,
    pub assignments: BTreeMap<ResourceIdentity, PartitionAssignment>,
    pub generation: u64,
}

impl Default for CoordinationSnapshot {
    fn default() -> Self {
        Self {
            nodes: BTreeMap::new(),
            assignments: BTreeMap::new(),
            generation: 0,
        }
    }
}

impl CoordinationSnapshot {
    pub fn assignment_for(&self, resource: &ResourceIdentity) -> Option<&PartitionAssignment> {
        self.assignments.get(resource)
    }

    pub fn owned_by(&self, node_id: &str) -> Vec<&PartitionAssignment> {
        self.assignments
            .values()
            .filter(|assignment| assignment.is_owned_by(node_id))
            .collect()
    }

    pub fn followed_by(&self, node_id: &str) -> Vec<&PartitionAssignment> {
        self.assignments
            .values()
            .filter(|assignment| assignment.is_followed_by(node_id))
            .collect()
    }
}

/// Inputs for pure planning logic.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PlacementInput {
    pub nodes: BTreeMap<String, NodeInfo>,
    pub resources: Vec<ResourceIdentity>,
    pub existing: BTreeMap<ResourceIdentity, PartitionAssignment>,
    pub target_followers: usize,
    pub generation: u64,
}

/// Output of planner policy execution.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PlacementPlan {
    pub snapshot: CoordinationSnapshot,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PlacementError {
    NoNodesForResources,
}

pub trait PartitionPlacementPolicy: std::fmt::Debug + Send + Sync {
    fn plan(&self, input: PlacementInput) -> Result<PlacementPlan, PlacementError>;
}

/// Deterministic baseline policy:
/// - Keep assignment owner if it is still a live node.
/// - Keep followers where possible, then fill missing followers from live nodes.
#[derive(Debug, Clone, Copy)]
pub struct DeterministicPartitionPlacement;

impl PartitionPlacementPolicy for DeterministicPartitionPlacement {
    fn plan(&self, input: PlacementInput) -> Result<PlacementPlan, PlacementError> {
        let mut resources = input.resources;
        resources.sort();
        resources.dedup();

        let nodes: Vec<String> = input.nodes.keys().cloned().collect();
        if resources.is_empty() {
            return Ok(PlacementPlan {
                snapshot: CoordinationSnapshot {
                    nodes: input.nodes,
                    assignments: BTreeMap::new(),
                    generation: input.generation,
                },
            });
        }

        if nodes.is_empty() {
            return Err(PlacementError::NoNodesForResources);
        }

        let mut assignments = BTreeMap::new();
        for (idx, resource) in resources.iter().enumerate() {
            let existing = input.existing.get(resource);

            let owner = pick_owner(idx, &nodes, existing);
            let follower_set =
                gather_followers(owner.as_str(), input.target_followers, &nodes, existing);
            let epoch = match existing {
                Some(existing) if existing.owner == owner => existing.epoch,
                Some(existing) => existing.epoch.saturating_add(1),
                None => 1,
            };
            let durability = existing
                .map(|existing| existing.durability)
                .unwrap_or_default();

            assignments.insert(
                resource.clone(),
                PartitionAssignment {
                    resource: resource.clone(),
                    owner,
                    followers: follower_set,
                    epoch,
                    durability,
                },
            );
        }

        Ok(PlacementPlan {
            snapshot: CoordinationSnapshot {
                nodes: input.nodes,
                assignments,
                generation: input.generation,
            },
        })
    }
}

fn pick_owner(idx: usize, node_ids: &[String], existing: Option<&PartitionAssignment>) -> String {
    if let Some(existing) = existing {
        if node_ids.iter().any(|node| node == &existing.owner) {
            return existing.owner.clone();
        }
    }

    node_ids[idx % node_ids.len()].clone()
}

fn gather_followers(
    owner: &str,
    target_followers: usize,
    node_ids: &[String],
    existing: Option<&PartitionAssignment>,
) -> Vec<String> {
    if node_ids.len() <= 1 || target_followers == 0 {
        return Vec::new();
    }

    let mut ordered_unique = BTreeSet::new();
    let mut followers = Vec::new();

    if let Some(existing) = existing {
        for node in &existing.followers {
            if node == owner {
                continue;
            }
            if !ordered_unique.contains(node) && node_ids.contains(node) {
                ordered_unique.insert(node.clone());
                followers.push(node.clone());
            }
        }
    }

    if followers.len() >= target_followers {
        followers.truncate(target_followers);
        return followers;
    }

    let owner_index = node_ids.iter().position(|node| node == owner).unwrap_or(0);
    let mut cursor = 1usize;
    while followers.len() < target_followers {
        let candidate = &node_ids[(owner_index + cursor) % node_ids.len()];
        if candidate == owner {
            cursor = cursor.saturating_add(1);
            if cursor > node_ids.len() * 2 {
                break;
            }
            continue;
        }

        if !ordered_unique.contains(candidate) {
            ordered_unique.insert(candidate.clone());
            followers.push(candidate.clone());
        }

        cursor = cursor.saturating_add(1);
        if cursor > node_ids.len() * 2 {
            break;
        }
    }

    followers.truncate(target_followers);
    followers
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum LocalRole {
    Owner,
    Follower,
    None,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum LocalTransition {
    KeepOwner {
        resource: ResourceIdentity,
    },
    KeepFollower {
        resource: ResourceIdentity,
    },
    PromoteFollowerToOwner {
        resource: ResourceIdentity,
        to_epoch: u64,
    },
    DemoteOwnerToFollower {
        resource: ResourceIdentity,
        from_epoch: u64,
        to_epoch: u64,
    },
    StopServing {
        resource: ResourceIdentity,
        was_owner: bool,
    },
}

pub fn local_assignment_role(
    snapshot: &CoordinationSnapshot,
    node_id: &str,
    resource: &ResourceIdentity,
) -> LocalRole {
    let Some(assignment) = snapshot.assignment_for(resource) else {
        return LocalRole::None;
    };

    if assignment.owner == node_id {
        return LocalRole::Owner;
    }

    if assignment
        .followers
        .iter()
        .any(|follower| follower == node_id)
    {
        return LocalRole::Follower;
    }

    LocalRole::None
}

pub fn plan_local_assignment_transitions(
    node_id: &str,
    previous: &CoordinationSnapshot,
    next: &CoordinationSnapshot,
) -> Vec<LocalTransition> {
    let mut resources: BTreeSet<ResourceIdentity> = previous.assignments.keys().cloned().collect();
    resources.extend(next.assignments.keys().cloned());

    let mut transitions = Vec::with_capacity(resources.len());

    for resource in resources {
        let previous_role = local_assignment_role(previous, node_id, &resource);
        let next_role = local_assignment_role(next, node_id, &resource);
        let transition = match (previous_role, next_role) {
            (LocalRole::Owner, LocalRole::Owner) => LocalTransition::KeepOwner { resource },
            (LocalRole::Follower, LocalRole::Follower) => {
                LocalTransition::KeepFollower { resource }
            }
            (LocalRole::None, LocalRole::Owner) => {
                let next_epoch = next.assignment_for(&resource).map_or(1, |a| a.epoch);
                LocalTransition::PromoteFollowerToOwner {
                    resource,
                    to_epoch: next_epoch,
                }
            }
            (LocalRole::Follower, LocalRole::Owner) => {
                let next_epoch = next.assignment_for(&resource).map_or(1, |a| a.epoch);
                LocalTransition::PromoteFollowerToOwner {
                    resource,
                    to_epoch: next_epoch,
                }
            }
            (LocalRole::Owner, LocalRole::Follower) => {
                let _previous_epoch = previous.assignment_for(&resource).map_or(0, |a| a.epoch);
                let next_epoch = next
                    .assignment_for(&resource)
                    .map_or(_previous_epoch, |a| a.epoch);
                LocalTransition::DemoteOwnerToFollower {
                    resource,
                    from_epoch: _previous_epoch,
                    to_epoch: next_epoch,
                }
            }
            (LocalRole::Owner, LocalRole::None) => LocalTransition::StopServing {
                resource,
                was_owner: true,
            },
            (LocalRole::Follower, LocalRole::None) => LocalTransition::StopServing {
                resource,
                was_owner: false,
            },
            (LocalRole::None, LocalRole::Follower) => LocalTransition::KeepFollower { resource },
            (LocalRole::None, LocalRole::None) => continue,
        };

        transitions.push(transition);
    }

    transitions
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::{BTreeMap, BTreeSet};

    fn sample_nodes() -> BTreeMap<String, NodeInfo> {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            "node-a".to_string(),
            NodeInfo::new("node-a", "10.0.0.1:1111", None::<String>),
        );
        nodes.insert(
            "node-b".to_string(),
            NodeInfo::new("node-b", "10.0.0.2:1111", None::<String>),
        );
        nodes.insert(
            "node-c".to_string(),
            NodeInfo::new("node-c", "10.0.0.3:1111", None::<String>),
        );
        nodes
    }

    fn dedupe_values<T: Eq>(values: Vec<T>) -> Vec<T> {
        let mut deduped = Vec::new();
        for value in values {
            if !deduped.contains(&value) {
                deduped.push(value);
            }
        }
        deduped
    }

    #[test]
    fn deterministic_planner_keeps_live_owner() {
        let resources = vec![
            ResourceIdentity::new("ns", "topic-a", 0, None::<String>),
            ResourceIdentity::new("ns", "topic-b", 1, Some("g".to_string())),
        ];
        let old_assignment = PartitionAssignment::new(
            ResourceIdentity::new("ns", "topic-a", 0, None::<String>),
            "node-b",
            vec!["node-c".to_string()],
            2,
        );

        let input = PlacementInput {
            nodes: sample_nodes(),
            resources: resources.clone(),
            existing: [(
                ResourceIdentity::new("ns", "topic-a", 0, None::<String>),
                old_assignment,
            )]
            .into_iter()
            .collect(),
            target_followers: 1,
            generation: 99,
        };

        let plan = DeterministicPartitionPlacement
            .plan(input)
            .expect("planner should succeed");
        let assign_a = plan
            .snapshot
            .assignments
            .get(&ResourceIdentity::new("ns", "topic-a", 0, None::<String>))
            .expect("assignment exists");

        assert_eq!(assign_a.owner, "node-b");
        assert_eq!(assign_a.epoch, 2);
        assert_eq!(assign_a.followers, vec!["node-c".to_string()]);
        assert_eq!(plan.snapshot.generation, 99);
    }

    #[test]
    fn deterministic_planner_reassigns_owner_when_missing_and_increments_epoch() {
        let resource = ResourceIdentity::new("ns", "topic-a", 0, None::<String>);
        let old_assignment = PartitionAssignment::new(resource.clone(), "node-z", vec![], 7);
        let input = PlacementInput {
            nodes: {
                let mut nodes = sample_nodes();
                nodes.remove("node-z");
                nodes
            },
            resources: vec![resource.clone()],
            existing: [(resource.clone(), old_assignment)].into_iter().collect(),
            target_followers: 2,
            generation: 3,
        };

        let plan = DeterministicPartitionPlacement
            .plan(input)
            .expect("planner should succeed");
        let assign = plan
            .snapshot
            .assignments
            .get(&resource)
            .expect("assignment exists");

        assert_ne!(assign.owner, "node-z");
        assert_eq!(assign.epoch, 8);
        assert_eq!(assign.followers.len(), 2);
    }

    #[test]
    fn deterministic_planner_errors_when_there_are_no_nodes() {
        let input = PlacementInput {
            nodes: BTreeMap::new(),
            resources: vec![ResourceIdentity::new("ns", "topic-a", 0, None::<String>)],
            existing: BTreeMap::new(),
            target_followers: 1,
            generation: 1,
        };

        assert!(DeterministicPartitionPlacement.plan(input).is_err());
    }

    #[test]
    fn transition_planner_reports_local_role_changes() {
        let resource = ResourceIdentity::new("ns", "topic-a", 0, None::<String>);
        let mut previous = CoordinationSnapshot::default();
        let mut next = CoordinationSnapshot::default();

        previous.assignments.insert(
            resource.clone(),
            PartitionAssignment::new(resource.clone(), "node-a", vec![], 1),
        );
        next.assignments.insert(
            resource.clone(),
            PartitionAssignment::new(resource.clone(), "node-b", vec!["node-a".to_string()], 2),
        );
        previous.generation = 7;
        next.generation = 8;

        let transitions = plan_local_assignment_transitions("node-a", &previous, &next);
        assert_eq!(transitions.len(), 1);
        match &transitions[0] {
            LocalTransition::DemoteOwnerToFollower {
                resource: r,
                from_epoch,
                to_epoch,
            } => {
                assert_eq!(r, &resource);
                assert_eq!(*from_epoch, 1);
                assert_eq!(*to_epoch, 2);
            }
            _ => panic!("expected owner demotion transition"),
        }
    }

    #[test]
    fn transition_planner_skips_resources_with_no_local_role_change() {
        let previous = CoordinationSnapshot::default();
        let next = CoordinationSnapshot::default();
        let transitions = plan_local_assignment_transitions("node-a", &previous, &next);

        assert!(transitions.is_empty());
    }

    proptest! {
        #[test]
        fn fuzz_planner_invariants_with_random_inputs(
            node_values in prop::collection::vec(1u16..4000, 1..8),
            resource_values in prop::collection::vec(1u16..5000, 0..12),
            target_followers in 0usize..10,
        ) {
            let mut nodes = BTreeMap::new();
            for value in node_values {
                let node_id = format!("node-{value}");
                nodes.insert(
                    node_id.clone(),
                    NodeInfo::new(
                        node_id.clone(),
                        format!("127.0.0.1:{}", value),
                        None::<String>,
                    ),
                );
            }

            let mut resources = Vec::new();
            for value in resource_values {
                let resource = ResourceIdentity::new(
                    format!("ns-{value}"),
                    format!("topic-{value}"),
                    u64::from(value) % 4,
                    None::<String>,
                );
                if !resources.contains(&resource) {
                    resources.push(resource);
                }
            }

            let input = PlacementInput {
                nodes: nodes.clone(),
                resources: resources.clone(),
                existing: BTreeMap::new(),
                target_followers,
                generation: 1,
            };

            let plan = DeterministicPartitionPlacement
                .plan(input.clone())
                .expect("planner should succeed with nodes");

            prop_assert_eq!(plan.snapshot.assignments.len(), resources.len());
            for (resource, assignment) in &plan.snapshot.assignments {
                prop_assert_eq!(&assignment.resource, resource);
                prop_assert!(plan.snapshot.nodes.contains_key(&assignment.owner));

                let follower_set_len = assignment.followers.iter().collect::<BTreeSet<&String>>().len();
                let max_followers = plan.snapshot.nodes.len().saturating_sub(1);
                prop_assert_eq!(follower_set_len, assignment.followers.len());
                prop_assert!(assignment.followers.len() <= max_followers);
                for follower in &assignment.followers {
                    prop_assert_ne!(follower, &assignment.owner);
                    prop_assert!(plan.snapshot.nodes.contains_key(follower));
                }
            }

            let replay = DeterministicPartitionPlacement
                .plan(input)
                .expect("planner should be deterministic");
            prop_assert_eq!(plan.snapshot, replay.snapshot);
        }

        #[test]
        fn fuzz_planner_with_existing_assignments(
            node_values in prop::collection::vec(1u16..4000, 1..8),
            resource_values in prop::collection::vec(1u16..5000, 0..12),
            owner_seed in prop::collection::vec(0u8..10, 12),
            follower_selector in prop::collection::vec(0u8..10, 24),
            follower_count_seed in prop::collection::vec(0u8..8, 12),
            epoch_seed in prop::collection::vec(1u8..15, 12),
            target_followers in 0usize..12,
        ) {
            let node_ids = dedupe_values(
                node_values
                    .into_iter()
                    .map(|value| format!("node-{value}"))
                    .collect(),
            );

            let nodes = node_ids
                .iter()
                .map(|node_id| {
                    (
                        node_id.clone(),
                        NodeInfo::new(
                            node_id.clone(),
                            format!("127.0.0.1:{}", node_id.trim_start_matches("node-")),
                            None::<String>,
                        ),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let sorted_node_ids = nodes.keys().cloned().collect::<Vec<_>>();
            let mut resources = dedupe_values(
                resource_values
                    .into_iter()
                    .map(|value| {
                        ResourceIdentity::new(
                            format!("ns-{value}"),
                            format!("topic-{value}"),
                            u64::from(value) % 7,
                            None::<String>,
                        )
                    })
                    .collect(),
            );

            resources.sort();

            let mut existing = BTreeMap::new();
            for (idx, resource) in resources.iter().enumerate() {
                if owner_seed[idx % owner_seed.len()] % 2 == 0 {
                    let owner_bucket = owner_seed[idx % owner_seed.len()] as usize % (node_ids.len() + 2);
                    let owner = if owner_bucket < node_ids.len() {
                        node_ids[owner_bucket].clone()
                    } else {
                        format!("ghost-owner-{idx}")
                    };

                    let max_followers = node_ids.len() + 2;
                    let requested_followers = follower_count_seed[idx % follower_count_seed.len()] as usize;
                    let follower_count = requested_followers % (max_followers + 2);

                    let mut followers = Vec::with_capacity(follower_count);
                    for follower_idx in 0..follower_count {
                        let selector =
                            follower_selector[(idx + follower_idx) % follower_selector.len()] as usize;
                        let bucket = (selector + follower_idx) % (max_followers + 2);
                        if bucket < node_ids.len() {
                            followers.push(node_ids[bucket].clone());
                        } else {
                            followers.push(format!("ghost-follower-{idx}-{follower_idx}"));
                        }
                    }

                    existing.insert(
                        resource.clone(),
                        PartitionAssignment {
                            resource: resource.clone(),
                            owner,
                            followers,
                            epoch: (epoch_seed[idx % epoch_seed.len()] as u64) % 20,
                            durability: ReplicationDurabilityPolicy::LocalDurable,
                        },
                    );
                }
            }

            let input = PlacementInput {
                nodes: nodes.clone(),
                resources: resources.clone(),
                existing: existing.clone(),
                target_followers,
                generation: 1,
            };

            let plan = DeterministicPartitionPlacement
                .plan(input.clone())
                .expect("planner should succeed with nodes");

            prop_assert_eq!(plan.snapshot.assignments.len(), resources.len());
            for (idx, resource) in resources.iter().enumerate() {
                let assignment = plan
                    .snapshot
                    .assignments
                    .get(resource)
                    .expect("assignment exists");
                let existing_assignment = existing.get(resource);

                let expected_owner = match existing_assignment {
                    Some(existing) if input.nodes.contains_key(&existing.owner) => {
                        existing.owner.clone()
                    }
                    _ => {
                        let fallback_index = idx % sorted_node_ids.len();
                        sorted_node_ids[fallback_index].clone()
                    }
                };
                prop_assert_eq!(&assignment.owner, &expected_owner);

                let expected_epoch = match existing_assignment {
                    Some(existing) if existing.owner == expected_owner => existing.epoch,
                    Some(existing) => existing.epoch.saturating_add(1),
                    None => 1,
                };
                prop_assert_eq!(assignment.epoch, expected_epoch);

                let follower_set_len =
                    assignment.followers.iter().collect::<BTreeSet<&String>>().len();
                let max_followers = node_ids.len().saturating_sub(1);
                prop_assert_eq!(follower_set_len, assignment.followers.len());
                prop_assert!(assignment.followers.len() <= max_followers);
                prop_assert!(assignment.followers.len() <= target_followers);

                let mut expected_from_existing = Vec::new();
                let mut seen = BTreeSet::new();
                if let Some(existing_assignment) = existing_assignment {
                    for follower in &existing_assignment.followers {
                        if follower == &expected_owner {
                            continue;
                        }
                        if seen.contains(follower) {
                            continue;
                        }
                        if !input.nodes.contains_key(follower) {
                            continue;
                        }

                        seen.insert(follower.clone());
                        expected_from_existing.push(follower.clone());
                    }
                }

                let prefix = std::cmp::min(
                    assignment.followers.len(),
                    std::cmp::min(expected_from_existing.len(), target_followers),
                );
                for prefix_idx in 0..prefix {
                    prop_assert_eq!(
                        &assignment.followers[prefix_idx],
                        &expected_from_existing[prefix_idx]
                    );
                }

                for follower in &assignment.followers {
                    prop_assert_ne!(follower, &assignment.owner);
                    prop_assert!(input.nodes.contains_key(follower));
                }
            }

            let replay = DeterministicPartitionPlacement
                .plan(input)
                .expect("planner should be deterministic");
            prop_assert_eq!(plan.snapshot, replay.snapshot);
        }

        #[test]
        fn fuzz_plan_local_assignment_transitions_follow_role_semantics(
            node_values in prop::collection::vec(1u16..4000, 1..8),
            resource_values in prop::collection::vec(1u16..5000, 0..12),
        ) {
            let node_ids = dedupe_values(
                node_values
                    .into_iter()
                    .map(|value| format!("node-{value}"))
                    .collect(),
            );
            prop_assume!(!node_ids.is_empty());

            let resources = dedupe_values(
                resource_values
                    .into_iter()
                    .map(|value| {
                        ResourceIdentity::new(
                            format!("ns-{value}"),
                            format!("topic-{value}"),
                            u64::from(value) % 7,
                            None::<String>,
                        )
                    })
                    .collect(),
            );
            prop_assume!(!resources.is_empty());

            let mut previous = CoordinationSnapshot {
                nodes: node_ids.iter().map(|node_id| {
                    (
                        node_id.clone(),
                        NodeInfo::new(
                            node_id.clone(),
                            format!("127.0.0.1:{}", node_id.trim_start_matches("node-")),
                            None::<String>,
                        ),
                    )
                }).collect(),
                ..CoordinationSnapshot::default()
            };
            let mut next = previous.clone();

            let node_count = node_ids.len();
            for (idx, resource) in resources.iter().enumerate() {
                let prev_selector = idx % 3;
                match prev_selector {
                    1 => {
                        let owner = node_ids[idx % node_count].clone();
                        previous.assignments.insert(
                            resource.clone(),
                            PartitionAssignment::new(
                                resource.clone(),
                                owner,
                                vec![],
                                1 + (idx as u64 % 4),
                            ),
                        );
                    }
                    2 => {
                        let follower = node_ids[(idx + 1) % node_count].clone();
                        previous.assignments.insert(
                            resource.clone(),
                            PartitionAssignment::new(
                                resource.clone(),
                                "other-node".to_string(),
                                vec![follower],
                                1 + (idx as u64 % 4),
                            ),
                        );
                    }
                    _ => {}
                }

                let next_selector = (idx + 2) % 3;
                match next_selector {
                    1 => {
                        let owner = node_ids[(idx + 2) % node_count].clone();
                        next.assignments.insert(
                            resource.clone(),
                            PartitionAssignment::new(
                                resource.clone(),
                                owner,
                                vec![],
                                10 + idx as u64,
                            ),
                        );
                    }
                    2 => {
                        let follower = node_ids[(idx + 3) % node_count].clone();
                        next.assignments.insert(
                            resource.clone(),
                            PartitionAssignment::new(
                                resource.clone(),
                                "other-node".to_string(),
                                vec![follower],
                                10 + idx as u64,
                            ),
                        );
                    }
                    _ => {}
                }
            }

            for node_id in &node_ids {
                let transitions = plan_local_assignment_transitions(node_id, &previous, &next);
                let resource_count = resources.len();
                prop_assert!(transitions.len() <= resource_count);

                let mut seen = BTreeSet::new();
                for transition in transitions.iter() {
                    let transition_resource = match transition {
                        LocalTransition::KeepOwner { resource } => resource,
                        LocalTransition::KeepFollower { resource } => resource,
                        LocalTransition::PromoteFollowerToOwner { resource, .. } => resource,
                        LocalTransition::DemoteOwnerToFollower { resource, .. } => resource,
                        LocalTransition::StopServing { resource, .. } => resource,
                    };

                    match transition {
                        LocalTransition::KeepOwner { resource } => {
                            prop_assert_eq!(local_assignment_role(&previous, node_id, resource), LocalRole::Owner);
                            prop_assert_eq!(local_assignment_role(&next, node_id, resource), LocalRole::Owner);
                        }
                        LocalTransition::KeepFollower { resource } => {
                            let prev = local_assignment_role(&previous, node_id, resource);
                            let next_role = local_assignment_role(&next, node_id, resource);
                            prop_assert!(
                                matches!(
                                    (prev, next_role),
                                    (LocalRole::Follower, LocalRole::Follower)
                                        | (LocalRole::None, LocalRole::Follower)
                                )
                            );
                        }
                        LocalTransition::PromoteFollowerToOwner { resource, to_epoch } => {
                            prop_assert_eq!(local_assignment_role(&next, node_id, resource), LocalRole::Owner);
                            let expected_epoch = next.assignment_for(&resource).map_or(1, |assignment| assignment.epoch);
                            prop_assert_eq!(*to_epoch, expected_epoch);
                        }
                        LocalTransition::DemoteOwnerToFollower { resource, from_epoch, to_epoch } => {
                            prop_assert_eq!(local_assignment_role(&previous, node_id, resource), LocalRole::Owner);
                            prop_assert_eq!(local_assignment_role(&next, node_id, resource), LocalRole::Follower);
                            let from = previous.assignment_for(&resource).map_or(0, |assignment| assignment.epoch);
                            let to = next.assignment_for(&resource).map_or(from, |assignment| assignment.epoch);
                            prop_assert_eq!(*from_epoch, from);
                            prop_assert_eq!(*to_epoch, to);
                        }
                        LocalTransition::StopServing { resource, was_owner } => {
                            let next_role = local_assignment_role(&next, node_id, resource);
                            if *was_owner {
                                prop_assert_eq!(
                                    local_assignment_role(&previous, node_id, resource),
                                    LocalRole::Owner
                                );
                            } else {
                                prop_assert_eq!(
                                    local_assignment_role(&previous, node_id, resource),
                                    LocalRole::Follower
                                );
                            }
                            prop_assert_eq!(next_role, LocalRole::None);
                        }
                    }

                    seen.insert(transition_resource.clone());
                }
                prop_assert_eq!(seen.len(), transitions.len());
            }
        }

        #[test]
        fn fuzz_planner_rejects_empty_cluster_with_resources(
            resource_values in prop::collection::vec(1u16..5000, 1..12),
        ) {
            let resources = resource_values
                .into_iter()
                .map(|value| {
                    ResourceIdentity::new(
                        format!("ns-{value}"),
                        format!("topic-{value}"),
                        u64::from(value) % 7,
                        None::<String>,
                    )
                })
                .collect::<Vec<_>>();
            let mut deduped_resources = Vec::new();
            for resource in resources {
                if !deduped_resources.contains(&resource) {
                    deduped_resources.push(resource);
                }
            }
            prop_assume!(!deduped_resources.is_empty());

            let input = PlacementInput {
                nodes: BTreeMap::new(),
                resources: deduped_resources,
                existing: BTreeMap::new(),
                target_followers: 1,
                generation: 1,
            };

            let err = DeterministicPartitionPlacement
                .plan(input)
                .expect_err("empty node set should reject active resources");
            prop_assert!(matches!(err, PlacementError::NoNodesForResources));
        }
    }
}
