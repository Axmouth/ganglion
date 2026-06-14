# Ganglion Examples

These examples show the current shape of the API. Ganglion is still pre-release,
so treat the snippets as orientation rather than a stable promise.

## Model A Resource

Ganglion does not know what your resource is. A resource is a namespace, a name,
a partition, and an optional group.

```rust
use ganglion_core::ResourceIdentity;

let resource = ResourceIdentity::new(
    "my-system/shard",
    "orders",
    0,
    Some("workers".to_string()),
);
```

Use the namespace to keep your application's resources separate from other
consumers that may share the same coordination plane.

## Plan Ownership

Placement strategies are pure functions. They take live nodes, desired
resources, existing assignments, and a follower target, then return a desired
snapshot.

```rust
use std::collections::BTreeMap;

use ganglion_core::{
    DeterministicPartitionPlacement, NodeInfo, PartitionPlacementPolicy,
    PlacementInput, ResourceIdentity,
};

let resource = ResourceIdentity::new("my-system/shard", "orders", 0, None::<String>);

let mut nodes = BTreeMap::new();
nodes.insert(
    "node-a".to_string(),
    NodeInfo::new("node-a", "127.0.0.1:7001", None::<String>),
);
nodes.insert(
    "node-b".to_string(),
    NodeInfo::new("node-b", "127.0.0.1:7002", None::<String>),
);

let plan = DeterministicPartitionPlacement.plan(PlacementInput {
    nodes,
    resources: vec![resource.clone()],
    existing: BTreeMap::new(),
    target_followers: 1,
    generation: 1,
})?;

let assignment = plan.snapshot.assignment_for(&resource).unwrap();
assert_eq!(assignment.replica_set_size(), 2);
Ok::<(), Box<dyn std::error::Error>>(())
```

Existing live owners are preserved where possible. Owner changes bump epochs
when `stamp_assignment_epochs` or `plan_and_propose_guarded` is used.

## Start A Single In-Memory Metadata Node

For tests or local development, an in-memory node is enough.

```rust
use std::collections::BTreeMap;

use ganglion_openraft::{default_raft_config, InProcessRouter, RaftMetadataNode};
use ganglion_openraft::openraft::BasicNode;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let router = InProcessRouter::new();
    let node = RaftMetadataNode::start(1, default_raft_config()?, &router).await?;

    let mut members = BTreeMap::new();
    members.insert(1, BasicNode::new("node-1"));
    node.initialize(members).await?;
    node.wait_for_leader(1, std::time::Duration::from_secs(10)).await?;

    let snapshot = node.committed_snapshot();
    println!("generation={}", snapshot.generation);

    node.shutdown().await?;
    Ok(())
}
```

Production-like tests should use the durable or TCP constructors so restart,
membership, and transport behavior are exercised.

## Register Resources And Watch Commits

Consumers usually register resources, then watch committed snapshots and react
when assignments or attributes change.

```rust
use ganglion_core::ResourceIdentity;

async fn example(node: ganglion_openraft::RaftMetadataNode) -> Result<(), Box<dyn std::error::Error>> {
    let resource = ResourceIdentity::new("my-system/shard", "orders", 0, None::<String>);

    let mut watch = node.watch_committed();
    node.register_resource(resource.clone()).await?;

    while watch.borrow_and_update().resources.get(&resource).is_none() {
        watch.changed().await?;
    }

    println!("resource is committed");
    Ok(())
}
```

The watch stream is the main consumption surface. Reads are cheap and local.
Writes still go through the metadata leader.

## Use Guarded Attributes

Attributes are small opaque control documents owned by the consumer. Use a CAS
write when concurrent writers must not silently clobber each other.

```rust
async fn example(node: ganglion_openraft::RaftMetadataNode) -> Result<(), Box<dyn std::error::Error>> {
    let key = "my-system/runtime-settings".to_string();
    let first = r#"{"version":1,"poll_ms":1000}"#.to_string();

    node.compare_and_set_attribute(key.clone(), None, first.clone()).await?;

    let next = r#"{"version":2,"poll_ms":500}"#.to_string();
    node.compare_and_set_attribute(key, Some(first), next).await?;
    Ok(())
}
```

Ganglion stores the value and decides the CAS. Your application owns the schema,
validation, and conflict policy inside that value.

## Controller Loop Shape

A controller loop normally does this:

1. Read the committed snapshot.
2. Compute a desired snapshot using pure planning logic.
3. Stamp epochs.
4. Propose with a generation guard.
5. Retry if another writer won the race.

`RaftMetadataNode::plan_and_propose_guarded` wraps that pattern for snapshot
writers. Smaller merge commands such as resource registration and attribute CAS
should be preferred when full snapshot replacement is not needed.
