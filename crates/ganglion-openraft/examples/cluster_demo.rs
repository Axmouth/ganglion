//! Interactive cluster playground: N durable raft nodes in one process.
//!
//! Usage:
//!   cargo run -p ganglion-openraft --features openraft --example cluster_demo -- \
//!       [--nodes N] [--data-dir DIR] [--script "status; write 5; kill 2; status; quit"]
//!
//! Without `--script`, reads commands from stdin. Commands:
//!   status            topology + telemetry + committed generation per node
//!   write <gen>       propose a snapshot with that generation via the leader
//!   kill <id>         partition + shut down a node
//!   restart <id>      restart a killed node from its data dir
//!   add <id>          start a new node, add as learner, promote to voter
//!   remove <id>       demote and remove a node from the cluster
//!   quit              shut everything down and exit

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use ganglion_core::CoordinationSnapshot;
use ganglion_openraft::{
    default_raft_config, openraft::BasicNode, FileRaftLogStore, InProcessRouter, RaftMetadataNode,
};

type Node = RaftMetadataNode<FileRaftLogStore>;

struct Cluster {
    router: InProcessRouter<FileRaftLogStore>,
    config: Arc<ganglion_openraft::openraft::Config>,
    data_dir: std::path::PathBuf,
    nodes: BTreeMap<u64, Node>,
}

impl Cluster {
    fn node_dir(&self, id: u64) -> std::path::PathBuf {
        self.data_dir.join(format!("node-{id}"))
    }

    async fn leader(&self) -> Option<(u64, &Node)> {
        for (id, node) in &self.nodes {
            if node.is_leader().await {
                return Some((*id, node));
            }
        }
        None
    }

    async fn wait_for_leader(&self) -> Option<(u64, &Node)> {
        for _ in 0..100 {
            if let Some(found) = self.leader().await {
                return Some(found);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        None
    }

    async fn status(&self) {
        match self.wait_for_leader().await {
            Some((leader_id, _)) => println!("leader: node {leader_id}"),
            None => println!("leader: none (no quorum?)"),
        }
        for (id, node) in &self.nodes {
            let topology = node.topology();
            let telemetry = node.telemetry();
            println!(
                "node {id}: leader={:?} voters={:?} learners={:?} generation={} applied={:?} \
                 appends={} fsyncs={} compactions={} snap_persists={}",
                topology.leader,
                topology.voters,
                topology.learners,
                topology.committed_generation,
                topology.last_applied_index,
                telemetry.appended_records,
                telemetry.fsyncs,
                telemetry.compactions,
                telemetry.snapshot_persists,
            );
        }
    }

    async fn write(&self, generation: u64) {
        let Some((leader_id, leader)) = self.wait_for_leader().await else {
            println!("write failed: no leader");
            return;
        };
        let snapshot = CoordinationSnapshot {
            generation,
            ..CoordinationSnapshot::default()
        };
        match leader.write_snapshot(snapshot).await {
            Ok(response) => println!(
                "write committed via node {leader_id}: generation={}",
                response.snapshot.generation
            ),
            Err(error) => println!("write failed: {error}"),
        }
    }

    async fn kill(&mut self, id: u64) {
        let Some(node) = self.nodes.remove(&id) else {
            println!("no such node: {id}");
            return;
        };
        self.router.deregister(id);
        match node.shutdown().await {
            Ok(()) => println!("node {id} killed"),
            Err(error) => println!("node {id} shutdown error: {error}"),
        }
    }

    async fn restart(&mut self, id: u64) {
        if self.nodes.contains_key(&id) {
            println!("node {id} is already running");
            return;
        }
        match RaftMetadataNode::start_durable(
            id,
            self.config.clone(),
            &self.router,
            self.node_dir(id),
        )
        .await
        {
            Ok(node) => {
                self.nodes.insert(id, node);
                println!("node {id} restarted from {}", self.node_dir(id).display());
            }
            Err(error) => println!("restart failed: {error}"),
        }
    }

    async fn add(&mut self, id: u64) {
        if self.nodes.contains_key(&id) {
            println!("node {id} already exists");
            return;
        }
        let node = match RaftMetadataNode::start_durable(
            id,
            self.config.clone(),
            &self.router,
            self.node_dir(id),
        )
        .await
        {
            Ok(node) => node,
            Err(error) => {
                println!("start failed: {error}");
                return;
            }
        };
        self.nodes.insert(id, node);

        let Some((_, leader)) = self.wait_for_leader().await else {
            println!("add failed: no leader");
            return;
        };
        if let Err(error) = leader
            .add_learner(id, BasicNode::new(format!("node-{id}")), true)
            .await
        {
            println!("add_learner failed: {error}");
            return;
        }
        let mut voters: Vec<u64> = leader.topology().voters;
        voters.push(id);
        match leader.change_membership(voters, false).await {
            Ok(()) => println!("node {id} added and promoted to voter"),
            Err(error) => println!("promotion failed: {error}"),
        }
    }

    async fn remove(&mut self, id: u64) {
        let Some((_, leader)) = self.wait_for_leader().await else {
            println!("remove failed: no leader");
            return;
        };
        let voters: Vec<u64> = leader
            .topology()
            .voters
            .into_iter()
            .filter(|voter| *voter != id)
            .collect();
        if let Err(error) = leader.change_membership(voters, false).await {
            println!("remove failed: {error}");
            return;
        }
        self.kill(id).await;
        println!("node {id} removed from the cluster");
    }

    async fn shutdown(&mut self) {
        let ids: Vec<u64> = self.nodes.keys().copied().collect();
        for id in ids {
            if let Some(node) = self.nodes.remove(&id) {
                let _ = node.shutdown().await;
            }
        }
    }
}

async fn run_command(cluster: &mut Cluster, line: &str) -> bool {
    let mut parts = line.split_whitespace();
    let command = parts.next().unwrap_or("");
    let arg = parts.next().and_then(|raw| raw.parse::<u64>().ok());

    match (command, arg) {
        ("", _) => {}
        ("status", _) => cluster.status().await,
        ("write", Some(generation)) => cluster.write(generation).await,
        ("kill", Some(id)) => cluster.kill(id).await,
        ("restart", Some(id)) => cluster.restart(id).await,
        ("add", Some(id)) => cluster.add(id).await,
        ("remove", Some(id)) => cluster.remove(id).await,
        ("quit", _) | ("exit", _) => return false,
        _ => println!("commands: status | write <gen> | kill <id> | restart <id> | add <id> | remove <id> | quit"),
    }
    true
}

fn parse_args() -> (u64, std::path::PathBuf, Option<String>) {
    let mut nodes = 3u64;
    let mut data_dir =
        std::env::temp_dir().join(format!("ganglion-playground-{}", std::process::id()));
    let mut script = None;

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--nodes" => {
                nodes = args
                    .next()
                    .and_then(|raw| raw.parse().ok())
                    .expect("--nodes needs a number");
            }
            "--data-dir" => {
                data_dir = args.next().expect("--data-dir needs a path").into();
            }
            "--script" => {
                script = Some(args.next().expect("--script needs commands"));
            }
            other => panic!("unknown argument: {other}"),
        }
    }
    (nodes, data_dir, script)
}

fn main() {
    let (node_count, data_dir, script) = parse_args();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("tokio runtime");

    runtime.block_on(async {
        let config = default_raft_config().expect("raft config");
        let router = InProcessRouter::new();
        let mut cluster = Cluster {
            router: router.clone(),
            config: config.clone(),
            data_dir,
            nodes: BTreeMap::new(),
        };

        let ids: Vec<u64> = (1..=node_count).collect();
        for id in &ids {
            let node = RaftMetadataNode::start_durable(
                *id,
                config.clone(),
                &router,
                cluster.node_dir(*id),
            )
            .await
            .expect("node should start");
            cluster.nodes.insert(*id, node);
        }

        let members: BTreeMap<u64, BasicNode> = ids
            .iter()
            .map(|id| (*id, BasicNode::new(format!("node-{id}"))))
            .collect();
        cluster.nodes[&ids[0]]
            .initialize(members)
            .await
            .expect("cluster should initialize");
        println!(
            "started {} durable nodes under {}",
            node_count,
            cluster.data_dir.display()
        );
        cluster.status().await;

        match script {
            Some(script) => {
                for command in script.split(';') {
                    let command = command.trim();
                    println!("> {command}");
                    if !run_command(&mut cluster, command).await {
                        break;
                    }
                }
            }
            None => {
                use std::io::BufRead as _;
                let stdin = std::io::stdin();
                print!("> ");
                use std::io::Write as _;
                std::io::stdout().flush().ok();
                for line in stdin.lock().lines() {
                    let line = line.expect("stdin read");
                    if !run_command(&mut cluster, line.trim()).await {
                        break;
                    }
                    print!("> ");
                    std::io::stdout().flush().ok();
                }
            }
        }

        cluster.shutdown().await;
        println!("playground stopped");
    });
}
