use std::fmt;
use std::sync::RwLock;

use ganglion_core::{
    CoordinationSnapshot, PartitionPlacementPolicy, PlacementError, PlacementInput as PlannerInput,
};
use ganglion_storage::{
    FileMetadataLog, FileMetadataReplayPolicy, InMemoryMetadataLog, MetadataLog, MetadataLogEntry,
    MetadataLogError,
};

/// A narrow error surface for the initial adapter scaffold.
#[derive(Debug, Clone)]
pub enum OpenraftAdapterError {
    NotLeader,
    StaleGeneration,
    PoisonedState,
    StaleTerm,
    Planner(PlacementError),
    Config(String),
    Storage(String),
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
            Self::Config(error) => write!(f, "configuration error: {error}"),
            Self::Planner(error) => write!(f, "planner error: {:?}", error),
            Self::Storage(error) => write!(f, "storage error: {error}"),
        }
    }
}

impl From<MetadataLogError> for OpenraftAdapterError {
    fn from(error: MetadataLogError) -> Self {
        Self::Storage(error.to_string())
    }
}

/// Configurable startup replay profile for persisted metadata recovery.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PersistedMetadataReplayProfile {
    /// Reject malformed/non-sequential tails during startup.
    Strict,
    /// Allow a bounded tail of malformed lines before startup continues.
    Default,
    /// Use an explicit bounded-tail threshold during startup recovery.
    TruncateTail { max_tail_lines: usize },
}

impl PersistedMetadataReplayProfile {
    const DEFAULT_TAIL_REPLAY_LIMIT: usize = 1;
    const ENV_REPLAY_PROFILE: &'static str = "GANGLION_PERSISTED_REPLAY_PROFILE";

    pub fn env_var_name() -> &'static str {
        Self::ENV_REPLAY_PROFILE
    }

    pub const fn to_replay_policy(self) -> FileMetadataReplayPolicy {
        match self {
            Self::Strict => FileMetadataReplayPolicy::Strict,
            Self::Default => FileMetadataReplayPolicy::TruncateTail {
                max_tail_lines: Self::DEFAULT_TAIL_REPLAY_LIMIT,
            },
            Self::TruncateTail { max_tail_lines } => {
                FileMetadataReplayPolicy::TruncateTail { max_tail_lines }
            }
        }
    }

    pub const fn from_replay_policy(policy: FileMetadataReplayPolicy) -> Self {
        match policy {
            FileMetadataReplayPolicy::Strict => Self::Strict,
            FileMetadataReplayPolicy::TruncateTail { max_tail_lines } => {
                if max_tail_lines == Self::DEFAULT_TAIL_REPLAY_LIMIT {
                    Self::Default
                } else {
                    Self::TruncateTail { max_tail_lines }
                }
            }
        }
    }

    pub fn from_env_var() -> Result<Self, OpenraftAdapterError> {
        match std::env::var(Self::ENV_REPLAY_PROFILE) {
            Ok(raw) => raw.parse::<Self>(),
            Err(std::env::VarError::NotPresent) => Ok(Self::Default),
            Err(error) => Err(OpenraftAdapterError::Config(format!(
                "failed to read {}: {error}",
                Self::ENV_REPLAY_PROFILE
            ))),
        }
    }
}

impl std::str::FromStr for PersistedMetadataReplayProfile {
    type Err = OpenraftAdapterError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let normalized = raw.trim().to_ascii_lowercase();

        if normalized.is_empty() || normalized == "default" {
            return Ok(Self::Default);
        }

        if normalized == "strict" {
            return Ok(Self::Strict);
        }

        if normalized == "resilient" {
            return Ok(Self::Default);
        }

        let parse_tail_limit = |suffix: &str| -> Result<Self, OpenraftAdapterError> {
            let max_tail_lines = suffix.parse::<usize>().map_err(|error| {
                OpenraftAdapterError::Config(format!("invalid tail limit `{suffix}`: {error}"))
            })?;
            Ok(Self::TruncateTail { max_tail_lines })
        };

        if let Some(suffix) = normalized.strip_prefix("truncate_tail:") {
            return parse_tail_limit(suffix);
        }

        if let Some(suffix) = normalized.strip_prefix("tail:") {
            return parse_tail_limit(suffix);
        }

        normalized
            .parse::<usize>()
            .map(|max_tail_lines| Self::TruncateTail { max_tail_lines })
            .map_err(|error| {
                OpenraftAdapterError::Config(format!(
                    "invalid persisted replay profile `{raw}`: {error}"
                ))
            })
    }
}

/// Trait contract for control-plane engines used by ganglion.
pub trait MetadataConsensus {
    fn local_node_id(&self) -> &str;
    fn leader_id(&self) -> Option<String>;
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

#[derive(Debug)]
struct OpenraftLikeStore {
    current_term: u64,
    leader: Option<String>,
    snapshot: CoordinationSnapshot,
}

impl OpenraftLikeStore {
    fn new(
        log: &dyn MetadataLog,
        initial_snapshot: CoordinationSnapshot,
    ) -> Result<Self, OpenraftAdapterError> {
        let mut current_term = 1u64;
        let snapshot = match log.latest_entry()? {
            Some(entry) => {
                current_term = entry.term;
                entry.snapshot
            }
            None => initial_snapshot,
        };

        Ok(Self {
            current_term,
            leader: None,
            snapshot,
        })
    }

    fn is_leader(&self, node_id: &str) -> bool {
        self.leader.as_deref() == Some(node_id)
    }

    fn append_snapshot(
        &mut self,
        proposer: &str,
        snapshot: CoordinationSnapshot,
        term: u64,
        log: &dyn MetadataLog,
    ) -> Result<MetadataLogEntry, OpenraftAdapterError> {
        if !self.is_leader(proposer) {
            return Err(OpenraftAdapterError::NotLeader);
        }

        if term < self.current_term {
            return Err(OpenraftAdapterError::StaleTerm);
        }

        if snapshot.generation < self.snapshot.generation {
            return Err(OpenraftAdapterError::StaleGeneration);
        }

        if term > self.current_term {
            self.current_term = term;
            log.clear()?;
        }

        let entry = log.append_entry(term, snapshot.clone())?;
        self.snapshot = snapshot;
        Ok(entry)
    }
}

#[derive(Debug)]
struct MetadataNode {
    local_node_id: String,
    store: RwLock<OpenraftLikeStore>,
    log: Box<dyn MetadataLog>,
}

impl MetadataNode {
    fn new(
        local_node_id: impl Into<String>,
        initial_snapshot: CoordinationSnapshot,
        log: Box<dyn MetadataLog>,
    ) -> Result<Self, OpenraftAdapterError> {
        let store = OpenraftLikeStore::new(log.as_ref(), initial_snapshot)?;
        Ok(Self {
            local_node_id: local_node_id.into(),
            store: RwLock::new(store),
            log,
        })
    }

    fn with_store_read<T>(
        &self,
        op: impl FnOnce(&OpenraftLikeStore) -> T,
    ) -> Result<T, OpenraftAdapterError> {
        let store = self
            .store
            .read()
            .map_err(|_| OpenraftAdapterError::PoisonedState)?;
        Ok(op(&store))
    }

    fn with_store_write<T>(
        &self,
        op: impl FnOnce(&mut OpenraftLikeStore) -> T,
    ) -> Result<T, OpenraftAdapterError> {
        let mut store = self
            .store
            .write()
            .map_err(|_| OpenraftAdapterError::PoisonedState)?;
        Ok(op(&mut store))
    }

    pub fn set_leader_term(&self, node_id: impl Into<String>, term: u64) {
        let node_id = node_id.into();
        let _ = self.with_store_write(|store| {
            if term >= store.current_term {
                store.current_term = term;
            }

            store.leader = Some(node_id);
        });
    }

    pub fn set_leader(&self, leader_id: impl Into<String>) {
        let local_term = self.current_term();
        self.set_leader_term(leader_id, local_term);
    }

    pub fn clear_leader(&self) {
        let _ = self.with_store_write(|store| {
            store.leader = None;
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
        self.log
            .entries()
            .map(|entries| entries.len())
            .unwrap_or_default()
    }

    pub fn current_term(&self) -> u64 {
        self.with_store_read(|store| store.current_term)
            .unwrap_or_default()
    }

    pub fn last_index(&self) -> u64 {
        self.log
            .latest_entry()
            .ok()
            .and_then(|entry| entry.map(|entry| entry.index))
            .unwrap_or_default()
    }

    pub fn last_term(&self) -> u64 {
        self.log
            .latest_entry()
            .ok()
            .and_then(|entry| entry.map(|entry| entry.term))
            .unwrap_or_default()
    }

    pub fn local_node_id(&self) -> &str {
        &self.local_node_id
    }

    pub fn leader_id(&self) -> Option<String> {
        self.store
            .read()
            .ok()
            .and_then(|store| store.leader.clone())
    }

    pub fn is_leader(&self) -> bool {
        self.leader_id().as_deref() == Some(self.local_node_id())
    }

    pub fn snapshot(&self) -> CoordinationSnapshot {
        self.store
            .read()
            .map(|store| store.snapshot.clone())
            .unwrap_or_default()
    }

    pub fn apply_snapshot(
        &self,
        proposer: &str,
        snapshot: CoordinationSnapshot,
        term: Option<u64>,
    ) -> Result<(), OpenraftAdapterError> {
        let term = term.unwrap_or_else(|| self.current_term());
        self.with_store_write(|store| {
            store.append_snapshot(proposer, snapshot, term, self.log.as_ref())
        })??;
        Ok(())
    }
}

#[derive(Debug)]
pub struct InMemoryMetadataNode {
    inner: MetadataNode,
}

impl InMemoryMetadataNode {
    pub fn new(node_id: impl Into<String>, initial_snapshot: CoordinationSnapshot) -> Self {
        let log = Box::new(InMemoryMetadataLog::new());
        let inner = MetadataNode::new(node_id, initial_snapshot, log).expect("in-memory init");
        Self { inner }
    }

    pub fn set_leader_term(&self, node_id: impl Into<String>, term: u64) {
        self.inner.set_leader_term(node_id, term)
    }

    pub fn set_leader(&self, leader_id: impl Into<String>) {
        self.inner.set_leader(leader_id)
    }

    pub fn clear_leader(&self) {
        self.inner.clear_leader()
    }

    pub fn plan_and_apply(
        &self,
        proposer: &str,
        planner: &dyn PartitionPlacementPolicy,
        input: PlannerInput,
    ) -> Result<CoordinationSnapshot, OpenraftAdapterError> {
        self.inner.plan_and_apply(proposer, planner, input)
    }

    pub fn log_len(&self) -> usize {
        self.inner.log_len()
    }

    pub fn current_term(&self) -> u64 {
        self.inner.current_term()
    }

    pub fn last_index(&self) -> u64 {
        self.inner.last_index()
    }

    pub fn last_term(&self) -> u64 {
        self.inner.last_term()
    }
}

impl MetadataConsensus for InMemoryMetadataNode {
    fn local_node_id(&self) -> &str {
        self.inner.local_node_id()
    }

    fn leader_id(&self) -> Option<String> {
        self.inner.leader_id()
    }

    fn is_leader(&self) -> bool {
        self.inner.is_leader()
    }

    fn snapshot(&self) -> CoordinationSnapshot {
        self.inner.snapshot()
    }

    fn apply_snapshot(
        &self,
        proposer: &str,
        snapshot: CoordinationSnapshot,
        term: Option<u64>,
    ) -> Result<(), OpenraftAdapterError> {
        self.inner.apply_snapshot(proposer, snapshot, term)
    }
}

#[derive(Debug)]
pub struct PersistedMetadataNode {
    inner: MetadataNode,
    startup_replay_profile: PersistedMetadataReplayProfile,
}

impl PersistedMetadataNode {
    pub fn new<P: Into<std::path::PathBuf>>(
        path: P,
        node_id: impl Into<String>,
        initial_snapshot: CoordinationSnapshot,
    ) -> Result<Self, OpenraftAdapterError> {
        Self::new_with_replay_profile(
            path,
            node_id,
            initial_snapshot,
            PersistedMetadataReplayProfile::Default,
        )
    }

    pub fn new_strict<P: Into<std::path::PathBuf>>(
        path: P,
        node_id: impl Into<String>,
        initial_snapshot: CoordinationSnapshot,
    ) -> Result<Self, OpenraftAdapterError> {
        Self::new_with_replay_profile(
            path,
            node_id,
            initial_snapshot,
            PersistedMetadataReplayProfile::Strict,
        )
    }

    pub fn new_with_replay_profile<P: Into<std::path::PathBuf>>(
        path: P,
        node_id: impl Into<String>,
        initial_snapshot: CoordinationSnapshot,
        replay_profile: PersistedMetadataReplayProfile,
    ) -> Result<Self, OpenraftAdapterError> {
        Self::new_with_replay_policy(
            path,
            node_id,
            initial_snapshot,
            replay_profile.to_replay_policy(),
        )
    }

    pub fn new_with_tail_replay_limit<P: Into<std::path::PathBuf>>(
        path: P,
        node_id: impl Into<String>,
        initial_snapshot: CoordinationSnapshot,
        max_tail_lines: usize,
    ) -> Result<Self, OpenraftAdapterError> {
        Self::new_with_replay_profile(
            path,
            node_id,
            initial_snapshot,
            PersistedMetadataReplayProfile::TruncateTail { max_tail_lines },
        )
    }

    pub fn new_with_profile_env<P: Into<std::path::PathBuf>>(
        path: P,
        node_id: impl Into<String>,
        initial_snapshot: CoordinationSnapshot,
    ) -> Result<Self, OpenraftAdapterError> {
        let replay_profile = PersistedMetadataReplayProfile::from_env_var()?;
        Self::new_with_replay_profile(path, node_id, initial_snapshot, replay_profile)
    }

    pub fn new_from_env<P: Into<std::path::PathBuf>>(
        path: P,
        node_id: impl Into<String>,
        initial_snapshot: CoordinationSnapshot,
    ) -> Result<Self, OpenraftAdapterError> {
        Self::new_with_profile_env(path, node_id, initial_snapshot)
    }

    pub fn new_with_replay_policy<P: Into<std::path::PathBuf>>(
        path: P,
        node_id: impl Into<String>,
        initial_snapshot: CoordinationSnapshot,
        replay_policy: FileMetadataReplayPolicy,
    ) -> Result<Self, OpenraftAdapterError> {
        let path = path.into();
        let replay_profile = PersistedMetadataReplayProfile::from_replay_policy(replay_policy);
        let log = match replay_profile.to_replay_policy() {
            FileMetadataReplayPolicy::Strict => Box::new(FileMetadataLog::new(path)),
            FileMetadataReplayPolicy::TruncateTail { .. } => {
                Box::new(FileMetadataLog::with_replay_policy(path, replay_policy))
            }
        };
        let inner = MetadataNode::new(node_id, initial_snapshot, log)?;
        Ok(Self {
            inner,
            startup_replay_profile: replay_profile,
        })
    }

    pub fn startup_replay_profile(&self) -> PersistedMetadataReplayProfile {
        self.startup_replay_profile
    }

    pub fn startup_replay_policy(&self) -> FileMetadataReplayPolicy {
        self.startup_replay_profile.to_replay_policy()
    }

    pub fn set_leader_term(&self, node_id: impl Into<String>, term: u64) {
        self.inner.set_leader_term(node_id, term)
    }

    pub fn set_leader(&self, leader_id: impl Into<String>) {
        self.inner.set_leader(leader_id)
    }

    pub fn clear_leader(&self) {
        self.inner.clear_leader()
    }

    pub fn plan_and_apply(
        &self,
        proposer: &str,
        planner: &dyn PartitionPlacementPolicy,
        input: PlannerInput,
    ) -> Result<CoordinationSnapshot, OpenraftAdapterError> {
        self.inner.plan_and_apply(proposer, planner, input)
    }

    pub fn log_len(&self) -> usize {
        self.inner.log_len()
    }

    pub fn current_term(&self) -> u64 {
        self.inner.current_term()
    }

    pub fn last_index(&self) -> u64 {
        self.inner.last_index()
    }

    pub fn last_term(&self) -> u64 {
        self.inner.last_term()
    }
}

impl MetadataConsensus for PersistedMetadataNode {
    fn local_node_id(&self) -> &str {
        self.inner.local_node_id()
    }

    fn leader_id(&self) -> Option<String> {
        self.inner.leader_id()
    }

    fn is_leader(&self) -> bool {
        self.inner.is_leader()
    }

    fn snapshot(&self) -> CoordinationSnapshot {
        self.inner.snapshot()
    }

    fn apply_snapshot(
        &self,
        proposer: &str,
        snapshot: CoordinationSnapshot,
        term: Option<u64>,
    ) -> Result<(), OpenraftAdapterError> {
        self.inner.apply_snapshot(proposer, snapshot, term)
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

/// Simple planner export for convenience.
pub use ganglion_core::DeterministicPartitionPlacement;

#[cfg(test)]
mod tests {
    use super::*;
    use ganglion_coordination::{CoordinationProvider, InMemoryCoordination};
    use ganglion_core::ResourceIdentity;
    use proptest::prelude::*;
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::env;
    use std::fs;
    use std::rc::Rc;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn decorate_profile_input(
        raw: &str,
        leading_whitespace: usize,
        trailing_whitespace: usize,
        uppercase: bool,
    ) -> String {
        let mut text = String::with_capacity(raw.len() + leading_whitespace + trailing_whitespace);
        text.push_str(&" ".repeat(leading_whitespace));
        text.push_str(raw);
        text.push_str(&" ".repeat(trailing_whitespace));
        if uppercase {
            text.to_ascii_uppercase()
        } else {
            text
        }
    }

    static ENV_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn with_env_lock() -> MutexGuard<'static, ()> {
        ENV_TEST_LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    fn unique_temp_path(tag: &str) -> std::path::PathBuf {
        let mut path = env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or_else(|_| std::time::Duration::from_nanos(0), |duration| duration);
        path.push(format!(
            "ganglion-openraft-{tag}-{}-{}.log",
            std::process::id(),
            nanos.as_nanos()
        ));
        path
    }

    fn valid_replay_profile_inputs(
    ) -> impl proptest::prelude::Strategy<Value = (PersistedMetadataReplayProfile, String)> {
        prop_oneof![
            (0u8..4, 0u8..4, prop::bool::ANY).prop_map(
                |(leading, trailing, uppercase)| (
                    PersistedMetadataReplayProfile::Default,
                    decorate_profile_input(
                        "default",
                        leading as usize,
                        trailing as usize,
                        uppercase,
                    ),
                )
            ),
            (0u8..4, 0u8..4, prop::bool::ANY).prop_map(
                |(leading, trailing, uppercase)| (
                    PersistedMetadataReplayProfile::Default,
                    decorate_profile_input(
                        "resilient",
                        leading as usize,
                        trailing as usize,
                        uppercase,
                    ),
                )
            ),
            (0u8..4, 0u8..4, prop::bool::ANY).prop_map(
                |(leading, trailing, uppercase)| (
                    PersistedMetadataReplayProfile::Strict,
                    decorate_profile_input(
                        "strict",
                        leading as usize,
                        trailing as usize,
                        uppercase,
                    ),
                )
            ),
            (0u8..4, 0u8..4, 0..16usize, prop::bool::ANY).prop_map(
                |(leading, trailing, tail, uppercase)| (
                    PersistedMetadataReplayProfile::TruncateTail {
                        max_tail_lines: tail,
                    },
                    decorate_profile_input(
                        &format!("tail:{tail}"),
                        leading as usize,
                        trailing as usize,
                        uppercase,
                    ),
                )
            ),
            (0u8..4, 0u8..4, 0..16usize, prop::bool::ANY).prop_map(
                |(leading, trailing, tail, uppercase)| (
                    PersistedMetadataReplayProfile::TruncateTail {
                        max_tail_lines: tail,
                    },
                    decorate_profile_input(
                        &format!("truncate_tail:{tail}"),
                        leading as usize,
                        trailing as usize,
                        uppercase,
                    ),
                )
            ),
            (0u8..4, 0u8..4, 0..16usize, prop::bool::ANY).prop_map(
                |(leading, trailing, tail, uppercase)| (
                    PersistedMetadataReplayProfile::TruncateTail {
                        max_tail_lines: tail,
                    },
                    decorate_profile_input(
                        &format!("{tail}"),
                        leading as usize,
                        trailing as usize,
                        uppercase,
                    ),
                )
            ),
        ]
    }

    fn invalid_replay_profile_inputs() -> impl proptest::prelude::Strategy<Value = String> {
        prop_oneof![
            proptest::collection::vec(prop::char::range('a', 'z'), 1..12).prop_map(|chars| {
                let mut raw: String = chars.into_iter().collect();
                if matches!(raw.as_str(), "default" | "strict" | "resilient") {
                    raw.push_str("-bad");
                }
                raw
            }),
            (
                0u8..4,
                0u8..4,
                prop::bool::ANY,
                proptest::collection::vec(prop::char::range('a', 'z'), 1..12),
            )
                .prop_map(|(leading, trailing, uppercase, chars)| {
                    let candidate = chars.into_iter().collect::<String>();
                    decorate_profile_input(
                        &format!("tail:{candidate}"),
                        leading as usize,
                        trailing as usize,
                        uppercase,
                    )
                }),
        ]
    }

    fn build_default_nodes() -> BTreeMap<String, ganglion_core::NodeInfo> {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            "node-a".to_string(),
            ganglion_core::NodeInfo::new("node-a", "127.0.0.1:1", None::<String>),
        );
        nodes.insert(
            "node-b".to_string(),
            ganglion_core::NodeInfo::new("node-b", "127.0.0.1:2", None::<String>),
        );
        nodes
    }

    #[test]
    fn parse_persisted_replay_profile_values() {
        assert_eq!(
            "default"
                .parse::<PersistedMetadataReplayProfile>()
                .expect("default profile should parse"),
            PersistedMetadataReplayProfile::Default
        );
        assert_eq!(
            "".parse::<PersistedMetadataReplayProfile>()
                .expect("empty profile should resolve to default"),
            PersistedMetadataReplayProfile::Default
        );
        assert_eq!(
            "resilient"
                .parse::<PersistedMetadataReplayProfile>()
                .expect("resilient profile should parse"),
            PersistedMetadataReplayProfile::Default
        );
        assert_eq!(
            "strict"
                .parse::<PersistedMetadataReplayProfile>()
                .expect("strict profile should parse"),
            PersistedMetadataReplayProfile::Strict
        );
        assert_eq!(
            "tail:3"
                .parse::<PersistedMetadataReplayProfile>()
                .expect("explicit tail profile should parse"),
            PersistedMetadataReplayProfile::TruncateTail { max_tail_lines: 3 }
        );
        assert_eq!(
            "7".parse::<PersistedMetadataReplayProfile>()
                .expect("numeric profile should parse"),
            PersistedMetadataReplayProfile::TruncateTail { max_tail_lines: 7 }
        );

        assert!(
            "bad-profile"
                .parse::<PersistedMetadataReplayProfile>()
                .is_err(),
            "unknown profile should fail"
        );
    }

    #[test]
    fn persisted_node_startup_profile_is_resolved_and_diagnostics_available() {
        let default_node = PersistedMetadataNode::new(
            unique_temp_path("profile-default"),
            "node-a",
            CoordinationSnapshot::default(),
        )
        .expect("default constructor should build");
        assert_eq!(
            default_node.startup_replay_profile(),
            PersistedMetadataReplayProfile::Default,
            "default constructor should record default profile"
        );
        assert_eq!(
            default_node.startup_replay_policy(),
            FileMetadataReplayPolicy::TruncateTail { max_tail_lines: 1 },
            "profile diagnostics should expose replay policy"
        );

        let strict_node = PersistedMetadataNode::new_strict(
            unique_temp_path("profile-strict"),
            "node-a",
            CoordinationSnapshot::default(),
        )
        .expect("strict constructor should build");
        assert_eq!(
            strict_node.startup_replay_profile(),
            PersistedMetadataReplayProfile::Strict
        );

        let custom_node = PersistedMetadataNode::new_with_replay_profile(
            unique_temp_path("profile-custom"),
            "node-a",
            CoordinationSnapshot::default(),
            PersistedMetadataReplayProfile::TruncateTail { max_tail_lines: 4 },
        )
        .expect("custom profile should build");
        assert_eq!(
            custom_node.startup_replay_profile(),
            PersistedMetadataReplayProfile::TruncateTail { max_tail_lines: 4 }
        );
    }

    #[test]
    fn persisted_node_respects_replay_profile_env_var() {
        let _env_guard = with_env_lock();
        let env_var = PersistedMetadataReplayProfile::env_var_name();
        let original = env::var_os(env_var);
        env::set_var(env_var, "strict");

        let node = PersistedMetadataNode::new_from_env(
            unique_temp_path("profile-env"),
            "node-a",
            CoordinationSnapshot::default(),
        )
        .expect("env-driven constructor should build");
        assert_eq!(
            node.startup_replay_profile(),
            PersistedMetadataReplayProfile::Strict
        );

        match original {
            Some(value) => env::set_var(env_var, value),
            None => env::remove_var(env_var),
        }
    }

    #[test]
    fn persisted_node_replay_profile_rejects_invalid_env_value() {
        let _env_guard = with_env_lock();
        let env_var = PersistedMetadataReplayProfile::env_var_name();
        let original = env::var_os(env_var);
        env::set_var(env_var, "tail:not-a-number");

        let err = PersistedMetadataNode::new_from_env(
            unique_temp_path("profile-env-invalid"),
            "node-a",
            CoordinationSnapshot::default(),
        )
        .expect_err("invalid env profile should fail");
        assert!(matches!(err, OpenraftAdapterError::Config(_)));

        match original {
            Some(value) => env::set_var(env_var, value),
            None => env::remove_var(env_var),
        }
    }

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
        let node = InMemoryMetadataNode::new("node-a", CoordinationSnapshot::default());
        node.set_leader("node-a");

        let input = PlannerInput {
            nodes: build_default_nodes(),
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
        assert_eq!(node.leader_id(), Some("node-remote".to_string()));
        assert!(!node.is_leader());
    }

    #[test]
    fn control_loop_publishes_planned_snapshot_to_watchers() {
        let node = InMemoryMetadataNode::new("node-a", CoordinationSnapshot::default());
        node.set_leader("node-a");
        let input = PlannerInput {
            nodes: build_default_nodes(),
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
        let input = PlannerInput {
            nodes: {
                let mut nodes = BTreeMap::new();
                nodes.insert(
                    "node-a".to_string(),
                    ganglion_core::NodeInfo::new("node-a", "127.0.0.1:1", None::<String>),
                );
                nodes
            },
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

    #[test]
    fn persisted_metadata_node_roundtrips_state_and_replays_logs() {
        let path = unique_temp_path("roundtrip");
        let node =
            PersistedMetadataNode::new(path.clone(), "node-a", CoordinationSnapshot::default())
                .expect("persisted node should initialize");
        node.set_leader("node-a");
        node.apply_snapshot(
            "node-a",
            CoordinationSnapshot {
                generation: 5,
                ..CoordinationSnapshot::default()
            },
            Some(2),
        )
        .expect("first persisted commit");
        assert_eq!(node.snapshot().generation, 5);
        assert_eq!(node.log_len(), 1);

        let recovered = PersistedMetadataNode::new(
            path,
            "node-a",
            CoordinationSnapshot {
                generation: 0,
                ..CoordinationSnapshot::default()
            },
        )
        .expect("persisted node should recover state");
        assert_eq!(recovered.snapshot().generation, 5);
        assert_eq!(recovered.current_term(), 2);
        assert_eq!(recovered.log_len(), 1);
        recovered.set_leader("node-a");

        recovered
            .apply_snapshot(
                "node-a",
                CoordinationSnapshot {
                    generation: 6,
                    ..CoordinationSnapshot::default()
                },
                None,
            )
            .expect("recovered write should continue");
        assert_eq!(recovered.snapshot().generation, 6);
        assert_eq!(recovered.log_len(), 2);
    }

    #[test]
    fn persisted_node_control_loop_publishes_to_watchers() {
        let node = PersistedMetadataNode::new(
            unique_temp_path("control-loop"),
            "node-a",
            CoordinationSnapshot::default(),
        )
        .expect("persisted node should initialize");
        node.set_leader("node-a");

        let mut nodes = build_default_nodes();
        nodes.insert(
            "node-c".to_string(),
            ganglion_core::NodeInfo::new("node-c", "127.0.0.1:3", None::<String>),
        );
        let input = PlannerInput {
            nodes,
            resources: vec![ResourceIdentity::new("svc", "orders", 0, None::<String>)],
            existing: BTreeMap::new(),
            target_followers: 1,
            generation: 1,
        };

        let coordination = InMemoryCoordination::new(CoordinationSnapshot::default());
        let published = plan_and_publish(
            &node,
            "node-a",
            &DeterministicPartitionPlacement,
            input,
            |snapshot| coordination.update_snapshot(snapshot),
        )
        .expect("persisted control loop should publish");

        assert_eq!(published.generation, 1);
        assert_eq!(coordination.snapshot(), published);
    }

    #[test]
    fn persisted_node_rejects_stale_term_after_restart() {
        let path = unique_temp_path("stale-term");
        let writer =
            PersistedMetadataNode::new(path.clone(), "node-a", CoordinationSnapshot::default())
                .expect("writer should initialize");
        writer.set_leader("node-a");

        writer
            .apply_snapshot(
                "node-a",
                CoordinationSnapshot {
                    generation: 1,
                    ..Default::default()
                },
                Some(2),
            )
            .expect("first write");

        let reader = PersistedMetadataNode::new(path, "node-a", CoordinationSnapshot::default())
            .expect("reader should initialize");
        assert_eq!(reader.current_term(), 2);
        reader.set_leader("node-a");

        let result = reader.apply_snapshot(
            "node-a",
            CoordinationSnapshot {
                generation: 2,
                ..Default::default()
            },
            Some(1),
        );
        assert!(matches!(result, Err(OpenraftAdapterError::StaleTerm)));
    }

    #[test]
    fn persisted_node_resets_log_on_term_bump() {
        let node = PersistedMetadataNode::new(
            unique_temp_path("term-bump"),
            "node-a",
            CoordinationSnapshot::default(),
        )
        .expect("persisted node should initialize");
        node.set_leader_term("node-a", 1);
        node.apply_snapshot(
            "node-a",
            CoordinationSnapshot {
                generation: 1,
                ..Default::default()
            },
            None,
        )
        .expect("first write");
        node.apply_snapshot(
            "node-a",
            CoordinationSnapshot {
                generation: 2,
                ..Default::default()
            },
            Some(1),
        )
        .expect("second write same term");
        assert_eq!(node.log_len(), 2);

        node.apply_snapshot(
            "node-a",
            CoordinationSnapshot {
                generation: 3,
                ..Default::default()
            },
            Some(3),
        )
        .expect("higher term write");
        assert_eq!(node.current_term(), 3);
        assert_eq!(node.log_len(), 1);
        assert_eq!(node.last_index(), 1);
    }

    #[test]
    fn persisted_node_rejects_corrupt_file_log() {
        let path = unique_temp_path("corrupt-log");
        fs::write(&path, b"{not-json}\n").expect("write invalid log payload");

        let err =
            PersistedMetadataNode::new_strict(path, "node-a", CoordinationSnapshot::default())
                .expect_err("invalid log must be rejected");
        assert!(matches!(err, OpenraftAdapterError::Storage(_)));
    }

    #[test]
    fn persisted_node_tolerates_truncated_tail_corruption_when_enabled_by_default() {
        let path = unique_temp_path("tolerate-tail-default");
        let writer =
            PersistedMetadataNode::new(path.clone(), "node-a", CoordinationSnapshot::default())
                .expect("writer should initialize");
        writer.set_leader("node-a");
        writer
            .apply_snapshot(
                "node-a",
                CoordinationSnapshot {
                    generation: 1,
                    ..CoordinationSnapshot::default()
                },
                Some(1),
            )
            .expect("first write");
        writer
            .apply_snapshot(
                "node-a",
                CoordinationSnapshot {
                    generation: 2,
                    ..CoordinationSnapshot::default()
                },
                None,
            )
            .expect("second write");

        {
            use std::fs::OpenOptions;
            use std::io::Write as _;
            let mut file = OpenOptions::new()
                .append(true)
                .open(&path)
                .expect("append corrupted line");
            file.write_all(b"{not-json}\n")
                .expect("append malformed tail");
        }

        let recovered = PersistedMetadataNode::new(path, "node-a", CoordinationSnapshot::default())
            .expect("node should recover from bounded tail corruption by default");

        assert_eq!(recovered.snapshot().generation, 2);
        assert_eq!(recovered.current_term(), 1);
        assert_eq!(recovered.log_len(), 2);
    }

    #[test]
    fn persisted_node_tolerates_truncated_tail_corruption_when_explicit() {
        let path = unique_temp_path("tolerate-tail");
        let writer =
            PersistedMetadataNode::new(path.clone(), "node-a", CoordinationSnapshot::default())
                .expect("writer should initialize");
        writer.set_leader("node-a");
        writer
            .apply_snapshot(
                "node-a",
                CoordinationSnapshot {
                    generation: 1,
                    ..CoordinationSnapshot::default()
                },
                Some(1),
            )
            .expect("first write");
        writer
            .apply_snapshot(
                "node-a",
                CoordinationSnapshot {
                    generation: 2,
                    ..CoordinationSnapshot::default()
                },
                None,
            )
            .expect("second write");

        {
            use std::fs::OpenOptions;
            use std::io::Write as _;
            let mut file = OpenOptions::new()
                .append(true)
                .open(&path)
                .expect("append corrupted line");
            file.write_all(b"{not-json}\n")
                .expect("append malformed tail");
        }

        let recovered = PersistedMetadataNode::new_with_replay_policy(
            path,
            "node-a",
            CoordinationSnapshot::default(),
            FileMetadataReplayPolicy::TruncateTail { max_tail_lines: 1 },
        )
        .expect("node should recover from bounded tail corruption");

        assert_eq!(recovered.snapshot().generation, 2);
        assert_eq!(recovered.current_term(), 1);
        assert_eq!(recovered.log_len(), 2);
    }

    #[test]
    fn persisted_node_tolerates_truncated_tail_corruption_with_custom_limit() {
        let path = unique_temp_path("tolerate-tail-custom");
        let writer =
            PersistedMetadataNode::new(path.clone(), "node-a", CoordinationSnapshot::default())
                .expect("writer should initialize");
        writer.set_leader("node-a");
        writer
            .apply_snapshot(
                "node-a",
                CoordinationSnapshot {
                    generation: 1,
                    ..CoordinationSnapshot::default()
                },
                Some(1),
            )
            .expect("first write");
        writer
            .apply_snapshot(
                "node-a",
                CoordinationSnapshot {
                    generation: 2,
                    ..CoordinationSnapshot::default()
                },
                None,
            )
            .expect("second write");

        {
            use std::fs::OpenOptions;
            use std::io::Write as _;
            let mut file = OpenOptions::new()
                .append(true)
                .open(&path)
                .expect("append corrupted lines");
            file.write_all(b"{bad}\n{bad}\n")
                .expect("append malformed tail");
        }

        let recovered = PersistedMetadataNode::new_with_tail_replay_limit(
            path,
            "node-a",
            CoordinationSnapshot::default(),
            2,
        )
        .expect("node should recover when custom limit permits tail");

        assert_eq!(recovered.snapshot().generation, 2);
        assert_eq!(recovered.current_term(), 1);
        assert_eq!(recovered.log_len(), 2);
    }

    #[test]
    fn persisted_node_rejects_non_sequential_file_log_indexes() {
        let path = unique_temp_path("non-sequential-log");
        let payload = r#"{"term":1,"index":1,"snapshot":{"nodes":{},"assignments":{},"generation":1}}
{"term":1,"index":3,"snapshot":{"nodes":{},"assignments":{},"generation":2}}
"#;
        fs::write(&path, payload.as_bytes()).expect("write test payload");

        let err =
            PersistedMetadataNode::new_strict(path, "node-a", CoordinationSnapshot::default())
                .expect_err("out-of-order log must be rejected");
        assert!(matches!(err, OpenraftAdapterError::Storage(_)));
    }

    proptest! {
        #[test]
        fn fuzz_persisted_replay_profile_parsing_and_mapping(
            (expected_profile, raw_profile) in valid_replay_profile_inputs()
        ) {
            let parsed = raw_profile
                .parse::<PersistedMetadataReplayProfile>()
                .expect("generated profile string should parse");

            prop_assert_eq!(parsed, expected_profile);

            let node = PersistedMetadataNode::new_with_replay_profile(
                unique_temp_path("fuzz-profile-constructor"),
                "node-a",
                CoordinationSnapshot::default(),
                parsed,
            )
            .expect("constructor should succeed for parsed profile");

            let startup_policy = node.startup_replay_policy();
            let startup_profile =
                PersistedMetadataReplayProfile::from_replay_policy(startup_policy);

            prop_assert_eq!(startup_profile.to_replay_policy(), parsed.to_replay_policy());
            prop_assert_eq!(
                startup_profile,
                PersistedMetadataReplayProfile::from_replay_policy(expected_profile.to_replay_policy())
            );
        }

        #[test]
        fn fuzz_replay_profile_parsing_rejects_invalid_inputs(
            invalid_profile in invalid_replay_profile_inputs()
        ) {
            prop_assert!(
                invalid_profile
                    .parse::<PersistedMetadataReplayProfile>()
                    .is_err()
            );
        }

        #[test]
    fn fuzz_control_loop_publishing_and_rejection_matrix(
        base_term in 1u64..6,
        initial_generation in 0u64..6,
        next_generation in 0u64..8,
            proposer_choice in 0u8..3,
            leader_choice in 0u8..3,
            nodes_count in 1u8..4,
        ) {
            let mut base_nodes = BTreeMap::new();
            for idx in 0..nodes_count {
                let node_id = format!("node-{idx}");
                base_nodes.insert(
                    node_id.clone(),
                    ganglion_core::NodeInfo::new(
                        node_id.clone(),
                        format!("127.0.0.1:{}", 10_000u16 + u16::from(idx)),
                        None::<String>,
                    ),
                );
            }

            let node = InMemoryMetadataNode::new("node-a", CoordinationSnapshot {
                generation: initial_generation,
                ..CoordinationSnapshot::default()
            });

            let set_leader = match leader_choice {
                0 => Some("node-a".to_string()),
                1 => Some("node-b".to_string()),
                _ => None,
            };
            if let Some(leader) = set_leader.as_deref() {
                node.set_leader_term(leader, base_term);
            } else {
                node.clear_leader();
            }

            let proposer = match proposer_choice {
                0 => "node-a",
                1 => "node-b",
                _ => "node-c",
            };

            let input = PlannerInput {
                nodes: base_nodes,
                resources: vec![ResourceIdentity::new("svc", "orders", 0, None::<String>)],
                existing: BTreeMap::new(),
                target_followers: 1,
                generation: next_generation,
            };

            let published = Rc::new(RefCell::new(None::<CoordinationSnapshot>));
            let publish_snapshot = Rc::clone(&published);

            let result = plan_and_publish(
                &node,
                proposer,
                &DeterministicPartitionPlacement,
                input,
                move |snapshot| {
                    *publish_snapshot.borrow_mut() = Some(snapshot);
                },
            );

            let expected_error = if !matches!(leader_choice, 0 | 1) {
                Some(OpenraftAdapterError::NotLeader)
            } else if proposer != set_leader.as_deref().unwrap_or("") {
                Some(OpenraftAdapterError::NotLeader)
            } else if next_generation < initial_generation {
                Some(OpenraftAdapterError::StaleGeneration)
            } else {
                None
            };

            match expected_error {
                None => {
                    prop_assert!(result.is_ok());
                    let published = published.borrow();
                    let snapshot = published.as_ref().expect("publish should occur on success");
                    prop_assert_eq!(snapshot.generation, next_generation);
                    prop_assert_eq!(node.snapshot().generation, next_generation);
                    prop_assert_eq!(node.current_term(), base_term);
                }
                Some(ref expected_error) => {
                    match result {
                        Ok(_) => {
                            prop_assert!(false, "expected rejection not success");
                        }
                        Err(actual_error) => {
                            prop_assert_eq!(
                                std::mem::discriminant(&actual_error),
                                std::mem::discriminant(expected_error)
                            );
                        }
                    }
                    prop_assert!(published.borrow().is_none());
                }
            }
        }

        #[test]
        fn fuzz_apply_snapshot_handles_term_and_generation_rejections(
            base_term in 1u64..6,
            use_term in prop::bool::ANY,
            next_term in 0u64..8,
            initial_generation in 0u64..6,
            next_generation in 0u64..8,
        ) {
            let node = InMemoryMetadataNode::new(
                "node-a",
                CoordinationSnapshot {
                    generation: initial_generation,
                    ..CoordinationSnapshot::default()
                },
            );
            node.set_leader_term("node-a", base_term);

            let snapshot = CoordinationSnapshot {
                generation: next_generation,
                ..CoordinationSnapshot::default()
            };

            let term = if use_term { Some(next_term) } else { None };
            let expected_error = if let Some(term) = term {
                if term < base_term {
                    Some(OpenraftAdapterError::StaleTerm)
                } else if next_generation < initial_generation {
                    Some(OpenraftAdapterError::StaleGeneration)
                } else {
                    None
                }
            } else if next_generation < initial_generation {
                Some(OpenraftAdapterError::StaleGeneration)
            } else {
                None
            };

            let result = node.apply_snapshot("node-a", snapshot, term);
            match expected_error {
                None => {
                    prop_assert!(result.is_ok());
                    prop_assert_eq!(node.snapshot().generation, next_generation);
                    let expected_term = term.unwrap_or(base_term);
                    prop_assert_eq!(node.current_term(), expected_term.max(base_term));
                }
                Some(expected_error) => {
                    prop_assert!(matches!(
                        result,
                        Err(actual_error)
                            if std::mem::discriminant(&actual_error)
                                == std::mem::discriminant(&expected_error)
                    ));
                }
            }
        }
    }

    #[test]
    fn repro_fuzz_control_loop_publish_case() {
        let mut base_nodes = BTreeMap::new();
        base_nodes.insert(
            "node-0".to_string(),
            ganglion_core::NodeInfo::new("node-0", "127.0.0.1:10000", None::<String>),
        );
        let node = InMemoryMetadataNode::new(
            "node-a",
            CoordinationSnapshot {
                generation: 0,
                ..CoordinationSnapshot::default()
            },
        );
        node.set_leader_term("node-b", 1);

        let input = PlannerInput {
            nodes: base_nodes,
            resources: vec![ResourceIdentity::new("svc", "orders", 0, None::<String>)],
            existing: BTreeMap::new(),
            target_followers: 1,
            generation: 0,
        };

        let published = Rc::new(RefCell::new(None::<CoordinationSnapshot>));
        let publish_snapshot = Rc::clone(&published);

        let result = plan_and_publish(
            &node,
            "node-b",
            &DeterministicPartitionPlacement,
            input,
            move |snapshot| {
                *publish_snapshot.borrow_mut() = Some(snapshot);
            },
        );

        assert!(result.is_ok(), "{result:?}");
        assert!(published.borrow().is_some());
    }
}
