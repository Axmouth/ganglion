//! Exercise detached replication tasks in a subprocess: a test can otherwise
//! pass even when Tokio reports a panic in one of OpenRaft's background tasks.
use super::*;
use openraft::storage::{LogFlushed, RaftLogStorage};
use openraft::{Entry, LogId, LogState, RaftLogReader, StorageError, Vote};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{collections::BTreeMap, fmt::Debug, ops::RangeBounds, sync::Arc, time::Duration};

#[derive(Clone, Default)]
struct EmptyReads {
    inner: GanglionLogStore,
    injected: Arc<AtomicUsize>,
    remaining: Arc<AtomicUsize>,
}

impl RaftLogReader<GanglionRaftConfig> for EmptyReads {
    async fn try_get_log_entries<R: RangeBounds<u64> + Clone + Debug + openraft::OptionalSend>(
        &mut self,
        range: R,
    ) -> Result<Vec<Entry<GanglionRaftConfig>>, StorageError<u64>> {
        self.inner.try_get_log_entries(range).await
    }

    async fn limited_get_log_entries(
        &mut self,
        start: u64,
        end: u64,
    ) -> Result<Vec<Entry<GanglionRaftConfig>>, StorageError<u64>> {
        let entries = self.inner.try_get_log_entries(start..end).await?;
        // Deliberately violate the reader contract. Published 0.9.24 and
        // 0.9.25 panic in different tasks for the initial index-zero request.
        if !entries.is_empty()
            && self
                .remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        {
            self.injected.fetch_add(1, Ordering::SeqCst);
            return Ok(vec![]);
        }
        Ok(entries)
    }
}

impl RaftLogStorage<GanglionRaftConfig> for EmptyReads {
    type LogReader = Self;
    async fn get_log_state(&mut self) -> Result<LogState<GanglionRaftConfig>, StorageError<u64>> {
        self.inner.get_log_state().await
    }
    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }
    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.inner.save_vote(vote).await
    }
    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        self.inner.read_vote().await
    }
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<GanglionRaftConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<GanglionRaftConfig>> + Send,
        I::IntoIter: Send,
    {
        self.inner.append(entries, callback).await
    }
    async fn truncate(&mut self, id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.inner.truncate(id).await
    }
    async fn purge(&mut self, id: LogId<u64>) -> Result<(), StorageError<u64>> {
        self.inner.purge(id).await
    }
}

#[test]
fn empty_replication_read_retries_without_background_panic() {
    const CHILD: &str = "GANGLION_EMPTY_REPLICATION_READ_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "openraft_runtime::replication_regression::empty_replication_read_retries_without_background_panic", "--nocapture"])
            .env(CHILD, "1")
            .output().unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            output.status.success() && !stderr.contains("panicked at"),
            "{stdout}\n{stderr}"
        );
        assert!(stdout.contains("injected empty read and replicated subsequent metadata"));
        return;
    }
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(10), async {
                let router = InProcessRouter::new();
                let store = EmptyReads::default();
                let mut inspect = store.inner.clone();
                let injected = store.injected.clone();
                let remaining = store.remaining.clone();
                remaining.store(3, Ordering::SeqCst);
                let owner = RaftMetadataNode::start_with_storage(
                    1,
                    default_raft_config().unwrap(),
                    &router,
                    store,
                    GanglionStateMachine::default(),
                )
                .await
                .unwrap();
                let follower = RaftMetadataNode::start(2, default_raft_config().unwrap(), &router)
                    .await
                    .unwrap();
                owner
                    .initialize(BTreeMap::from([(1, openraft::BasicNode::new("one"))]))
                    .await
                    .unwrap();
                owner
                    .wait_for_any_leader(Duration::from_secs(5))
                    .await
                    .unwrap();
                owner
                    .add_learner(2, openraft::BasicNode::new("two"), true)
                    .await
                    .unwrap();
                // add_learner(blocking=true) permits the configured lag; wait
                // for actual application before arming the next fault schedule.
                owner
                    .write_snapshot(CoordinationSnapshot {
                        generation: 41,
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                while follower.committed_snapshot().generation != 41 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                assert_eq!(injected.load(Ordering::SeqCst), 3);
                // Repeat after an established prefix, so prev_log_id is Some.
                remaining.store(3, Ordering::SeqCst);
                owner
                    .write_snapshot(CoordinationSnapshot {
                        generation: 42,
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                while follower.committed_snapshot().generation != 42 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                assert_eq!(injected.load(Ordering::SeqCst), 6);
                // A persistently unreadable range must neither acknowledge its
                // payload nor prevent cancellation of the replication stream.
                remaining.store(usize::MAX, Ordering::SeqCst);
                owner
                    .write_snapshot(CoordinationSnapshot {
                        generation: 43,
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                while injected.load(Ordering::SeqCst) < 9 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                assert_eq!(follower.committed_snapshot().generation, 42);
                // A higher-term leader can retire this stream while the read
                // stays empty. It must stop retrying even before node shutdown.
                // This single-voter fixture would otherwise immediately reelect
                // itself; hold it in the new follower role for this observation.
                owner.raft().runtime_config().elect(false);
                let term = inspect.read_vote().await.unwrap().unwrap().leader_id().term + 1;
                let response = owner
                    .raft()
                    .append_entries(openraft::raft::AppendEntriesRequest {
                        vote: Vote::new_committed(term, 2),
                        prev_log_id: inspect.get_log_state().await.unwrap().last_log_id,
                        entries: vec![],
                        leader_commit: None,
                    })
                    .await
                    .unwrap();
                assert!(
                    matches!(response, openraft::raft::AppendEntriesResponse::Success),
                    "{response:?}"
                );
                tokio::time::sleep(Duration::from_millis(40)).await;
                let stopped_at = injected.load(Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(60)).await;
                assert_eq!(
                    injected.load(Ordering::SeqCst),
                    stopped_at,
                    "a retired leader must stop retrying unreadable old ranges"
                );
                tokio::time::timeout(Duration::from_secs(1), owner.shutdown())
                    .await
                    .unwrap()
                    .unwrap();
                follower.shutdown().await.unwrap();
                println!("injected empty read and replicated subsequent metadata");
            })
            .await
            .unwrap();
        });
}
