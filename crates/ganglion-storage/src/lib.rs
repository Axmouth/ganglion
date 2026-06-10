use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use ganglion_core::{CoordinationSnapshot, PartitionAssignment, ResourceIdentity};
use serde::{Deserialize, Serialize};

#[cfg(feature = "keratin")]
use futures::executor::block_on;
#[cfg(feature = "keratin")]
use keratin_log::{KDurability, Keratin, KeratinConfig, KeratinReplicaExt, Message};

/// Record stored by durable metadata logs.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct MetadataLogEntry {
    pub term: u64,
    pub index: u64,
    pub snapshot: CoordinationSnapshot,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct PersistedMetadataLogEntry {
    term: u64,
    index: u64,
    snapshot: PersistedCoordinationSnapshot,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
struct PersistedCoordinationSnapshot {
    pub nodes: BTreeMap<String, ganglion_core::NodeInfo>,
    pub assignments: BTreeMap<String, PartitionAssignment>,
    pub generation: u64,
}

/// Error surface for persistent logs.
#[derive(Debug, Clone)]
pub enum MetadataLogError {
    Io(String),
    Parse(String),
}

impl fmt::Display for MetadataLogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(details) => write!(f, "metadata log I/O error: {details}"),
            Self::Parse(details) => write!(f, "metadata log parse error: {details}"),
        }
    }
}

impl std::error::Error for MetadataLogError {}

impl MetadataLogError {
    fn io(error: io::Error) -> Self {
        Self::Io(error.to_string())
    }

    fn parse_with_line(error: serde_json::Error, line: usize) -> Self {
        Self::Parse(format!("line {line}: {error}"))
    }

    fn parse(error: serde_json::Error) -> Self {
        Self::Parse(error.to_string())
    }
}

fn encode_snapshot(
    snapshot: &CoordinationSnapshot,
) -> Result<PersistedCoordinationSnapshot, MetadataLogError> {
    let assignments = snapshot
        .assignments
        .iter()
        .map(|(resource, assignment)| {
            let key = serde_json::to_string(resource).map_err(MetadataLogError::parse)?;
            Ok((key, assignment.clone()))
        })
        .collect::<Result<BTreeMap<_, _>, MetadataLogError>>()?;

    Ok(PersistedCoordinationSnapshot {
        nodes: snapshot.nodes.clone(),
        assignments,
        generation: snapshot.generation,
    })
}

fn decode_snapshot(
    snapshot: PersistedCoordinationSnapshot,
) -> Result<CoordinationSnapshot, MetadataLogError> {
    let assignments = snapshot
        .assignments
        .into_iter()
        .map(|(raw_resource, assignment)| {
            let resource = serde_json::from_str::<ResourceIdentity>(&raw_resource)
                .map_err(MetadataLogError::parse)?;
            Ok((resource, assignment))
        })
        .collect::<Result<BTreeMap<_, _>, MetadataLogError>>()?;

    Ok(CoordinationSnapshot {
        nodes: snapshot.nodes,
        assignments,
        generation: snapshot.generation,
    })
}

/// A persistence abstraction shared by openraft-backed and memory nodes.
pub trait MetadataLog: Send + Sync + fmt::Debug {
    fn append_entry(
        &self,
        term: u64,
        snapshot: CoordinationSnapshot,
    ) -> Result<MetadataLogEntry, MetadataLogError>;
    fn latest_entry(&self) -> Result<Option<MetadataLogEntry>, MetadataLogError>;
    fn entries(&self) -> Result<Vec<MetadataLogEntry>, MetadataLogError>;
    fn clear(&self) -> Result<(), MetadataLogError>;
    fn truncate_from(&self, first_index: u64) -> Result<(), MetadataLogError>;
}

/// Replay policy for file-backed metadata logs.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum FileMetadataReplayPolicy {
    /// Fail hard on malformed or non-sequential records.
    Strict,
    /// Discard a bounded tail of unparsable/unordered lines and recover the earlier prefix.
    ///
    /// Recovery is only allowed when the number of trailing non-blank lines that cannot be
    /// parsed into a consistent log does not exceed `max_tail_lines`.
    TruncateTail { max_tail_lines: usize },
}

impl Default for FileMetadataReplayPolicy {
    fn default() -> Self {
        Self::Strict
    }
}

/// In-memory metadata log used for tests and non-durable adapters.
#[derive(Debug, Default)]
pub struct InMemoryMetadataLog {
    entries: RwLock<Vec<MetadataLogEntry>>,
}

impl InMemoryMetadataLog {
    pub fn new() -> Self {
        Self::default()
    }
}

impl MetadataLog for InMemoryMetadataLog {
    fn append_entry(
        &self,
        term: u64,
        snapshot: CoordinationSnapshot,
    ) -> Result<MetadataLogEntry, MetadataLogError> {
        let mut entries = self
            .entries
            .write()
            .map_err(|_| MetadataLogError::Io("metadata lock poisoned".to_string()))?;
        let index = entries
            .last()
            .map_or(1, |entry| entry.index.saturating_add(1));
        let entry = MetadataLogEntry {
            term,
            index,
            snapshot,
        };
        entries.push(entry.clone());
        Ok(entry)
    }

    fn latest_entry(&self) -> Result<Option<MetadataLogEntry>, MetadataLogError> {
        let entries = self
            .entries
            .read()
            .map_err(|_| MetadataLogError::Io("metadata lock poisoned".to_string()))?;
        Ok(entries.last().cloned())
    }

    fn entries(&self) -> Result<Vec<MetadataLogEntry>, MetadataLogError> {
        let entries = self
            .entries
            .read()
            .map_err(|_| MetadataLogError::Io("metadata lock poisoned".to_string()))?;
        Ok(entries.clone())
    }

    fn clear(&self) -> Result<(), MetadataLogError> {
        let mut entries = self
            .entries
            .write()
            .map_err(|_| MetadataLogError::Io("metadata lock poisoned".to_string()))?;
        entries.clear();
        Ok(())
    }

    fn truncate_from(&self, first_index: u64) -> Result<(), MetadataLogError> {
        let mut entries = self
            .entries
            .write()
            .map_err(|_| MetadataLogError::Io("metadata lock poisoned".to_string()))?;
        let mut first = first_index;
        if first == 0 {
            first = 1;
        }
        entries.retain(|entry| entry.index >= first);
        Ok(())
    }
}

/// Append-only file log. Entries are newline-delimited JSON values.
#[derive(Debug)]
pub struct FileMetadataLog {
    path: PathBuf,
    guard: Arc<RwLock<()>>,
    replay_policy: FileMetadataReplayPolicy,
}

impl FileMetadataLog {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            guard: Arc::new(RwLock::new(())),
            replay_policy: FileMetadataReplayPolicy::default(),
        }
    }

    pub fn with_replay_policy(
        path: impl Into<PathBuf>,
        replay_policy: FileMetadataReplayPolicy,
    ) -> Self {
        Self {
            path: path.into(),
            guard: Arc::new(RwLock::new(())),
            replay_policy,
        }
    }

    fn read_all_entries(&self) -> Result<Vec<MetadataLogEntry>, MetadataLogError> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(MetadataLogError::io(error)),
        };

        let reader = BufReader::new(file);
        let raw_lines = reader
            .lines()
            .collect::<Result<Vec<_>, _>>()
            .map_err(MetadataLogError::io)?;
        let mut entries = Vec::new();

        for (line_offset, line) in raw_lines.iter().enumerate() {
            let cleaned = line.trim();
            if cleaned.is_empty() || cleaned.starts_with('#') {
                continue;
            }

            let line_no = line_offset + 1;
            let persisted = match serde_json::from_str::<PersistedMetadataLogEntry>(cleaned) {
                Ok(entry) => entry,
                Err(error) => {
                    let remaining_tail_lines = raw_lines
                        .iter()
                        .skip(line_offset + 1)
                        .filter(|candidate| {
                            let cleaned = candidate.trim();
                            !cleaned.is_empty() && !cleaned.starts_with('#')
                        })
                        .count()
                        + 1;

                    return self.recoverable_tail_error(
                        MetadataLogError::parse_with_line(error, line_no),
                        remaining_tail_lines,
                        &entries,
                    );
                }
            };

            let expected_index = entries
                .last()
                .map_or(persisted.index, |entry: &MetadataLogEntry| entry.index + 1);
            if persisted.index == 0 {
                let error = MetadataLogError::Parse(format!(
                    "line {line_no}: metadata log index must be >= 1"
                ));
                return self.recoverable_tail_error(
                    error,
                    raw_lines
                        .iter()
                        .skip(line_offset + 1)
                        .filter(|candidate| {
                            let cleaned = candidate.trim();
                            !cleaned.is_empty() && !cleaned.starts_with('#')
                        })
                        .count()
                        + 1,
                    &entries,
                );
            }
            if persisted.index != expected_index {
                let error = MetadataLogError::Parse(format!(
                    "line {line_no}: non-sequential log index; expected {expected_index}, got {}",
                    persisted.index
                ));
                return self.recoverable_tail_error(
                    error,
                    raw_lines
                        .iter()
                        .skip(line_offset + 1)
                        .filter(|candidate| {
                            let cleaned = candidate.trim();
                            !cleaned.is_empty() && !cleaned.starts_with('#')
                        })
                        .count()
                        + 1,
                    &entries,
                );
            }

            let snapshot = decode_snapshot(persisted.snapshot)?;
            entries.push(MetadataLogEntry {
                term: persisted.term,
                index: persisted.index,
                snapshot,
            });
        }

        Ok(entries)
    }

    fn recoverable_tail_error(
        &self,
        error: MetadataLogError,
        skipped_tail_lines: usize,
        entries: &[MetadataLogEntry],
    ) -> Result<Vec<MetadataLogEntry>, MetadataLogError> {
        match self.replay_policy {
            FileMetadataReplayPolicy::Strict => Err(error),
            FileMetadataReplayPolicy::TruncateTail { max_tail_lines } => {
                if skipped_tail_lines <= max_tail_lines {
                    Ok(entries.to_vec())
                } else {
                    Err(error)
                }
            }
        }
    }

    fn write_entries(
        &self,
        entries: impl IntoIterator<Item = MetadataLogEntry>,
    ) -> Result<(), MetadataLogError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(MetadataLogError::io)?;
        }

        let mut file = File::create(&self.path).map_err(MetadataLogError::io)?;
        for entry in entries {
            let payload = serde_json::to_string(&PersistedMetadataLogEntry {
                term: entry.term,
                index: entry.index,
                snapshot: encode_snapshot(&entry.snapshot)?,
            })
            .map_err(MetadataLogError::parse)?;
            writeln!(file, "{payload}").map_err(MetadataLogError::io)?;
        }

        Ok(())
    }
}

impl MetadataLog for FileMetadataLog {
    fn append_entry(
        &self,
        term: u64,
        snapshot: CoordinationSnapshot,
    ) -> Result<MetadataLogEntry, MetadataLogError> {
        let _guard = self
            .guard
            .write()
            .map_err(|_| MetadataLogError::Io("metadata lock poisoned".to_string()))?;

        let mut entries = self.read_all_entries()?;
        let index = entries
            .last()
            .map_or(1, |entry| entry.index.saturating_add(1));
        let entry = MetadataLogEntry {
            term,
            index,
            snapshot,
        };
        entries.push(entry.clone());

        self.write_entries(entries.into_iter())?;
        Ok(entry)
    }

    fn latest_entry(&self) -> Result<Option<MetadataLogEntry>, MetadataLogError> {
        let _guard = self
            .guard
            .read()
            .map_err(|_| MetadataLogError::Io("metadata lock poisoned".to_string()))?;
        let entries = self.read_all_entries()?;
        Ok(entries.into_iter().last())
    }

    fn entries(&self) -> Result<Vec<MetadataLogEntry>, MetadataLogError> {
        let _guard = self
            .guard
            .read()
            .map_err(|_| MetadataLogError::Io("metadata lock poisoned".to_string()))?;
        self.read_all_entries()
    }

    fn clear(&self) -> Result<(), MetadataLogError> {
        let _guard = self
            .guard
            .write()
            .map_err(|_| MetadataLogError::Io("metadata lock poisoned".to_string()))?;
        self.write_entries(std::iter::empty())
    }

    fn truncate_from(&self, first_index: u64) -> Result<(), MetadataLogError> {
        let _guard = self
            .guard
            .write()
            .map_err(|_| MetadataLogError::Io("metadata lock poisoned".to_string()))?;
        let first_index = if first_index == 0 { 1 } else { first_index };
        let entries = self
            .read_all_entries()?
            .into_iter()
            .filter(|entry| entry.index >= first_index)
            .collect::<Vec<_>>();
        self.write_entries(entries)
    }
}

#[cfg(feature = "keratin")]
#[derive(Debug)]
pub struct KeratinMetadataLog {
    keratin: Keratin,
    guard: Arc<RwLock<()>>,
    replay_policy: FileMetadataReplayPolicy,
}

#[cfg(feature = "keratin")]
impl KeratinMetadataLog {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self, MetadataLogError> {
        Self::with_replay_policy(path, FileMetadataReplayPolicy::default())
    }

    pub fn with_replay_policy(
        path: impl Into<PathBuf>,
        replay_policy: FileMetadataReplayPolicy,
    ) -> Result<Self, MetadataLogError> {
        let root = path.into();
        let keratin = block_on(Keratin::open(&root, KeratinConfig::default()))
            .map_err(MetadataLogError::io)?;

        Ok(Self {
            keratin,
            guard: Arc::new(RwLock::new(())),
            replay_policy,
        })
    }

    fn run_async<T, E>(
        &self,
        future: impl std::future::Future<Output = Result<T, E>>,
    ) -> Result<T, MetadataLogError>
    where
        E: std::fmt::Display,
    {
        let result = block_on(future);
        result.map_err(|error| MetadataLogError::Io(error.to_string()))
    }

    fn recoverable_tail_error(
        &self,
        error: MetadataLogError,
        skipped_tail_records: usize,
        entries: &[MetadataLogEntry],
    ) -> Result<Vec<MetadataLogEntry>, MetadataLogError> {
        match self.replay_policy {
            FileMetadataReplayPolicy::Strict => Err(error),
            FileMetadataReplayPolicy::TruncateTail { max_tail_lines } => {
                if skipped_tail_records <= max_tail_lines {
                    Ok(entries.to_vec())
                } else {
                    Err(error)
                }
            }
        }
    }

    fn read_all_entries(&self) -> Result<Vec<MetadataLogEntry>, MetadataLogError> {
        let reader = self.keratin.reader();
        let mut records: Vec<keratin_log::OwnedRecord> = Vec::new();
        let mut cursor = 0u64;

        loop {
            let batch = reader
                .scan_from(cursor, 256)
                .map_err(MetadataLogError::io)?;
            if batch.is_empty() {
                break;
            }

            cursor = match batch.last() {
                Some(last) => last.offset.saturating_add(1),
                None => break,
            };
            records.extend(batch);
        }

        let mut entries = Vec::new();
        for (record_offset, record) in records.iter().enumerate() {
            let persisted =
                match serde_json::from_slice::<PersistedMetadataLogEntry>(&record.payload) {
                    Ok(persisted) => persisted,
                    Err(error) => {
                        let skipped_tail_records = records.len().saturating_sub(record_offset);
                        return self.recoverable_tail_error(
                            MetadataLogError::Parse(format!(
                                "record at offset {}: {error}",
                                record.offset
                            )),
                            skipped_tail_records,
                            &entries,
                        );
                    }
                };

            if persisted.index == 0 {
                let skipped_tail_records = records.len().saturating_sub(record_offset);
                return self.recoverable_tail_error(
                    MetadataLogError::Parse(format!(
                        "record at offset {}: metadata log index must be >= 1",
                        record.offset
                    )),
                    skipped_tail_records,
                    &entries,
                );
            }
            let expected_index = entries
                .last()
                .map_or(persisted.index, |entry: &MetadataLogEntry| entry.index + 1);
            if persisted.index != expected_index {
                let skipped_tail_records = records.len().saturating_sub(record_offset);
                return self.recoverable_tail_error(
                    MetadataLogError::Parse(format!(
                        "record at offset {}: non-sequential log index; expected {expected_index}, got {}",
                        record.offset,
                        persisted.index
                    )),
                    skipped_tail_records,
                    &entries,
                );
            }

            let snapshot = decode_snapshot(persisted.snapshot)?;
            entries.push(MetadataLogEntry {
                term: persisted.term,
                index: persisted.index,
                snapshot,
            });
        }

        Ok(entries)
    }

    fn switch_to_follower_and_reset(&self, next_offset: u64) -> Result<(), MetadataLogError> {
        use keratin_log::KeratinRole;

        let previous_role = self.keratin.role();
        self.keratin.become_follower();
        let result = self
            .run_async(self.keratin.destructive_reset_to_checkpoint(next_offset))
            .map(|_| ());

        match previous_role {
            KeratinRole::Owner => self.keratin.become_owner(),
            KeratinRole::Follower => self.keratin.become_follower(),
            KeratinRole::Frozen => self.keratin.freeze(),
        };

        result
    }

    fn append_raw_entry(
        &self,
        term: u64,
        index: u64,
        snapshot: CoordinationSnapshot,
    ) -> Result<MetadataLogEntry, MetadataLogError> {
        let payload = serde_json::to_vec(&PersistedMetadataLogEntry {
            term,
            index,
            snapshot: encode_snapshot(&snapshot)?,
        })
        .map_err(MetadataLogError::parse)?;

        let message = Message {
            flags: 0,
            headers: Vec::new(),
            payload,
        };

        self.run_async(self.keratin.append(message, Some(KDurability::AfterFsync)))?;

        Ok(MetadataLogEntry {
            term,
            index,
            snapshot,
        })
    }
}

#[cfg(feature = "keratin")]
impl MetadataLog for KeratinMetadataLog {
    fn append_entry(
        &self,
        term: u64,
        snapshot: CoordinationSnapshot,
    ) -> Result<MetadataLogEntry, MetadataLogError> {
        let _guard = self
            .guard
            .write()
            .map_err(|_| MetadataLogError::Io("metadata lock poisoned".to_string()))?;

        let next_index = self
            .read_all_entries()?
            .into_iter()
            .next_back()
            .map_or(1, |entry| entry.index.saturating_add(1));
        self.append_raw_entry(term, next_index, snapshot)
    }

    fn latest_entry(&self) -> Result<Option<MetadataLogEntry>, MetadataLogError> {
        let _guard = self
            .guard
            .read()
            .map_err(|_| MetadataLogError::Io("metadata lock poisoned".to_string()))?;
        Ok(self.read_all_entries()?.into_iter().last())
    }

    fn entries(&self) -> Result<Vec<MetadataLogEntry>, MetadataLogError> {
        let _guard = self
            .guard
            .read()
            .map_err(|_| MetadataLogError::Io("metadata lock poisoned".to_string()))?;
        self.read_all_entries()
    }

    fn clear(&self) -> Result<(), MetadataLogError> {
        let _guard = self
            .guard
            .write()
            .map_err(|_| MetadataLogError::Io("metadata lock poisoned".to_string()))?;
        self.switch_to_follower_and_reset(0)
    }

    fn truncate_from(&self, first_index: u64) -> Result<(), MetadataLogError> {
        let _guard = self
            .guard
            .write()
            .map_err(|_| MetadataLogError::Io("metadata lock poisoned".to_string()))?;
        let first_index = if first_index == 0 { 1 } else { first_index };
        let replay = self
            .read_all_entries()?
            .into_iter()
            .filter(|entry| entry.index >= first_index)
            .collect::<Vec<_>>();
        self.switch_to_follower_and_reset(0)?;
        for entry in &replay {
            let _ = self.append_raw_entry(entry.term, entry.index, entry.snapshot.clone())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_temp_path(tag: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or_else(|_| std::time::Duration::from_nanos(0), |duration| duration);
        path.push(format!(
            "ganglion-storage-{tag}-{}-{}.log",
            std::process::id(),
            nanos.as_nanos()
        ));
        path
    }

    #[cfg(feature = "keratin")]
    fn unique_temp_dir_path(tag: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or_else(|_| std::time::Duration::from_nanos(0), |duration| duration);
        path.push(format!(
            "ganglion-storage-keratin-{tag}-{}-{}",
            std::process::id(),
            nanos.as_nanos()
        ));
        path
    }

    fn sample_snapshot() -> CoordinationSnapshot {
        CoordinationSnapshot {
            generation: 1,
            ..Default::default()
        }
    }

    fn write_raw_entry(
        path: &std::path::Path,
        entry: PersistedMetadataLogEntry,
    ) -> std::io::Result<()> {
        let mut file = File::options().create(true).append(true).open(path)?;
        let payload = serde_json::to_string(&entry)?;
        writeln!(file, "{payload}")?;
        Ok(())
    }

    #[test]
    fn file_metadata_log_roundtrips_append_and_replay() {
        let path = unique_temp_path("roundtrip");
        let log = FileMetadataLog::new(path.clone());

        let _first = log
            .append_entry(
                1,
                CoordinationSnapshot {
                    generation: 10,
                    ..Default::default()
                },
            )
            .expect("append first entry");

        let _second = log
            .append_entry(1, sample_snapshot())
            .expect("append second entry");

        let entries = log.entries().expect("entries should load");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].index, 1);
        assert_eq!(entries[1].index, 2);
        assert_eq!(entries[1].snapshot.generation, 1);
        assert_eq!(log.latest_entry().expect("latest entry").unwrap().index, 2);

        let reopened = FileMetadataLog::new(path);
        let reopened_entries = reopened.entries().expect("reopen should load");
        assert_eq!(reopened_entries.len(), 2);
        assert_eq!(reopened_entries[1].index, 2);
    }

    #[test]
    fn file_metadata_log_supports_blank_and_comment_lines() {
        let path = unique_temp_path("comments");
        write_raw_entry(
            &path,
            PersistedMetadataLogEntry {
                term: 1,
                index: 1,
                snapshot: encode_snapshot(&CoordinationSnapshot {
                    generation: 1,
                    ..Default::default()
                })
                .expect("encode snapshot"),
            },
        )
        .expect("write entry");

        {
            let mut file = File::options()
                .append(true)
                .open(&path)
                .expect("open for append");
            writeln!(file, "# comment line").expect("write comment");
            writeln!(file, "").expect("write blank line");
            writeln!(
                file,
                "{}",
                serde_json::to_string(&PersistedMetadataLogEntry {
                    term: 1,
                    index: 2,
                    snapshot: encode_snapshot(&CoordinationSnapshot {
                        generation: 2,
                        ..Default::default()
                    })
                    .expect("encode snapshot"),
                })
                .expect("encode entry")
            )
            .expect("write second");
        }

        let log = FileMetadataLog::new(path);
        let entries = log.entries().expect("commented log should parse");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].index, 1);
        assert_eq!(entries[1].index, 2);
        assert_eq!(entries[1].snapshot.generation, 2);
    }

    #[test]
    fn file_metadata_log_rejects_malformed_json() {
        let path = unique_temp_path("malformed-json");
        std::fs::write(&path, b"{not-json}\n").expect("write malformed payload");

        let log = FileMetadataLog::new(path);
        let err = log
            .entries()
            .expect_err("non-json should return parse error");
        assert!(matches!(err, MetadataLogError::Parse(_)));
    }

    #[test]
    fn file_metadata_log_rejects_nonsequential_indexes() {
        let path = unique_temp_path("bad-indexes");
        write_raw_entry(
            &path,
            PersistedMetadataLogEntry {
                term: 1,
                index: 1,
                snapshot: encode_snapshot(&CoordinationSnapshot {
                    generation: 1,
                    ..Default::default()
                })
                .expect("encode snapshot"),
            },
        )
        .expect("write first");
        write_raw_entry(
            &path,
            PersistedMetadataLogEntry {
                term: 1,
                index: 3,
                snapshot: encode_snapshot(&CoordinationSnapshot {
                    generation: 2,
                    ..Default::default()
                })
                .expect("encode snapshot"),
            },
        )
        .expect("write bad index");

        let log = FileMetadataLog::new(path);
        let err = log.entries().expect_err("gap should fail");
        assert!(matches!(err, MetadataLogError::Parse(_)));
    }

    #[test]
    fn file_metadata_log_rejects_zero_index() {
        let path = unique_temp_path("zero-index");
        write_raw_entry(
            &path,
            PersistedMetadataLogEntry {
                term: 1,
                index: 0,
                snapshot: encode_snapshot(&CoordinationSnapshot {
                    generation: 1,
                    ..Default::default()
                })
                .expect("encode snapshot"),
            },
        )
        .expect("write zero-index entry");

        let log = FileMetadataLog::new(path);
        let err = log.entries().expect_err("zero index should fail");
        assert!(matches!(err, MetadataLogError::Parse(_)));
    }

    #[test]
    fn file_metadata_log_truncates_small_tailing_corruption_tail() {
        let path = unique_temp_path("tail-corruption");
        write_raw_entry(
            &path,
            PersistedMetadataLogEntry {
                term: 1,
                index: 1,
                snapshot: encode_snapshot(&CoordinationSnapshot {
                    generation: 1,
                    ..Default::default()
                })
                .expect("encode snapshot"),
            },
        )
        .expect("write valid entry");
        write_raw_entry(
            &path,
            PersistedMetadataLogEntry {
                term: 1,
                index: 2,
                snapshot: encode_snapshot(&CoordinationSnapshot {
                    generation: 2,
                    ..Default::default()
                })
                .expect("encode snapshot"),
            },
        )
        .expect("write valid entry");
        {
            use std::io::Write as _;
            let mut file = File::options()
                .append(true)
                .open(&path)
                .expect("open for append");
            file.write_all(b"{not-json}\n")
                .expect("append malformed tail");
        }

        let log = FileMetadataLog::with_replay_policy(
            path,
            FileMetadataReplayPolicy::TruncateTail { max_tail_lines: 1 },
        );
        let entries = log
            .entries()
            .expect("truncation policy should recover consistent prefix");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].snapshot.generation, 2);
    }

    #[test]
    fn file_metadata_log_rejects_large_tailing_corruption_tail() {
        let path = unique_temp_path("tail-corruption-limit");
        write_raw_entry(
            &path,
            PersistedMetadataLogEntry {
                term: 1,
                index: 1,
                snapshot: encode_snapshot(&CoordinationSnapshot {
                    generation: 1,
                    ..Default::default()
                })
                .expect("encode snapshot"),
            },
        )
        .expect("write valid entry");
        let raw = b"{bad}\n{bad}\n{bad}\n";
        {
            use std::io::Write as _;
            let mut file = File::options()
                .append(true)
                .open(&path)
                .expect("open for append");
            file.write_all(raw).expect("append malformed lines");
        }

        let log = FileMetadataLog::with_replay_policy(
            path,
            FileMetadataReplayPolicy::TruncateTail { max_tail_lines: 1 },
        );
        let err = log
            .entries()
            .expect_err("too much trailing corruption should fail");
        assert!(matches!(err, MetadataLogError::Parse(_)));
    }

    #[cfg(feature = "keratin")]
    #[test]
    fn keratin_metadata_log_roundtrips_append_and_replay() {
        let root = unique_temp_dir_path("roundtrip");
        {
            let log = KeratinMetadataLog::new(&root).expect("keratin log should open");

            let first = log
                .append_entry(
                    1,
                    CoordinationSnapshot {
                        generation: 10,
                        ..Default::default()
                    },
                )
                .expect("append first entry");
            let second = log
                .append_entry(
                    1,
                    CoordinationSnapshot {
                        generation: 1,
                        ..Default::default()
                    },
                )
                .expect("append second entry");

            assert_eq!(first.index, 1);
            assert_eq!(second.index, 2);

            let entries = log.entries().expect("entries should load");
            assert_eq!(entries.len(), 2);
            assert_eq!(entries[1].index, 2);
        }

        let reopened = KeratinMetadataLog::new(&root).expect("keratin reopen should succeed");
        let reopened_entries = reopened.entries().expect("reopened entries should parse");
        assert_eq!(reopened_entries.len(), 2);
        assert_eq!(reopened_entries[0].index, 1);
        assert_eq!(reopened_entries[1].index, 2);
        assert_eq!(reopened_entries[1].snapshot.generation, 1);
    }

    #[cfg(feature = "keratin")]
    #[test]
    fn keratin_metadata_log_clear_and_truncate_from() {
        let root = unique_temp_dir_path("clear-truncate");
        let log = KeratinMetadataLog::new(&root).expect("keratin log should open");

        log.append_entry(
            1,
            CoordinationSnapshot {
                generation: 1,
                ..Default::default()
            },
        )
        .expect("append first");
        log.append_entry(
            1,
            CoordinationSnapshot {
                generation: 2,
                ..Default::default()
            },
        )
        .expect("append second");
        log.append_entry(
            1,
            CoordinationSnapshot {
                generation: 3,
                ..Default::default()
            },
        )
        .expect("append third");

        log.truncate_from(2)
            .expect("truncate_from should keep suffix");
        let kept = log.entries().expect("entries after truncate");
        assert_eq!(kept.len(), 2);
        assert_eq!(kept[0].index, 2);
        assert_eq!(kept[1].index, 3);

        log.clear().expect("clear should discard history");
        assert!(log.entries().expect("entries after clear").is_empty());
    }
}
