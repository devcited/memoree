//! Private, non-recallable staging for deliberate agent-authored checkpoints.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Duration, Utc};
use fs2::FileExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    error::{MemoryError, Result},
    protocol::MAX_CONTEXT_ID_BYTES,
};

pub const CHECKPOINT_SCHEMA_VERSION: u32 = 1;
pub const MAX_CHECKPOINT_BYTES: usize = 4 * 1024;
pub const MAX_CHECKPOINT_INPUT_BYTES: usize = 64 * 1024;
pub const CHECKPOINT_RETENTION_DAYS: i64 = 14;
const TRUNCATION_MARKER: &str = "\n… [checkpoint truncated]";

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PendingCheckpoint {
    pub schema_version: u32,
    pub checkpoint_id: String,
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<String>,
    pub text: String,
    pub original_bytes: usize,
    pub stored_bytes: usize,
    pub truncated: bool,
    #[serde(default)]
    pub sensitive_flags: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct PendingCheckpointSummary {
    pub checkpoint_id: String,
    pub session_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task: Option<String>,
    pub original_bytes: usize,
    pub stored_bytes: usize,
    pub truncated: bool,
    #[serde(default)]
    pub sensitive_flags: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub expired: bool,
}

impl PendingCheckpoint {
    pub fn summary(&self, now: DateTime<Utc>) -> PendingCheckpointSummary {
        let expires_at = self.updated_at + Duration::days(CHECKPOINT_RETENTION_DAYS);
        PendingCheckpointSummary {
            checkpoint_id: self.checkpoint_id.clone(),
            session_id: self.session_id.clone(),
            task: self.task.clone(),
            original_bytes: self.original_bytes,
            stored_bytes: self.stored_bytes,
            truncated: self.truncated,
            sensitive_flags: self.sensitive_flags.clone(),
            created_at: self.created_at,
            updated_at: self.updated_at,
            expires_at,
            expired: now >= expires_at,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CheckpointStore {
    directory: PathBuf,
}

impl CheckpointStore {
    pub fn new(data_dir: &Path) -> Self {
        Self {
            directory: data_dir.join("pending-checkpoints"),
        }
    }

    pub fn put(
        &self,
        session_id: &str,
        task: Option<&str>,
        text: &str,
    ) -> Result<PendingCheckpoint> {
        validate_id("session", session_id)?;
        if let Some(task) = task {
            validate_id("task", task)?;
        }
        if text.trim().is_empty() {
            return Err(MemoryError::InvalidRequest(
                "checkpoint text must not be empty".into(),
            ));
        }
        if text.len() > MAX_CHECKPOINT_INPUT_BYTES {
            return Err(MemoryError::ContentTooLarge);
        }
        self.ensure_private_directory()?;
        let lock_file = self.lock_file()?;
        lock_file.lock_exclusive()?;

        let checkpoint_id = checkpoint_id(session_id);
        let destination = self.path(&checkpoint_id);
        let existing = destination
            .exists()
            .then(|| read_checkpoint(&destination))
            .transpose()?;
        let now = Utc::now();
        let (stored, truncated) = truncate_checkpoint(text);
        let checkpoint = PendingCheckpoint {
            schema_version: CHECKPOINT_SCHEMA_VERSION,
            checkpoint_id,
            session_id: session_id.into(),
            task: task.map(str::to_owned),
            text: stored,
            original_bytes: text.len(),
            stored_bytes: 0,
            truncated,
            sensitive_flags: sensitive_flags(text),
            created_at: existing
                .as_ref()
                .map_or(now, |checkpoint| checkpoint.created_at),
            updated_at: now,
        };
        let checkpoint = PendingCheckpoint {
            stored_bytes: checkpoint.text.len(),
            ..checkpoint
        };
        write_checkpoint_atomic(&self.directory, &destination, &checkpoint)?;
        lock_file.unlock()?;
        Ok(checkpoint)
    }

    pub fn get(&self, checkpoint_or_session_id: &str) -> Result<PendingCheckpoint> {
        validate_id("checkpoint or session", checkpoint_or_session_id)?;
        let checkpoint_id = if checkpoint_or_session_id.starts_with("cp_") {
            checkpoint_or_session_id.to_owned()
        } else {
            checkpoint_id(checkpoint_or_session_id)
        };
        let path = self.path(&checkpoint_id);
        if !path.exists() {
            return Err(MemoryError::NotFound(format!(
                "pending checkpoint {checkpoint_or_session_id}"
            )));
        }
        read_checkpoint(&path)
    }

    pub fn list(&self, include_expired: bool) -> Result<Vec<PendingCheckpointSummary>> {
        if !self.directory.exists() {
            return Ok(vec![]);
        }
        let now = Utc::now();
        let mut checkpoints = Vec::new();
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            let summary = read_checkpoint(&path)?.summary(now);
            if include_expired || !summary.expired {
                checkpoints.push(summary);
            }
        }
        checkpoints.sort_by(|left, right| {
            right
                .updated_at
                .cmp(&left.updated_at)
                .then_with(|| left.checkpoint_id.cmp(&right.checkpoint_id))
        });
        Ok(checkpoints)
    }

    pub fn drop_checkpoint(&self, checkpoint_or_session_id: &str) -> Result<PendingCheckpoint> {
        let checkpoint = self.get(checkpoint_or_session_id)?;
        let lock_file = self.lock_file()?;
        lock_file.lock_exclusive()?;
        fs::remove_file(self.path(&checkpoint.checkpoint_id))?;
        lock_file.unlock()?;
        Ok(checkpoint)
    }

    fn ensure_private_directory(&self) -> Result<()> {
        fs::create_dir_all(&self.directory)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Some(data_dir) = self.directory.parent() {
                fs::set_permissions(data_dir, fs::Permissions::from_mode(0o700))?;
            }
            fs::set_permissions(&self.directory, fs::Permissions::from_mode(0o700))?;
        }
        Ok(())
    }

    fn lock_file(&self) -> Result<std::fs::File> {
        self.ensure_private_directory()?;
        Ok(OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(self.directory.join(".lock"))?)
    }

    fn path(&self, checkpoint_id: &str) -> PathBuf {
        self.directory.join(format!("{checkpoint_id}.json"))
    }
}

fn checkpoint_id(session_id: &str) -> String {
    let digest = blake3::hash(session_id.as_bytes()).to_hex().to_string();
    format!("cp_{}", &digest[..26])
}

fn validate_id(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > MAX_CONTEXT_ID_BYTES {
        return Err(MemoryError::InvalidRequest(format!(
            "{field} id must contain 1..={MAX_CONTEXT_ID_BYTES} bytes"
        )));
    }
    Ok(())
}

fn truncate_checkpoint(text: &str) -> (String, bool) {
    if text.len() <= MAX_CHECKPOINT_BYTES {
        return (text.to_owned(), false);
    }
    let budget = MAX_CHECKPOINT_BYTES - TRUNCATION_MARKER.len();
    let mut end = budget;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    (format!("{}{TRUNCATION_MARKER}", &text[..end]), true)
}

fn sensitive_flags(text: &str) -> Vec<String> {
    let lower = text.to_ascii_lowercase();
    let mut flags = Vec::new();
    if contains_aws_access_key(text) {
        flags.push("aws_access_key".into());
    }
    if lower.contains("-----begin private key-----")
        || lower.contains("-----begin rsa private key-----")
        || lower.contains("-----begin openssh private key-----")
    {
        flags.push("private_key_block".into());
    }
    if lower.contains("authorization: bearer ") || lower.contains("bearer eyj") {
        flags.push("bearer_token".into());
    }
    if [
        "openai_api_key=",
        "anthropic_api_key=",
        "aws_secret_access_key=",
        "github_token=",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
    {
        flags.push("credential_assignment".into());
    }
    flags
}

fn contains_aws_access_key(text: &str) -> bool {
    let bytes = text.as_bytes();
    bytes.windows(20).any(|window| {
        matches!(&window[..4], b"AKIA" | b"ASIA")
            && window[4..]
                .iter()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit())
    })
}

fn read_checkpoint(path: &Path) -> Result<PendingCheckpoint> {
    let checkpoint: PendingCheckpoint = serde_json::from_slice(&fs::read(path)?)?;
    if checkpoint.schema_version != CHECKPOINT_SCHEMA_VERSION {
        return Err(MemoryError::Integrity(format!(
            "unsupported pending checkpoint schema {} in {}",
            checkpoint.schema_version,
            path.display()
        )));
    }
    Ok(checkpoint)
}

fn write_checkpoint_atomic(
    directory: &Path,
    destination: &Path,
    checkpoint: &PendingCheckpoint,
) -> Result<()> {
    let mut temporary = tempfile::NamedTempFile::new_in(directory)?;
    temporary.write_all(&serde_json::to_vec(checkpoint)?)?;
    temporary.as_file_mut().sync_all()?;
    temporary
        .persist(destination)
        .map_err(|error| MemoryError::Io(error.error))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_is_bounded_last_write_wins_and_private_from_memory_store() {
        let temporary = tempfile::tempdir().unwrap();
        let store = CheckpointStore::new(temporary.path());
        let first = store
            .put("session-1", Some("task"), &"🦀".repeat(2_000))
            .unwrap();
        assert!(first.truncated);
        assert!(first.stored_bytes <= MAX_CHECKPOINT_BYTES);
        assert!(first.text.ends_with(TRUNCATION_MARKER));

        let second = store.put("session-1", Some("task"), "later note").unwrap();
        assert_eq!(first.checkpoint_id, second.checkpoint_id);
        assert_eq!(store.list(false).unwrap().len(), 1);
        assert_eq!(store.get("session-1").unwrap().text, "later note");
        assert!(!temporary.path().join("memoree.sqlite3").exists());
    }

    #[test]
    fn checkpoint_flags_common_secret_shapes_before_promotion() {
        let temporary = tempfile::tempdir().unwrap();
        let store = CheckpointStore::new(temporary.path());
        let checkpoint = store
            .put(
                "session-secret",
                None,
                "Authorization: Bearer eyJabc and AKIAABCDEFGHIJKLMNOP",
            )
            .unwrap();
        assert_eq!(
            checkpoint.sensitive_flags,
            vec!["aws_access_key", "bearer_token"]
        );
    }
}
