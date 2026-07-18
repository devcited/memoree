//! SQLite authority and synchronous FTS5 retrieval.
//!
//! Logical entities have mutable heads, but their revision rows are immutable.
//! The FTS tables are derived projections populated in the same transaction as
//! each revision, which provides read-your-writes without an index worker.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use rusqlite::{Connection, OptionalExtension, Row, Transaction, TransactionBehavior, params};
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use ulid::Ulid;

use crate::cas::{BlobRef, Cas};
use crate::error::{MemoryError, Result};
use crate::protocol::{
    AmbientContext, ArtifactContent, ArtifactForgetInput, ArtifactGetInput, ArtifactHistoryInput,
    ArtifactPutInput, ArtifactReviseInput, ClaimAssertInput, ClaimGetInput, ClaimHistoryInput,
    ClaimRetractInput, ClaimReviseInput, ClaimStatus, ClaimType, ConflictListInput, ConflictState,
    EntityType, EvidenceLocator, Horizon, MAX_ARTIFACT_BYTES, MAX_CLAIM_STATEMENT_BYTES,
    MAX_CONFLICT_LIST_ITEMS, MAX_CONTEXT_ID_BYTES, MAX_CONTEXT_PINS, MAX_ENCODED_CONTENT_BYTES,
    MAX_EVIDENCE_ITEMS, MAX_HISTORY_ITEMS, MAX_METADATA_BYTES, MAX_PIN_BYTES, MAX_QUERY_BYTES,
    MAX_RELATION_LIST_ITEMS, MAX_SEARCH_ITEMS, MAX_TITLE_BYTES, RecencyDecayClass,
    RecencyTimestampBasis, RelationListInput, RelationListItem, RelationListResult,
    RelationPutInput, RelationType, SearchHit, SearchInput, SearchRanking, SearchResult,
};

const SCHEMA_VERSION: i64 = 3;
pub const MEMOREE_DATABASE_FILE: &str = "memoree.sqlite3";
const MAX_KIND_BYTES: usize = 128;
const MAX_MEDIA_TYPE_BYTES: usize = 512;
const MAX_ACTOR_BYTES: usize = 1024;
const MAX_REASON_BYTES: usize = 64 * 1024;
const MAX_RELATION_LIST_ENCODED_BYTES: usize = 8 * 1024 * 1024;
const MAX_CONFLICT_LIST_ENCODED_BYTES: usize = 12 * 1024 * 1024;
const RECENCY_POLICY_VERSION: &str = "bounded_recency_v1";
const RECENCY_MAX_PROMOTION: usize = 2;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;
INSERT OR IGNORE INTO meta(key, value) VALUES
    ('schema_version', '3'),
    ('commit_seq', '0');

CREATE TABLE IF NOT EXISTS artifacts (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    task_id TEXT,
    component TEXT,
    kind TEXT NOT NULL,
    current_revision_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('active', 'forgotten')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    forgotten_reason TEXT,
    FOREIGN KEY(current_revision_id) REFERENCES artifact_revisions(id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE IF NOT EXISTS artifact_revisions (
    id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    revision_number INTEGER NOT NULL CHECK(revision_number > 0),
    title TEXT NOT NULL,
    media_type TEXT NOT NULL,
    blob_hash TEXT NOT NULL,
    blob_size INTEGER NOT NULL CHECK(blob_size >= 0),
    inline_blob BLOB,
    search_text TEXT NOT NULL,
    provenance_json TEXT NOT NULL,
    actor TEXT,
    created_at TEXT NOT NULL,
    commit_seq INTEGER NOT NULL UNIQUE,
    UNIQUE(artifact_id, revision_number)
) STRICT;

CREATE TABLE IF NOT EXISTS claims (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    task_id TEXT,
    component TEXT,
    claim_type TEXT NOT NULL,
    current_revision_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('active', 'superseded', 'retracted', 'conflicted')),
    valid_from TEXT,
    valid_until TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    retraction_reason TEXT,
    FOREIGN KEY(current_revision_id) REFERENCES claim_revisions(id)
        DEFERRABLE INITIALLY DEFERRED
) STRICT;

CREATE TABLE IF NOT EXISTS claim_revisions (
    id TEXT PRIMARY KEY,
    claim_id TEXT NOT NULL REFERENCES claims(id) ON DELETE RESTRICT
        DEFERRABLE INITIALLY DEFERRED,
    revision_number INTEGER NOT NULL CHECK(revision_number > 0),
    statement TEXT NOT NULL,
    confidence REAL CHECK(confidence IS NULL OR (confidence >= 0.0 AND confidence <= 1.0)),
    evidence_json TEXT NOT NULL,
    actor TEXT,
    created_at TEXT NOT NULL,
    commit_seq INTEGER NOT NULL UNIQUE,
    UNIQUE(claim_id, revision_number)
) STRICT;

CREATE TABLE IF NOT EXISTS relations (
    id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL CHECK(source_type IN ('artifact', 'claim')),
    source_id TEXT NOT NULL,
    relation TEXT NOT NULL CHECK(relation IN ('derived_from', 'supports', 'contradicts', 'supersedes', 'references', 'duplicates')),
    target_type TEXT NOT NULL CHECK(target_type IN ('artifact', 'claim')),
    target_id TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    workspace_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    task_id TEXT,
    component TEXT,
    created_at TEXT NOT NULL,
    commit_seq INTEGER NOT NULL UNIQUE,
    UNIQUE(source_type, source_id, relation, target_type, target_id)
) STRICT;

CREATE TABLE IF NOT EXISTS conflict_cases (
    case_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id TEXT NOT NULL UNIQUE,
    relation_id TEXT NOT NULL REFERENCES relations(id) ON DELETE RESTRICT,
    source_claim_id TEXT NOT NULL REFERENCES claims(id) ON DELETE RESTRICT,
    source_revision_id TEXT NOT NULL REFERENCES claim_revisions(id) ON DELETE RESTRICT,
    target_claim_id TEXT NOT NULL REFERENCES claims(id) ON DELETE RESTRICT,
    target_revision_id TEXT NOT NULL REFERENCES claim_revisions(id) ON DELETE RESTRICT,
    state TEXT NOT NULL CHECK(state IN ('open', 'stale', 'resolved')),
    opened_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    state_reason TEXT,
    opened_commit_seq INTEGER NOT NULL,
    UNIQUE(relation_id, source_revision_id, target_revision_id)
) STRICT;

CREATE TABLE IF NOT EXISTS conflict_events (
    event_id TEXT PRIMARY KEY,
    case_id TEXT NOT NULL REFERENCES conflict_cases(case_id) ON DELETE RESTRICT,
    event_type TEXT NOT NULL CHECK(event_type IN ('opened', 'stale', 'resolved')),
    source_revision_id TEXT NOT NULL,
    target_revision_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    operation_commit_seq INTEGER NOT NULL,
    created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS events (
    commit_seq INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    revision_id TEXT,
    actor TEXT,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS idempotency (
    key TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    operation TEXT NOT NULL,
    response_json TEXT NOT NULL,
    commit_seq INTEGER NOT NULL REFERENCES events(commit_seq),
    created_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS artifacts_context_idx
    ON artifacts(workspace_id, project_id, task_id, status);
CREATE INDEX IF NOT EXISTS claims_context_idx
    ON claims(workspace_id, project_id, task_id, status);
CREATE INDEX IF NOT EXISTS artifact_revisions_artifact_idx
    ON artifact_revisions(artifact_id, revision_number DESC);
CREATE INDEX IF NOT EXISTS claim_revisions_claim_idx
    ON claim_revisions(claim_id, revision_number DESC);
CREATE INDEX IF NOT EXISTS relations_source_idx
    ON relations(source_type, source_id);
CREATE INDEX IF NOT EXISTS relations_target_idx
    ON relations(target_type, target_id);
CREATE INDEX IF NOT EXISTS conflict_cases_source_idx
    ON conflict_cases(source_claim_id, state, case_sequence DESC);
CREATE INDEX IF NOT EXISTS conflict_cases_target_idx
    ON conflict_cases(target_claim_id, state, case_sequence DESC);
CREATE UNIQUE INDEX IF NOT EXISTS conflict_cases_one_open_per_relation
    ON conflict_cases(relation_id) WHERE state = 'open';
CREATE INDEX IF NOT EXISTS conflict_events_case_idx
    ON conflict_events(case_id, created_at);

CREATE VIRTUAL TABLE IF NOT EXISTS artifact_fts USING fts5(
    revision_id UNINDEXED,
    artifact_id UNINDEXED,
    title,
    body,
    tokenize = 'unicode61 remove_diacritics 2'
);
CREATE VIRTUAL TABLE IF NOT EXISTS claim_fts USING fts5(
    revision_id UNINDEXED,
    claim_id UNINDEXED,
    statement,
    tokenize = 'unicode61 remove_diacritics 2'
);

CREATE TRIGGER IF NOT EXISTS artifact_revision_index
AFTER INSERT ON artifact_revisions BEGIN
    INSERT INTO artifact_fts(revision_id, artifact_id, title, body)
    VALUES (new.id, new.artifact_id, new.title, new.search_text);
END;
CREATE TRIGGER IF NOT EXISTS claim_revision_index
AFTER INSERT ON claim_revisions BEGIN
    INSERT INTO claim_fts(revision_id, claim_id, statement)
    VALUES (new.id, new.claim_id, new.statement);
END;

CREATE TRIGGER IF NOT EXISTS artifact_revisions_no_update
BEFORE UPDATE ON artifact_revisions BEGIN
    SELECT RAISE(ABORT, 'artifact revisions are immutable');
END;
CREATE TRIGGER IF NOT EXISTS artifact_revisions_no_delete
BEFORE DELETE ON artifact_revisions BEGIN
    SELECT RAISE(ABORT, 'artifact revisions are immutable');
END;
CREATE TRIGGER IF NOT EXISTS claim_revisions_no_update
BEFORE UPDATE ON claim_revisions BEGIN
    SELECT RAISE(ABORT, 'claim revisions are immutable');
END;
CREATE TRIGGER IF NOT EXISTS claim_revisions_no_delete
BEFORE DELETE ON claim_revisions BEGIN
    SELECT RAISE(ABORT, 'claim revisions are immutable');
END;
CREATE TRIGGER IF NOT EXISTS events_no_update
BEFORE UPDATE ON events BEGIN
    SELECT RAISE(ABORT, 'events are immutable');
END;
CREATE TRIGGER IF NOT EXISTS events_no_delete
BEFORE DELETE ON events BEGIN
    SELECT RAISE(ABORT, 'events are immutable');
END;
CREATE TRIGGER IF NOT EXISTS conflict_events_no_update
BEFORE UPDATE ON conflict_events BEGIN
    SELECT RAISE(ABORT, 'conflict events are immutable');
END;
CREATE TRIGGER IF NOT EXISTS conflict_events_no_delete
BEFORE DELETE ON conflict_events BEGIN
    SELECT RAISE(ABORT, 'conflict events are immutable');
END;
CREATE TRIGGER IF NOT EXISTS conflict_cases_no_delete
BEFORE DELETE ON conflict_cases BEGIN
    SELECT RAISE(ABORT, 'conflict cases are durable audit records');
END;
"#;

/// Table-only schema used while atomically rebuilding schema-v2 conflict
/// heads before the general idempotent schema pass creates indexes/triggers.
const CONFLICT_SCHEMA_V3_TABLES: &str = r#"
CREATE TABLE conflict_cases (
    case_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id TEXT NOT NULL UNIQUE,
    relation_id TEXT NOT NULL REFERENCES relations(id) ON DELETE RESTRICT,
    source_claim_id TEXT NOT NULL REFERENCES claims(id) ON DELETE RESTRICT,
    source_revision_id TEXT NOT NULL REFERENCES claim_revisions(id) ON DELETE RESTRICT,
    target_claim_id TEXT NOT NULL REFERENCES claims(id) ON DELETE RESTRICT,
    target_revision_id TEXT NOT NULL REFERENCES claim_revisions(id) ON DELETE RESTRICT,
    state TEXT NOT NULL CHECK(state IN ('open', 'stale', 'resolved')),
    opened_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    state_reason TEXT,
    opened_commit_seq INTEGER NOT NULL,
    UNIQUE(relation_id, source_revision_id, target_revision_id)
) STRICT;
CREATE TABLE conflict_events (
    event_id TEXT PRIMARY KEY,
    case_id TEXT NOT NULL REFERENCES conflict_cases(case_id) ON DELETE RESTRICT,
    event_type TEXT NOT NULL CHECK(event_type IN ('opened', 'stale', 'resolved')),
    source_revision_id TEXT NOT NULL,
    target_revision_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    operation_commit_seq INTEGER NOT NULL,
    created_at TEXT NOT NULL
) STRICT;
"#;

#[derive(Clone)]
pub struct Store {
    connection: Arc<Mutex<Connection>>,
    cas: Cas,
    db_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct MutationResult<T> {
    #[serde(flatten)]
    pub value: T,
    pub commit_seq: i64,
    pub created: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ArtifactRecord {
    pub artifact_id: String,
    pub revision_id: String,
    pub revision_number: i64,
    pub kind: String,
    pub title: String,
    pub media_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<ArtifactContent>,
    pub blob_hash: String,
    pub size_bytes: u64,
    pub status: String,
    pub context: AmbientContext,
    #[serde(default)]
    pub provenance: BTreeMap<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    pub created_at: DateTime<Utc>,
    pub revision_created_at: DateTime<Utc>,
    #[serde(rename = "revision_commit_seq")]
    pub commit_seq: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub forgotten_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ArtifactHistoryPage {
    pub revisions: Vec<ArtifactRecord>,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_before_revision_number: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ClaimRecord {
    pub claim_id: String,
    pub revision_id: String,
    pub revision_number: i64,
    pub claim_type: ClaimType,
    pub status: ClaimStatus,
    pub statement: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    #[serde(default)]
    pub evidence: Vec<EvidenceLocator>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valid_from: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valid_until: Option<DateTime<Utc>>,
    pub context: AmbientContext,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    pub created_at: DateTime<Utc>,
    pub revision_created_at: DateTime<Utc>,
    #[serde(rename = "revision_commit_seq")]
    pub commit_seq: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retraction_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ClaimHistoryPage {
    /// Immutable claim revisions in newest-first order. Revision fields such as
    /// statement, evidence, actor, and `revision_commit_seq` are historical;
    /// lifecycle fields such as status and retraction reason describe the
    /// logical claim's current state on every item.
    pub revisions: Vec<ClaimRecord>,
    /// True when older revisions exist beyond this page.
    pub truncated: bool,
    /// Exclusive cursor for the next older page. Present only when truncated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_before_revision_number: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RelationRecord {
    pub relation_id: String,
    pub source_type: EntityType,
    pub source_id: String,
    pub relation: RelationType,
    pub target_type: EntityType,
    pub target_id: String,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
    pub context: AmbientContext,
    pub created_at: DateTime<Utc>,
    #[serde(rename = "relation_commit_seq")]
    pub commit_seq: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConflictClaimSnapshot {
    pub claim_id: String,
    /// The immutable revision that participated in the contradiction.
    pub frozen: ClaimRecord,
    /// The claim's current revision and lifecycle presentation.
    pub current: ClaimRecord,
    pub frozen_is_current: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConflictListItem {
    pub case_id: String,
    pub relation_id: String,
    pub state: ConflictState,
    pub source: ConflictClaimSnapshot,
    pub target: ConflictClaimSnapshot,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
    pub context: AmbientContext,
    pub opened_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_reason: Option<String>,
    pub opened_commit_seq: i64,
    pub case_sequence: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConflictListResult {
    pub horizon: Horizon,
    pub include_stale: bool,
    pub conflicts: Vec<ConflictListItem>,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_before_case_sequence: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broaden_hint: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OpenConflictRecord {
    pub case_id: String,
    pub relation: RelationRecord,
}

impl From<RelationRecord> for RelationListItem {
    fn from(record: RelationRecord) -> Self {
        Self {
            relation_id: record.relation_id,
            source_type: record.source_type,
            source_id: record.source_id,
            relation: record.relation,
            target_type: record.target_type,
            target_id: record.target_id,
            metadata: record.metadata,
            context: record.context,
            created_at: record.created_at,
            relation_commit_seq: record.commit_seq,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct VerifyReport {
    pub ok: bool,
    pub schema_version: i64,
    pub last_commit_seq: i64,
    pub checked_artifact_revisions: usize,
    pub checked_claim_revisions: usize,
    pub checked_external_blobs: usize,
    pub issues: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BackupReport {
    pub destination: String,
    pub database: String,
    pub blobs: String,
    pub commit_seq: i64,
    pub copied_external_blobs: usize,
    pub created_at: DateTime<Utc>,
}

impl Store {
    /// Open a self-contained data directory (`memoree.sqlite3` plus `blobs/`).
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        ensure_private_directory(data_dir)?;
        Self::open_paths(data_dir.join(MEMOREE_DATABASE_FILE), data_dir.join("blobs"))
    }

    pub fn open_paths(db_path: impl AsRef<Path>, blob_dir: impl AsRef<Path>) -> Result<Self> {
        let db_path = db_path.as_ref().to_path_buf();
        if let Some(parent) = db_path.parent() {
            ensure_private_directory(parent)?;
        }
        let mut connection = Connection::open(&db_path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        let existing_schema_version = preflight_schema_version(&connection)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        if existing_schema_version == Some(2) {
            migrate_schema_v2_to_v3(&mut connection)?;
        }
        connection.execute_batch(SCHEMA)?;
        migrate_schema(&mut connection)?;

        let schema_version: i64 = connection.query_row(
            "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )?;
        if schema_version != SCHEMA_VERSION {
            return Err(MemoryError::Config(format!(
                "database schema version {schema_version} is unsupported (expected {SCHEMA_VERSION})"
            )));
        }
        set_sqlite_file_permissions(&db_path)?;

        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
            cas: Cas::new(blob_dir)?,
            db_path,
        })
    }

    pub fn database_path(&self) -> &Path {
        &self.db_path
    }

    pub fn cas(&self) -> &Cas {
        &self.cas
    }

    pub fn last_commit_seq(&self) -> Result<i64> {
        let connection = self.connection.lock();
        Ok(connection.query_row(
            "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'commit_seq'",
            [],
            |row| row.get(0),
        )?)
    }

    pub fn artifact_put(
        &self,
        context: &AmbientContext,
        input: &ArtifactPutInput,
        idempotency_key: Option<&str>,
        request_hash: &str,
    ) -> Result<MutationResult<ArtifactRecord>> {
        validate_context(context)?;
        validate_artifact_input(&input.kind, &input.title, &input.media_type)?;
        validate_serialized_size("provenance", &input.provenance, MAX_METADATA_BYTES)?;
        validate_optional_size("actor", input.actor.as_deref(), MAX_ACTOR_BYTES)?;
        let bytes = content_bytes(&input.content)?;
        let search_text = searchable_text(&input.media_type, &bytes);

        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(replay) = idempotency_replay::<ArtifactRecord>(
            &transaction,
            idempotency_key,
            request_hash,
            "artifact.put",
        )? {
            return Ok(replay);
        }

        let blob = self.cas.put(&bytes)?;
        let artifact_id = new_id("art");
        let revision_id = new_id("arev");
        let now = Utc::now();
        let commit_seq = next_commit_seq(&transaction)?;
        let provenance = serde_json::to_string(&input.provenance)?;

        transaction.execute(
            "INSERT INTO artifacts (
                id, workspace_id, project_id, task_id, component, kind,
                current_revision_id, status, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'active', ?8, ?8)",
            params![
                artifact_id,
                context.workspace_id,
                context.project_id,
                context.task_id,
                context.component,
                input.kind,
                revision_id,
                now,
            ],
        )?;
        transaction.execute(
            "INSERT INTO artifact_revisions (
                id, artifact_id, revision_number, title, media_type, blob_hash,
                blob_size, inline_blob, search_text, provenance_json, actor,
                created_at, commit_seq
             ) VALUES (?1, ?2, 1, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                revision_id,
                artifact_id,
                input.title,
                input.media_type,
                blob.hash,
                i64::try_from(blob.size_bytes).map_err(|_| MemoryError::ContentTooLarge)?,
                blob.inline_bytes,
                search_text,
                provenance,
                input.actor,
                now,
                commit_seq,
            ],
        )?;

        let record = ArtifactRecord {
            artifact_id: artifact_id.clone(),
            revision_id: revision_id.clone(),
            revision_number: 1,
            kind: input.kind.clone(),
            title: input.title.clone(),
            media_type: input.media_type.clone(),
            // Mutation acknowledgements return metadata only so a large input
            // is not duplicated in the response frame. Exact get is the
            // explicit content retrieval path.
            content: None,
            blob_hash: blob.hash,
            size_bytes: blob.size_bytes,
            status: "active".into(),
            context: context.clone(),
            provenance: input.provenance.clone(),
            actor: input.actor.clone(),
            created_at: now,
            revision_created_at: now,
            commit_seq,
            forgotten_reason: None,
        };
        append_event(
            &transaction,
            commit_seq,
            "artifact.put",
            "artifact",
            &artifact_id,
            Some(&revision_id),
            input.actor.as_deref(),
            &json!({"blob_hash": record.blob_hash, "context": context}),
        )?;
        record_idempotency(
            &transaction,
            idempotency_key,
            request_hash,
            "artifact.put",
            &record,
            commit_seq,
        )?;
        transaction.commit()?;
        Ok(MutationResult {
            value: record,
            commit_seq,
            created: true,
        })
    }

    pub fn artifact_revise(
        &self,
        context: &AmbientContext,
        input: &ArtifactReviseInput,
        idempotency_key: Option<&str>,
        request_hash: &str,
    ) -> Result<MutationResult<ArtifactRecord>> {
        validate_context(context)?;
        require_bounded("artifact_id", &input.artifact_id, MAX_CONTEXT_ID_BYTES)?;
        require_bounded("if_revision", &input.if_revision, MAX_CONTEXT_ID_BYTES)?;
        validate_serialized_size("provenance", &input.provenance, MAX_METADATA_BYTES)?;
        validate_optional_size("actor", input.actor.as_deref(), MAX_ACTOR_BYTES)?;
        let bytes = content_bytes(&input.content)?;

        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let head = load_artifact_raw(&transaction, &input.artifact_id, None)?
            .ok_or_else(|| MemoryError::NotFound(format!("artifact {}", input.artifact_id)))?;
        ensure_write_scope(context, &head.context, "artifact", &input.artifact_id)?;
        if let Some(replay) = idempotency_replay::<ArtifactRecord>(
            &transaction,
            idempotency_key,
            request_hash,
            "artifact.revise",
        )? {
            return Ok(replay);
        }

        if head.status == "forgotten" {
            return Err(MemoryError::InvalidRequest(format!(
                "artifact {} has been forgotten",
                input.artifact_id
            )));
        }
        if head.revision_id != input.if_revision {
            return Err(MemoryError::RevisionConflict {
                entity_type: "artifact",
                entity_id: input.artifact_id.clone(),
                current_revision: head.revision_id,
                requested_revision: input.if_revision.clone(),
            });
        }

        let title = input.title.as_deref().unwrap_or(&head.title);
        let media_type = input.media_type.as_deref().unwrap_or(&head.media_type);
        validate_artifact_input(&head.kind, title, media_type)?;
        let search_text = searchable_text(media_type, &bytes);
        let blob = self.cas.put(&bytes)?;
        let revision_id = new_id("arev");
        let revision_number = head.revision_number + 1;
        let now = Utc::now();
        let commit_seq = next_commit_seq(&transaction)?;
        let provenance = serde_json::to_string(&input.provenance)?;

        transaction.execute(
            "INSERT INTO artifact_revisions (
                id, artifact_id, revision_number, title, media_type, blob_hash,
                blob_size, inline_blob, search_text, provenance_json, actor,
                created_at, commit_seq
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                revision_id,
                input.artifact_id,
                revision_number,
                title,
                media_type,
                blob.hash,
                i64::try_from(blob.size_bytes).map_err(|_| MemoryError::ContentTooLarge)?,
                blob.inline_bytes,
                search_text,
                provenance,
                input.actor,
                now,
                commit_seq,
            ],
        )?;
        transaction.execute(
            "UPDATE artifacts SET current_revision_id = ?1, updated_at = ?2 WHERE id = ?3",
            params![revision_id, now, input.artifact_id],
        )?;

        let record = ArtifactRecord {
            artifact_id: input.artifact_id.clone(),
            revision_id: revision_id.clone(),
            revision_number,
            kind: head.kind,
            title: title.to_owned(),
            media_type: media_type.to_owned(),
            content: None,
            blob_hash: blob.hash,
            size_bytes: blob.size_bytes,
            status: "active".into(),
            context: head.context,
            provenance: input.provenance.clone(),
            actor: input.actor.clone(),
            created_at: head.created_at,
            revision_created_at: now,
            commit_seq,
            forgotten_reason: None,
        };
        append_event(
            &transaction,
            commit_seq,
            "artifact.revise",
            "artifact",
            &input.artifact_id,
            Some(&revision_id),
            input.actor.as_deref(),
            &json!({"previous_revision_id": input.if_revision, "blob_hash": record.blob_hash}),
        )?;
        record_idempotency(
            &transaction,
            idempotency_key,
            request_hash,
            "artifact.revise",
            &record,
            commit_seq,
        )?;
        transaction.commit()?;
        Ok(MutationResult {
            value: record,
            commit_seq,
            created: true,
        })
    }

    pub fn artifact_get(&self, input: &ArtifactGetInput) -> Result<ArtifactRecord> {
        require_bounded("artifact_id", &input.artifact_id, MAX_CONTEXT_ID_BYTES)?;
        let connection = self.connection.lock();
        let raw = load_artifact_raw(
            &connection,
            &input.artifact_id,
            input.revision_id.as_deref(),
        )?
        .ok_or_else(|| {
            let suffix = input
                .revision_id
                .as_deref()
                .map(|revision| format!(" revision {revision}"))
                .unwrap_or_default();
            MemoryError::NotFound(format!("artifact {}{suffix}", input.artifact_id))
        })?;
        raw.into_record(&self.cas, input.include_content)
    }

    pub fn artifact_history(&self, input: &ArtifactHistoryInput) -> Result<ArtifactHistoryPage> {
        require_bounded("artifact_id", &input.artifact_id, MAX_CONTEXT_ID_BYTES)?;
        if input.limit == 0 || input.limit > MAX_HISTORY_ITEMS {
            return Err(MemoryError::InvalidRequest(format!(
                "artifact history limit must be between 1 and {MAX_HISTORY_ITEMS}"
            )));
        }
        if input
            .before_revision_number
            .is_some_and(|number| number <= 0)
        {
            return Err(MemoryError::InvalidRequest(
                "before_revision_number must be positive".into(),
            ));
        }
        let connection = self.connection.lock();
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM artifacts WHERE id = ?1)",
            [&input.artifact_id],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(MemoryError::NotFound(format!(
                "artifact {}",
                input.artifact_id
            )));
        }
        let mut statement = connection.prepare(&format!(
            "{} WHERE a.id = ?1 AND (?2 IS NULL OR ar.revision_number < ?2)
             ORDER BY ar.revision_number DESC LIMIT ?3",
            artifact_select()
        ))?;
        let fetch_limit = i64::try_from(input.limit + 1)
            .map_err(|_| MemoryError::InvalidRequest("history limit is too large".into()))?;
        let rows = statement.query_map(
            params![input.artifact_id, input.before_revision_number, fetch_limit],
            RawArtifact::from_row,
        )?;
        let mut records = Vec::new();
        for row in rows {
            records.push(row?.into_record(&self.cas, false)?);
        }
        let truncated = records.len() > input.limit;
        records.truncate(input.limit);
        let next_before_revision_number = truncated
            .then(|| records.last().map(|record| record.revision_number))
            .flatten();
        Ok(ArtifactHistoryPage {
            revisions: records,
            truncated,
            next_before_revision_number,
        })
    }

    pub fn artifact_forget(
        &self,
        context: &AmbientContext,
        input: &ArtifactForgetInput,
        idempotency_key: Option<&str>,
        request_hash: &str,
    ) -> Result<MutationResult<ArtifactRecord>> {
        validate_context(context)?;
        require_bounded("artifact_id", &input.artifact_id, MAX_CONTEXT_ID_BYTES)?;
        require_bounded("reason", &input.reason, MAX_REASON_BYTES)?;
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let head = load_artifact_raw(&transaction, &input.artifact_id, None)?
            .ok_or_else(|| MemoryError::NotFound(format!("artifact {}", input.artifact_id)))?;
        ensure_write_scope(context, &head.context, "artifact", &input.artifact_id)?;
        if let Some(replay) = idempotency_replay::<ArtifactRecord>(
            &transaction,
            idempotency_key,
            request_hash,
            "artifact.forget",
        )? {
            return Ok(replay);
        }
        if head.status == "forgotten" {
            return Err(MemoryError::InvalidRequest(format!(
                "artifact {} is already forgotten",
                input.artifact_id
            )));
        }
        let now = Utc::now();
        let commit_seq = next_commit_seq(&transaction)?;
        transaction.execute(
            "UPDATE artifacts
             SET status = 'forgotten', forgotten_reason = ?1, updated_at = ?2
             WHERE id = ?3",
            params![input.reason, now, input.artifact_id],
        )?;
        let mut record = head.into_record(&self.cas, false)?;
        record.status = "forgotten".into();
        record.forgotten_reason = Some(input.reason.clone());
        append_event(
            &transaction,
            commit_seq,
            "artifact.forget",
            "artifact",
            &input.artifact_id,
            Some(&record.revision_id),
            None,
            &json!({"reason": input.reason}),
        )?;
        record_idempotency(
            &transaction,
            idempotency_key,
            request_hash,
            "artifact.forget",
            &record,
            commit_seq,
        )?;
        transaction.commit()?;
        Ok(MutationResult {
            value: record,
            commit_seq,
            created: true,
        })
    }

    pub fn claim_assert(
        &self,
        context: &AmbientContext,
        input: &ClaimAssertInput,
        idempotency_key: Option<&str>,
        request_hash: &str,
    ) -> Result<MutationResult<ClaimRecord>> {
        validate_context(context)?;
        validate_claim(
            &input.statement,
            input.confidence,
            input.valid_from,
            input.valid_until,
        )?;
        validate_optional_size("actor", input.actor.as_deref(), MAX_ACTOR_BYTES)?;
        if input.evidence.len() > MAX_EVIDENCE_ITEMS {
            return Err(MemoryError::InvalidRequest(format!(
                "evidence must not contain more than {MAX_EVIDENCE_ITEMS} entries"
            )));
        }
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(replay) = idempotency_replay::<ClaimRecord>(
            &transaction,
            idempotency_key,
            request_hash,
            "claim.assert",
        )? {
            return Ok(replay);
        }
        validate_evidence(&transaction, &input.evidence)?;

        let claim_id = new_id("clm");
        let revision_id = new_id("crev");
        let now = Utc::now();
        let commit_seq = next_commit_seq(&transaction)?;
        let claim_type = enum_string(&input.claim_type)?;
        let evidence = serde_json::to_string(&input.evidence)?;
        transaction.execute(
            "INSERT INTO claims (
                id, workspace_id, project_id, task_id, component, claim_type,
                current_revision_id, status, valid_from, valid_until,
                created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'active', ?8, ?9, ?10, ?10)",
            params![
                claim_id,
                context.workspace_id,
                context.project_id,
                context.task_id,
                context.component,
                claim_type,
                revision_id,
                input.valid_from,
                input.valid_until,
                now,
            ],
        )?;
        transaction.execute(
            "INSERT INTO claim_revisions (
                id, claim_id, revision_number, statement, confidence,
                evidence_json, actor, created_at, commit_seq
             ) VALUES (?1, ?2, 1, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                revision_id,
                claim_id,
                input.statement,
                input.confidence,
                evidence,
                input.actor,
                now,
                commit_seq,
            ],
        )?;
        let record = ClaimRecord {
            claim_id: claim_id.clone(),
            revision_id: revision_id.clone(),
            revision_number: 1,
            claim_type: input.claim_type,
            status: ClaimStatus::Active,
            statement: input.statement.clone(),
            confidence: input.confidence,
            evidence: input.evidence.clone(),
            valid_from: input.valid_from,
            valid_until: input.valid_until,
            context: context.clone(),
            actor: input.actor.clone(),
            created_at: now,
            revision_created_at: now,
            commit_seq,
            retraction_reason: None,
        };
        append_event(
            &transaction,
            commit_seq,
            "claim.assert",
            "claim",
            &claim_id,
            Some(&revision_id),
            input.actor.as_deref(),
            &json!({"claim_type": claim_type, "evidence_count": input.evidence.len()}),
        )?;
        record_idempotency(
            &transaction,
            idempotency_key,
            request_hash,
            "claim.assert",
            &record,
            commit_seq,
        )?;
        transaction.commit()?;
        Ok(MutationResult {
            value: record,
            commit_seq,
            created: true,
        })
    }

    pub fn claim_revise(
        &self,
        context: &AmbientContext,
        input: &ClaimReviseInput,
        idempotency_key: Option<&str>,
        request_hash: &str,
    ) -> Result<MutationResult<ClaimRecord>> {
        validate_context(context)?;
        require_bounded("claim_id", &input.claim_id, MAX_CONTEXT_ID_BYTES)?;
        require_bounded("if_revision", &input.if_revision, MAX_CONTEXT_ID_BYTES)?;
        validate_claim(&input.statement, input.confidence, None, None)?;
        validate_optional_size("actor", input.actor.as_deref(), MAX_ACTOR_BYTES)?;
        if input.evidence.len() > MAX_EVIDENCE_ITEMS {
            return Err(MemoryError::InvalidRequest(format!(
                "evidence must not contain more than {MAX_EVIDENCE_ITEMS} entries"
            )));
        }
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let head = load_claim_raw(&transaction, &input.claim_id, None)?
            .ok_or_else(|| MemoryError::NotFound(format!("claim {}", input.claim_id)))?;
        ensure_write_scope(context, &head.context, "claim", &input.claim_id)?;
        if let Some(replay) = idempotency_replay::<ClaimRecord>(
            &transaction,
            idempotency_key,
            request_hash,
            "claim.revise",
        )? {
            return Ok(replay);
        }
        validate_evidence(&transaction, &input.evidence)?;
        if head.revision_id != input.if_revision {
            return Err(MemoryError::RevisionConflict {
                entity_type: "claim",
                entity_id: input.claim_id.clone(),
                current_revision: head.revision_id,
                requested_revision: input.if_revision.clone(),
            });
        }
        if matches!(head.status.as_str(), "retracted" | "superseded") {
            return Err(MemoryError::InvalidRequest(format!(
                "cannot revise {} claim {}",
                head.status, input.claim_id
            )));
        }

        let revision_id = new_id("crev");
        let revision_number = head.revision_number + 1;
        let evidence = serde_json::to_string(&input.evidence)?;
        let now = Utc::now();
        let commit_seq = next_commit_seq(&transaction)?;
        transaction.execute(
            "INSERT INTO claim_revisions (
                id, claim_id, revision_number, statement, confidence,
                evidence_json, actor, created_at, commit_seq
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                revision_id,
                input.claim_id,
                revision_number,
                input.statement,
                input.confidence,
                evidence,
                input.actor,
                now,
                commit_seq,
            ],
        )?;
        transaction.execute(
            "UPDATE claims SET current_revision_id = ?1, updated_at = ?2 WHERE id = ?3",
            params![revision_id, now, input.claim_id],
        )?;
        let mut affected_claims = transition_open_conflicts(
            &transaction,
            &input.claim_id,
            ConflictState::Stale,
            "claim revision changed",
            commit_seq,
            now,
        )?;
        affected_claims.extend(reassess_live_conflicts(
            &transaction,
            &input.claim_id,
            commit_seq,
            now,
        )?);
        affected_claims.push(input.claim_id.clone());
        recompute_claim_statuses(&transaction, &affected_claims, now)?;
        let record = load_claim_raw(&transaction, &input.claim_id, None)?
            .ok_or_else(|| MemoryError::NotFound(format!("claim {}", input.claim_id)))?
            .into_record()?;
        append_event(
            &transaction,
            commit_seq,
            "claim.revise",
            "claim",
            &input.claim_id,
            Some(&revision_id),
            input.actor.as_deref(),
            &json!({"previous_revision_id": input.if_revision}),
        )?;
        record_idempotency(
            &transaction,
            idempotency_key,
            request_hash,
            "claim.revise",
            &record,
            commit_seq,
        )?;
        transaction.commit()?;
        Ok(MutationResult {
            value: record,
            commit_seq,
            created: true,
        })
    }

    pub fn claim_get(&self, input: &ClaimGetInput) -> Result<ClaimRecord> {
        require_bounded("claim_id", &input.claim_id, MAX_CONTEXT_ID_BYTES)?;
        let connection = self.connection.lock();
        load_claim_raw(&connection, &input.claim_id, input.revision_id.as_deref())?
            .ok_or_else(|| MemoryError::NotFound(format!("claim {}", input.claim_id)))?
            .into_record()
    }

    pub fn claim_history(&self, input: &ClaimHistoryInput) -> Result<ClaimHistoryPage> {
        require_bounded("claim_id", &input.claim_id, MAX_CONTEXT_ID_BYTES)?;
        if input.limit == 0 || input.limit > MAX_HISTORY_ITEMS {
            return Err(MemoryError::InvalidRequest(format!(
                "claim history limit must be between 1 and {MAX_HISTORY_ITEMS}"
            )));
        }
        if input
            .before_revision_number
            .is_some_and(|number| number <= 0)
        {
            return Err(MemoryError::InvalidRequest(
                "before_revision_number must be positive".into(),
            ));
        }

        let connection = self.connection.lock();
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM claims WHERE id = ?1)",
            [&input.claim_id],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(MemoryError::NotFound(format!("claim {}", input.claim_id)));
        }

        let mut statement = connection.prepare(&format!(
            "{} WHERE c.id = ?1 AND (?2 IS NULL OR cr.revision_number < ?2)
             ORDER BY cr.revision_number DESC LIMIT ?3",
            claim_select()
        ))?;
        let fetch_limit = i64::try_from(input.limit + 1)
            .map_err(|_| MemoryError::InvalidRequest("history limit is too large".into()))?;
        let rows = statement.query_map(
            params![input.claim_id, input.before_revision_number, fetch_limit],
            RawClaim::from_row,
        )?;
        let mut records = Vec::new();
        for row in rows {
            records.push(row?.into_record()?);
        }
        let truncated = records.len() > input.limit;
        records.truncate(input.limit);
        let next_before_revision_number = truncated
            .then(|| records.last().map(|record| record.revision_number))
            .flatten();
        Ok(ClaimHistoryPage {
            revisions: records,
            truncated,
            next_before_revision_number,
        })
    }

    pub fn claim_retract(
        &self,
        context: &AmbientContext,
        input: &ClaimRetractInput,
        idempotency_key: Option<&str>,
        request_hash: &str,
    ) -> Result<MutationResult<ClaimRecord>> {
        validate_context(context)?;
        require_bounded("claim_id", &input.claim_id, MAX_CONTEXT_ID_BYTES)?;
        require_bounded("reason", &input.reason, MAX_REASON_BYTES)?;
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let head = load_claim_raw(&transaction, &input.claim_id, None)?
            .ok_or_else(|| MemoryError::NotFound(format!("claim {}", input.claim_id)))?;
        ensure_write_scope(context, &head.context, "claim", &input.claim_id)?;
        if let Some(replay) = idempotency_replay::<ClaimRecord>(
            &transaction,
            idempotency_key,
            request_hash,
            "claim.retract",
        )? {
            return Ok(replay);
        }
        if head.status == "retracted" {
            return Err(MemoryError::InvalidRequest(format!(
                "claim {} is already retracted",
                input.claim_id
            )));
        }
        let now = Utc::now();
        let commit_seq = next_commit_seq(&transaction)?;
        transaction.execute(
            "UPDATE claims
             SET status = 'retracted', retraction_reason = ?1, updated_at = ?2
             WHERE id = ?3",
            params![input.reason, now, input.claim_id],
        )?;
        let affected_claims = transition_open_conflicts(
            &transaction,
            &input.claim_id,
            ConflictState::Resolved,
            "claim retracted",
            commit_seq,
            now,
        )?;
        recompute_claim_statuses(&transaction, &affected_claims, now)?;
        let mut record = head.into_record()?;
        record.status = ClaimStatus::Retracted;
        record.retraction_reason = Some(input.reason.clone());
        append_event(
            &transaction,
            commit_seq,
            "claim.retract",
            "claim",
            &input.claim_id,
            Some(&record.revision_id),
            None,
            &json!({"reason": input.reason}),
        )?;
        record_idempotency(
            &transaction,
            idempotency_key,
            request_hash,
            "claim.retract",
            &record,
            commit_seq,
        )?;
        transaction.commit()?;
        Ok(MutationResult {
            value: record,
            commit_seq,
            created: true,
        })
    }

    pub fn relation_put(
        &self,
        context: &AmbientContext,
        input: &RelationPutInput,
        idempotency_key: Option<&str>,
        request_hash: &str,
    ) -> Result<MutationResult<RelationRecord>> {
        validate_context(context)?;
        require_bounded("source_id", &input.source_id, MAX_CONTEXT_ID_BYTES)?;
        require_bounded("target_id", &input.target_id, MAX_CONTEXT_ID_BYTES)?;
        validate_serialized_size("relation metadata", &input.metadata, MAX_METADATA_BYTES)?;
        if input.source_type as u8 == input.target_type as u8 && input.source_id == input.target_id
        {
            return Err(MemoryError::InvalidRequest(
                "a relation cannot point an entity to itself".into(),
            ));
        }

        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        ensure_entity_in_write_scope(&transaction, context, input.source_type, &input.source_id)?;
        ensure_entity_in_write_scope(&transaction, context, input.target_type, &input.target_id)?;
        if let Some(replay) = idempotency_replay::<RelationRecord>(
            &transaction,
            idempotency_key,
            request_hash,
            "relation.put",
        )? {
            return Ok(replay);
        }
        let source_type = enum_string(&input.source_type)?;
        let relation = enum_string(&input.relation)?;
        let target_type = enum_string(&input.target_type)?;
        let metadata_json = serde_json::to_string(&input.metadata)?;
        if let Some(raw) = load_relation_by_edge(
            &transaction,
            &source_type,
            &input.source_id,
            &relation,
            &target_type,
            &input.target_id,
        )? {
            let existing = raw.into_record()?;
            if existing.metadata != input.metadata {
                return Err(MemoryError::InvalidRequest(format!(
                    "relation {} already exists with different metadata",
                    existing.relation_id
                )));
            }
            let commit_seq = existing.commit_seq;
            record_idempotency(
                &transaction,
                idempotency_key,
                request_hash,
                "relation.put",
                &existing,
                commit_seq,
            )?;
            transaction.commit()?;
            return Ok(MutationResult {
                commit_seq,
                value: existing,
                created: false,
            });
        }

        validate_relation_semantics(&transaction, input)?;
        let relation_id = new_id("rel");
        let now = Utc::now();
        let commit_seq = next_commit_seq(&transaction)?;
        transaction.execute(
            "INSERT INTO relations (
                id, source_type, source_id, relation, target_type, target_id,
                metadata_json, workspace_id, project_id, task_id, component,
                created_at, commit_seq
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                relation_id,
                source_type,
                input.source_id,
                relation,
                target_type,
                input.target_id,
                metadata_json,
                context.workspace_id,
                context.project_id,
                context.task_id,
                context.component,
                now,
                commit_seq,
            ],
        )?;

        apply_relation_semantics(&transaction, input, &relation_id, commit_seq, now)?;
        let record = RelationRecord {
            relation_id: relation_id.clone(),
            source_type: input.source_type,
            source_id: input.source_id.clone(),
            relation: input.relation,
            target_type: input.target_type,
            target_id: input.target_id.clone(),
            metadata: input.metadata.clone(),
            context: AmbientContext {
                workspace_id: context.workspace_id.clone(),
                project_id: context.project_id.clone(),
                task_id: context.task_id.clone(),
                component: context.component.clone(),
                pins: Vec::new(),
            },
            created_at: now,
            commit_seq,
        };
        append_event(
            &transaction,
            commit_seq,
            "relation.put",
            "relation",
            &relation_id,
            None,
            None,
            &serde_json::to_value(&record)?,
        )?;
        record_idempotency(
            &transaction,
            idempotency_key,
            request_hash,
            "relation.put",
            &record,
            commit_seq,
        )?;
        transaction.commit()?;
        Ok(MutationResult {
            value: record,
            commit_seq,
            created: true,
        })
    }

    pub fn relation_list(
        &self,
        context: &AmbientContext,
        input: &RelationListInput,
    ) -> Result<RelationListResult> {
        validate_context(context)?;
        require_bounded("entity_id", &input.entity_id, MAX_CONTEXT_ID_BYTES)?;
        if let Some(reason) = &input.reason
            && reason.len() > MAX_REASON_BYTES
        {
            return Err(MemoryError::InvalidRequest(format!(
                "relation list reason must not exceed {MAX_REASON_BYTES} bytes"
            )));
        }
        if input.limit == 0 || input.limit > MAX_RELATION_LIST_ITEMS {
            return Err(MemoryError::InvalidRequest(format!(
                "relation list limit must be between 1 and {MAX_RELATION_LIST_ITEMS}"
            )));
        }
        if input
            .before_commit_seq
            .is_some_and(|sequence| sequence <= 0)
        {
            return Err(MemoryError::InvalidRequest(
                "before_commit_seq must be positive".into(),
            ));
        }

        let entity_type = enum_string(&input.entity_type)?;
        let direction = enum_string(&input.direction)?;
        let horizon = enum_string(&input.horizon)?;
        let relation = input
            .relation
            .map(|relation| enum_string(&relation))
            .transpose()?;
        let connection = self.connection.lock();
        ensure_entity_in_read_scope(
            &connection,
            context,
            input.entity_type,
            &input.entity_id,
            input.horizon,
        )?;

        let mut statement = connection.prepare(
            "SELECT id, source_type, source_id, relation, target_type, target_id,
                    metadata_json, workspace_id, project_id, task_id, component,
                    created_at, commit_seq
             FROM relations
             WHERE (
                    (?3 = 'both' AND (
                        (source_type = ?1 AND source_id = ?2)
                        OR (target_type = ?1 AND target_id = ?2)
                    ))
                    OR (?3 = 'outgoing' AND source_type = ?1 AND source_id = ?2)
                    OR (?3 = 'incoming' AND target_type = ?1 AND target_id = ?2)
               )
               AND (?4 IS NULL OR relation = ?4)
               AND (?5 IS NULL OR commit_seq < ?5)
               AND (
                    ?7 = 'personal'
                    OR (?7 = 'workspace' AND workspace_id = ?8)
                    OR (?7 = 'ambient' AND workspace_id = ?8 AND project_id = ?9
                        AND (?10 IS NULL OR task_id IS NULL OR task_id = ?10))
               )
             ORDER BY commit_seq DESC
             LIMIT ?6",
        )?;
        let fetch_limit = i64::try_from(input.limit + 1)
            .map_err(|_| MemoryError::InvalidRequest("relation list limit is too large".into()))?;
        let rows = statement.query_map(
            params![
                entity_type,
                input.entity_id,
                direction,
                relation,
                input.before_commit_seq,
                fetch_limit,
                horizon,
                context.workspace_id,
                context.project_id,
                context.task_id,
            ],
            RawRelation::from_row,
        )?;

        let mut relations = Vec::new();
        let mut encoded_bytes = 0usize;
        let mut truncated = false;
        for row in rows {
            let item = RelationListItem::from(row?.into_record()?);
            if relations.len() == input.limit {
                truncated = true;
                break;
            }
            let item_bytes = serde_json::to_vec(&item)?.len();
            if encoded_bytes.saturating_add(item_bytes) > MAX_RELATION_LIST_ENCODED_BYTES {
                truncated = true;
                break;
            }
            encoded_bytes += item_bytes;
            relations.push(item);
        }
        let next_before_commit_seq = truncated
            .then(|| relations.last().map(|item| item.relation_commit_seq))
            .flatten();

        let broaden_hint = if relations.is_empty() {
            match input.horizon {
                Horizon::Ambient => Some(
                    "No ambient relations. Retry explicitly with horizon=workspace (and a reason) if broader graph traversal is needed."
                        .into(),
                ),
                Horizon::Workspace => Some(
                    "No workspace relations. Retry explicitly with horizon=personal (and a reason) if broader graph traversal is needed."
                        .into(),
                ),
                Horizon::Personal => None,
            }
        } else {
            None
        };

        Ok(RelationListResult {
            entity_type: input.entity_type,
            entity_id: input.entity_id.clone(),
            direction: input.direction,
            relation: input.relation,
            horizon: input.horizon,
            relations,
            truncated,
            next_before_commit_seq,
            broaden_hint,
        })
    }

    pub fn relations_for(
        &self,
        entity_type: EntityType,
        entity_id: &str,
    ) -> Result<Vec<RelationRecord>> {
        let entity_type = enum_string(&entity_type)?;
        let connection = self.connection.lock();
        let mut statement = connection.prepare(
            "SELECT id, source_type, source_id, relation, target_type, target_id,
                    metadata_json, workspace_id, project_id, task_id, component,
                    created_at, commit_seq
             FROM relations
             WHERE (source_type = ?1 AND source_id = ?2)
                OR (target_type = ?1 AND target_id = ?2)
             ORDER BY commit_seq",
        )?;
        let rows = statement.query_map(params![entity_type, entity_id], RawRelation::from_row)?;
        rows.map(|row| {
            row.map_err(MemoryError::from)
                .and_then(RawRelation::into_record)
        })
        .collect()
    }

    pub fn conflicts_for_claims(
        &self,
        context: &AmbientContext,
        horizon: Horizon,
        claim_ids: &[String],
    ) -> Result<Vec<OpenConflictRecord>> {
        validate_context(context)?;
        let horizon = enum_string(&horizon)?;
        let connection = self.connection.lock();
        let mut result = Vec::new();
        let mut statement = connection.prepare(
            "SELECT r.id, r.source_type, r.source_id, r.relation, r.target_type, r.target_id,
                    r.metadata_json, r.workspace_id, r.project_id, r.task_id, r.component,
                    r.created_at, r.commit_seq, cc.case_id
             FROM relations r
             JOIN conflict_cases cc ON cc.relation_id = r.id
             JOIN claims sc ON sc.id = cc.source_claim_id
             JOIN claims tc ON tc.id = cc.target_claim_id
             WHERE r.relation = 'contradicts'
               AND cc.state = 'open'
               AND sc.current_revision_id = cc.source_revision_id
               AND tc.current_revision_id = cc.target_revision_id
               AND (r.source_id = ?1 OR r.target_id = ?1)
               AND (
                    ?2 = 'personal'
                    OR (?2 = 'workspace' AND r.workspace_id = ?3)
                    OR (?2 = 'ambient' AND r.workspace_id = ?3 AND r.project_id = ?4
                        AND (?5 IS NULL OR r.task_id IS NULL OR r.task_id = ?5))
               )
             ORDER BY r.commit_seq",
        )?;
        for claim_id in claim_ids {
            let rows = statement.query_map(
                params![
                    claim_id,
                    horizon,
                    context.workspace_id,
                    context.project_id,
                    context.task_id,
                ],
                |row| Ok((RawRelation::from_row(row)?, row.get::<_, String>(13)?)),
            )?;
            for row in rows {
                let (relation, case_id) = row?;
                let record = OpenConflictRecord {
                    case_id,
                    relation: relation.into_record()?,
                };
                if !result
                    .iter()
                    .any(|existing: &OpenConflictRecord| existing.case_id == record.case_id)
                {
                    result.push(record);
                }
            }
        }
        Ok(result)
    }

    pub fn conflict_list(
        &self,
        context: &AmbientContext,
        input: &ConflictListInput,
    ) -> Result<ConflictListResult> {
        validate_context(context)?;
        if let Some(reason) = &input.reason
            && reason.len() > MAX_REASON_BYTES
        {
            return Err(MemoryError::InvalidRequest(format!(
                "conflict list reason must not exceed {MAX_REASON_BYTES} bytes"
            )));
        }
        if input.limit == 0 || input.limit > MAX_CONFLICT_LIST_ITEMS {
            return Err(MemoryError::InvalidRequest(format!(
                "conflict list limit must be between 1 and {MAX_CONFLICT_LIST_ITEMS}"
            )));
        }
        if input
            .before_case_sequence
            .is_some_and(|sequence| sequence <= 0)
        {
            return Err(MemoryError::InvalidRequest(
                "before_case_sequence must be positive".into(),
            ));
        }

        let horizon = enum_string(&input.horizon)?;
        let connection = self.connection.lock();
        let mut statement = connection.prepare(
            "SELECT cc.case_id, cc.relation_id, cc.source_claim_id, cc.source_revision_id,
                    cc.target_claim_id, cc.target_revision_id, cc.state,
                    cc.opened_at, cc.updated_at, cc.state_reason,
                    cc.opened_commit_seq, cc.case_sequence, r.metadata_json,
                    r.workspace_id, r.project_id, r.task_id, r.component
               FROM conflict_cases cc
               JOIN relations r ON r.id = cc.relation_id
              WHERE (cc.state = 'open' OR (?1 AND cc.state = 'stale'))
                AND (?2 IS NULL OR cc.case_sequence < ?2)
                AND (
                    ?3 = 'personal'
                    OR (?3 = 'workspace' AND r.workspace_id = ?4)
                    OR (?3 = 'ambient' AND r.workspace_id = ?4 AND r.project_id = ?5
                        AND (?6 IS NULL OR r.task_id IS NULL OR r.task_id = ?6))
                )
              ORDER BY cc.case_sequence DESC
              LIMIT ?7",
        )?;
        let fetch_limit = i64::try_from(input.limit + 1)
            .map_err(|_| MemoryError::InvalidRequest("conflict list limit is too large".into()))?;
        let rows = statement.query_map(
            params![
                input.include_stale,
                input.before_case_sequence,
                horizon,
                context.workspace_id,
                context.project_id,
                context.task_id,
                fetch_limit,
            ],
            RawConflictCase::from_row,
        )?;

        let mut conflicts = Vec::new();
        let mut encoded_bytes = 0usize;
        let mut truncated = false;
        for row in rows {
            if conflicts.len() == input.limit {
                truncated = true;
                break;
            }
            let item = row?.into_item(&connection)?;
            let item_bytes = serde_json::to_vec(&item)?.len();
            if encoded_bytes.saturating_add(item_bytes) > MAX_CONFLICT_LIST_ENCODED_BYTES {
                truncated = true;
                break;
            }
            encoded_bytes += item_bytes;
            conflicts.push(item);
        }
        let next_before_case_sequence = truncated
            .then(|| conflicts.last().map(|item| item.case_sequence))
            .flatten();
        let broaden_hint = if conflicts.is_empty() {
            match input.horizon {
                Horizon::Ambient => Some(
                    "No actionable ambient conflicts. Retry explicitly with horizon=workspace (and a reason) only if broader reconciliation is needed."
                        .into(),
                ),
                Horizon::Workspace => Some(
                    "No actionable workspace conflicts. Retry explicitly with horizon=personal (and a reason) only if broader reconciliation is needed."
                        .into(),
                ),
                Horizon::Personal => None,
            }
        } else {
            None
        };

        Ok(ConflictListResult {
            horizon: input.horizon,
            include_stale: input.include_stale,
            conflicts,
            truncated,
            next_before_case_sequence,
            broaden_hint,
        })
    }

    pub fn search(&self, context: &AmbientContext, input: &SearchInput) -> Result<SearchResult> {
        self.search_filtered(context, input, None)
    }

    pub fn search_entity(
        &self,
        context: &AmbientContext,
        input: &SearchInput,
        entity_type: EntityType,
    ) -> Result<SearchResult> {
        self.search_filtered(context, input, Some(entity_type))
    }

    fn search_filtered(
        &self,
        context: &AmbientContext,
        input: &SearchInput,
        entity_type: Option<EntityType>,
    ) -> Result<SearchResult> {
        validate_context(context)?;
        if let Some(reason) = &input.reason
            && reason.len() > MAX_REASON_BYTES
        {
            return Err(MemoryError::InvalidRequest(format!(
                "search reason must not exceed {MAX_REASON_BYTES} bytes"
            )));
        }
        let query = fts_query(&input.query)?;
        if input.limit == 0 || input.limit > MAX_SEARCH_ITEMS {
            return Err(MemoryError::InvalidRequest(format!(
                "search limit must be between 1 and {MAX_SEARCH_ITEMS}"
            )));
        }
        let connection = self.connection.lock();
        let current_seq: i64 = connection.query_row(
            "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'commit_seq'",
            [],
            |row| row.get(0),
        )?;
        if let Some(required) = input.min_commit_seq
            && required > current_seq
        {
            return Err(MemoryError::IndexNotReady {
                requested: required,
                current: current_seq,
            });
        }

        let candidate_limit = i64::try_from((input.limit + 1).saturating_mul(2))
            .map_err(|_| MemoryError::InvalidRequest("search limit is too large".into()))?;
        let (pin_artifacts, exact_revision_pins) = normalized_artifact_pins(&context.pins);
        let pins = serde_json::to_string(&pin_artifacts)?;
        let pinned_revisions = serde_json::to_string(&exact_revision_pins)?;
        let horizon = enum_string(&input.horizon)?;
        let historical = i64::from(input.include_historical);
        // One instant governs both filtering and the validity metadata emitted
        // for every claim hit in this search response.
        let evaluated_at = Utc::now();
        let artifact_sql = format!(
            "SELECT a.id, ar.id, ar.title,
                    snippet(artifact_fts, 3, '‹', '›', ' … ', 36),
                    a.status, a.workspace_id, a.project_id, a.task_id, a.component,
                    a.kind, ar.provenance_json, ar.created_at,
                    a.current_revision_id = ar.id,
                    bm25(artifact_fts, 0.0, 0.0, 5.0, 1.0)
             FROM artifact_fts
             JOIN artifact_revisions ar ON ar.id = artifact_fts.revision_id
             JOIN artifacts a ON a.id = ar.artifact_id
             WHERE artifact_fts MATCH ?1
               AND (?2 = 1 OR (a.status = 'active' AND (
                    a.current_revision_id = ar.id
                    OR (a.id || '@' || ar.id) IN (SELECT value FROM json_each(?8))
               )))
               AND ({} OR (a.id || '@' || ar.id) IN (SELECT value FROM json_each(?8)))
             ORDER BY bm25(artifact_fts, 0.0, 0.0, 5.0, 1.0), ar.commit_seq DESC
             LIMIT ?9",
            horizon_filter_sql("a", true)
        );
        let mut hits = Vec::new();
        if !matches!(entity_type, Some(EntityType::Claim)) {
            let mut artifact_statement = connection.prepare(&artifact_sql)?;
            let artifact_rows = artifact_statement.query_map(
                params![
                    query,
                    historical,
                    horizon,
                    context.workspace_id,
                    context.project_id,
                    context.task_id,
                    pins,
                    pinned_revisions,
                    candidate_limit,
                ],
                search_artifact_row,
            )?;
            for row in artifact_rows {
                hits.push(row?.into_candidate()?);
            }
        }

        let claim_sql = format!(
            "SELECT c.id, cr.id, c.claim_type, cr.statement, c.status,
                    c.workspace_id, c.project_id, c.task_id, c.component,
                    cr.evidence_json, cr.confidence, c.valid_from, c.valid_until,
                    c.current_revision_id = cr.id, ?10,
                    CASE
                      WHEN c.valid_from IS NOT NULL AND c.valid_from > ?10 THEN 'future'
                      WHEN c.valid_until IS NOT NULL AND c.valid_until <= ?10 THEN 'expired'
                      ELSE 'current'
                    END,
                    cr.created_at,
                    bm25(claim_fts, 0.0, 0.0, 1.0)
             FROM claim_fts
             JOIN claim_revisions cr ON cr.id = claim_fts.revision_id
             JOIN claims c ON c.id = cr.claim_id
             WHERE claim_fts MATCH ?1
               AND (?2 = 1 OR (
                    c.status IN ('active', 'conflicted')
                    AND c.current_revision_id = cr.id
                    AND (c.valid_from IS NULL OR c.valid_from <= ?10)
                    AND (c.valid_until IS NULL OR c.valid_until > ?10)
               ))
               AND {}
             ORDER BY bm25(claim_fts, 0.0, 0.0, 1.0), cr.commit_seq DESC
             LIMIT ?9",
            horizon_filter_sql("c", false)
        );
        if !matches!(entity_type, Some(EntityType::Artifact)) {
            let mut claim_statement = connection.prepare(&claim_sql)?;
            let claim_rows = claim_statement.query_map(
                params![
                    query,
                    historical,
                    horizon,
                    context.workspace_id,
                    context.project_id,
                    context.task_id,
                    "[]",
                    "[]",
                    candidate_limit,
                    evaluated_at,
                ],
                search_claim_row,
            )?;
            for row in claim_rows {
                hits.push(row?.into_candidate()?);
            }
        }

        hits.sort_by(|left, right| {
            right
                .hit
                .score
                .partial_cmp(&left.hit.score)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.hit.entity_id.cmp(&right.hit.entity_id))
        });
        let truncated = hits.len() > input.limit;
        hits.truncate(input.limit);
        let hits = rerank_with_recency(hits, input.recency.enabled, evaluated_at);
        Ok(SearchResult {
            query: input.query.clone(),
            horizon: input.horizon,
            retrieval_mode: if input.recency.enabled {
                format!("sqlite_fts5_bm25+{RECENCY_POLICY_VERSION}")
            } else {
                "sqlite_fts5_bm25".into()
            },
            broaden_hint: if hits.is_empty() && matches!(input.horizon, Horizon::Ambient) {
                Some(
                    "No ambient matches. Retry explicitly with horizon=workspace (and a reason) if broader precedent is needed."
                        .into(),
                )
            } else {
                None
            },
            hits,
            truncated,
            refine_hint: truncated.then(|| {
                format!(
                    "More matches exist than the limit of {}. Refine the query or explicitly raise limit up to {MAX_SEARCH_ITEMS}.",
                    input.limit
                )
            }),
        })
    }

    pub fn reindex(&self) -> Result<()> {
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(
            "DELETE FROM artifact_fts;
             INSERT INTO artifact_fts(revision_id, artifact_id, title, body)
               SELECT id, artifact_id, title, search_text FROM artifact_revisions;
             DELETE FROM claim_fts;
             INSERT INTO claim_fts(revision_id, claim_id, statement)
               SELECT id, claim_id, statement FROM claim_revisions;",
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn verify(&self) -> Result<VerifyReport> {
        let connection = self.connection.lock();
        let schema_version: i64 = connection.query_row(
            "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )?;
        let last_commit_seq: i64 = connection.query_row(
            "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'commit_seq'",
            [],
            |row| row.get(0),
        )?;
        let checked_artifact_revisions =
            connection.query_row("SELECT COUNT(*) FROM artifact_revisions", [], |row| {
                row.get::<_, i64>(0)
            })? as usize;
        let checked_claim_revisions =
            connection.query_row("SELECT COUNT(*) FROM claim_revisions", [], |row| {
                row.get::<_, i64>(0)
            })? as usize;
        let mut issues = Vec::new();
        let integrity: String =
            connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        if integrity != "ok" {
            issues.push(format!("SQLite integrity_check: {integrity}"));
        }
        {
            let mut statement = connection.prepare("PRAGMA foreign_key_check")?;
            let rows = statement.query_map([], |row| {
                Ok(format!(
                    "foreign key violation in {} row {} -> {}",
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?
                ))
            })?;
            for row in rows {
                issues.push(row?);
            }
        }
        let event_count: i64 =
            connection.query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))?;
        let max_event: i64 = connection.query_row(
            "SELECT COALESCE(MAX(commit_seq), 0) FROM events",
            [],
            |row| row.get(0),
        )?;
        if max_event != last_commit_seq || event_count != last_commit_seq {
            issues.push(format!(
                "event sequence is not contiguous: meta={last_commit_seq}, max={max_event}, count={event_count}"
            ));
        }
        let wrong_artifact_heads: i64 = connection.query_row(
            "SELECT COUNT(*) FROM artifacts a
             JOIN artifact_revisions ar ON ar.id = a.current_revision_id
             WHERE ar.artifact_id <> a.id",
            [],
            |row| row.get(0),
        )?;
        let wrong_claim_heads: i64 = connection.query_row(
            "SELECT COUNT(*) FROM claims c
             JOIN claim_revisions cr ON cr.id = c.current_revision_id
             WHERE cr.claim_id <> c.id",
            [],
            |row| row.get(0),
        )?;
        if wrong_artifact_heads > 0 {
            issues.push(format!(
                "{wrong_artifact_heads} artifacts point at another artifact's revision"
            ));
        }
        if wrong_claim_heads > 0 {
            issues.push(format!(
                "{wrong_claim_heads} claims point at another claim's revision"
            ));
        }
        let missing_artifact_fts = connection.query_row(
            "SELECT COUNT(*) FROM artifact_revisions ar
             LEFT JOIN artifact_fts f ON f.revision_id = ar.id
             WHERE f.revision_id IS NULL",
            [],
            |row| row.get::<_, i64>(0),
        )? as usize;
        let missing_claim_fts = connection.query_row(
            "SELECT COUNT(*) FROM claim_revisions cr
             LEFT JOIN claim_fts f ON f.revision_id = cr.id
             WHERE f.revision_id IS NULL",
            [],
            |row| row.get::<_, i64>(0),
        )? as usize;
        if missing_artifact_fts > 0 {
            issues.push(format!(
                "{missing_artifact_fts} artifact revisions missing from FTS"
            ));
        }
        if missing_claim_fts > 0 {
            issues.push(format!(
                "{missing_claim_fts} claim revisions missing from FTS"
            ));
        }
        let artifact_fts_count: i64 =
            connection.query_row("SELECT COUNT(*) FROM artifact_fts", [], |row| row.get(0))?;
        let claim_fts_count: i64 =
            connection.query_row("SELECT COUNT(*) FROM claim_fts", [], |row| row.get(0))?;
        if artifact_fts_count != checked_artifact_revisions as i64 {
            issues.push(format!(
                "artifact FTS row count {artifact_fts_count} differs from revision count {checked_artifact_revisions}"
            ));
        }
        if claim_fts_count != checked_claim_revisions as i64 {
            issues.push(format!(
                "claim FTS row count {claim_fts_count} differs from revision count {checked_claim_revisions}"
            ));
        }
        let broken_relations: i64 = connection.query_row(
            "SELECT COUNT(*) FROM relations r WHERE
                (r.source_type = 'artifact' AND NOT EXISTS(SELECT 1 FROM artifacts a WHERE a.id = r.source_id))
             OR (r.source_type = 'claim' AND NOT EXISTS(SELECT 1 FROM claims c WHERE c.id = r.source_id))
             OR (r.target_type = 'artifact' AND NOT EXISTS(SELECT 1 FROM artifacts a WHERE a.id = r.target_id))
             OR (r.target_type = 'claim' AND NOT EXISTS(SELECT 1 FROM claims c WHERE c.id = r.target_id))",
            [],
            |row| row.get(0),
        )?;
        if broken_relations > 0 {
            issues.push(format!(
                "{broken_relations} relations have missing endpoints"
            ));
        }
        let broken_conflict_cases: i64 = connection.query_row(
            "SELECT COUNT(*) FROM conflict_cases cc
             LEFT JOIN relations r ON r.id = cc.relation_id
             LEFT JOIN claim_revisions sr ON sr.id = cc.source_revision_id
             LEFT JOIN claim_revisions tr ON tr.id = cc.target_revision_id
             WHERE r.id IS NULL OR sr.id IS NULL OR tr.id IS NULL
                OR r.relation <> 'contradicts'
                OR r.source_type <> 'claim' OR r.target_type <> 'claim'
                OR r.source_id <> cc.source_claim_id OR r.target_id <> cc.target_claim_id
                OR sr.claim_id <> cc.source_claim_id OR tr.claim_id <> cc.target_claim_id",
            [],
            |row| row.get(0),
        )?;
        if broken_conflict_cases > 0 {
            issues.push(format!(
                "{broken_conflict_cases} conflict cases have inconsistent relation or revision references"
            ));
        }
        let open_revision_drift: i64 = connection.query_row(
            "SELECT COUNT(*) FROM conflict_cases cc
             JOIN claims sc ON sc.id = cc.source_claim_id
             JOIN claims tc ON tc.id = cc.target_claim_id
             WHERE cc.state = 'open'
               AND (sc.current_revision_id <> cc.source_revision_id
                 OR tc.current_revision_id <> cc.target_revision_id
                 OR sc.status IN ('retracted', 'superseded')
                 OR tc.status IN ('retracted', 'superseded'))",
            [],
            |row| row.get(0),
        )?;
        if open_revision_drift > 0 {
            issues.push(format!(
                "{open_revision_drift} open conflict cases do not match current non-terminal revisions"
            ));
        }
        let missing_live_assessments: i64 = connection.query_row(
            "SELECT COUNT(*) FROM relations r
             JOIN claims sc ON sc.id = r.source_id
             JOIN claims tc ON tc.id = r.target_id
             WHERE r.relation = 'contradicts'
               AND r.source_type = 'claim' AND r.target_type = 'claim'
               AND sc.status IN ('active', 'conflicted')
               AND tc.status IN ('active', 'conflicted')
               AND NOT EXISTS (
                 SELECT 1 FROM conflict_cases cc
                  WHERE cc.relation_id = r.id AND cc.state = 'open'
                    AND cc.source_revision_id = sc.current_revision_id
                    AND cc.target_revision_id = tc.current_revision_id
               )",
            [],
            |row| row.get(0),
        )?;
        if missing_live_assessments > 0 {
            issues.push(format!(
                "{missing_live_assessments} live contradiction relations lack a current open assessment"
            ));
        }
        let missing_conflict_events: i64 = connection.query_row(
            "SELECT COUNT(*) FROM conflict_cases cc
             WHERE NOT EXISTS (
                 SELECT 1 FROM conflict_events ce WHERE ce.case_id = cc.case_id
             )",
            [],
            |row| row.get(0),
        )?;
        if missing_conflict_events > 0 {
            issues.push(format!(
                "{missing_conflict_events} conflict cases have no audit events"
            ));
        }
        let broken_conflict_events: i64 = connection.query_row(
            "SELECT COUNT(*) FROM conflict_events ce
             LEFT JOIN conflict_cases cc ON cc.case_id = ce.case_id
             WHERE cc.case_id IS NULL
                OR ce.source_revision_id <> cc.source_revision_id
                OR ce.target_revision_id <> cc.target_revision_id",
            [],
            |row| row.get(0),
        )?;
        if broken_conflict_events > 0 {
            issues.push(format!(
                "{broken_conflict_events} conflict events do not match their frozen case revisions"
            ));
        }
        let mismatched_conflict_events: i64 = connection.query_row(
            "SELECT COUNT(*) FROM conflict_cases cc
             WHERE NOT EXISTS (
                 SELECT 1 FROM conflict_events ce
                  WHERE ce.case_id = cc.case_id
                    AND ce.event_type = CASE cc.state
                      WHEN 'open' THEN 'opened' ELSE cc.state END
             )",
            [],
            |row| row.get(0),
        )?;
        if mismatched_conflict_events > 0 {
            issues.push(format!(
                "{mismatched_conflict_events} conflict case heads have no matching lifecycle event"
            ));
        }
        let wrong_conflicted_presentation: i64 = connection.query_row(
            "SELECT COUNT(*) FROM claims c
             WHERE c.status IN ('active', 'conflicted')
               AND ((c.status = 'conflicted') <> EXISTS (
                 SELECT 1 FROM conflict_cases cc
                  WHERE cc.state = 'open'
                    AND ((cc.source_claim_id = c.id
                          AND cc.source_revision_id = c.current_revision_id)
                      OR (cc.target_claim_id = c.id
                          AND cc.target_revision_id = c.current_revision_id))
               ))",
            [],
            |row| row.get(0),
        )?;
        if wrong_conflicted_presentation > 0 {
            issues.push(format!(
                "{wrong_conflicted_presentation} claims have stale derived conflict presentation"
            ));
        }
        let evidence_rows: Vec<(String, String)> = {
            let mut statement =
                connection.prepare("SELECT id, evidence_json FROM claim_revisions")?;
            let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
            rows.collect::<std::result::Result<_, _>>()?
        };
        for (claim_revision, evidence_json) in evidence_rows {
            let evidence: Vec<EvidenceLocator> = match serde_json::from_str(&evidence_json) {
                Ok(evidence) => evidence,
                Err(error) => {
                    issues.push(format!(
                        "claim revision {claim_revision} has invalid evidence JSON: {error}"
                    ));
                    continue;
                }
            };
            for locator in evidence {
                let size: Option<i64> = connection
                    .query_row(
                        "SELECT blob_size FROM artifact_revisions
                         WHERE artifact_id = ?1 AND id = ?2",
                        params![locator.artifact_id, locator.revision_id],
                        |row| row.get(0),
                    )
                    .optional()?;
                let Some(size) = size else {
                    issues.push(format!(
                        "claim revision {claim_revision} cites missing {}@{}",
                        locator.artifact_id, locator.revision_id
                    ));
                    continue;
                };
                if locator.start_byte.is_some() != locator.end_byte.is_some()
                    || locator
                        .start_byte
                        .zip(locator.end_byte)
                        .is_some_and(|(start, end)| start >= end || end > size as u64)
                {
                    issues.push(format!(
                        "claim revision {claim_revision} has an invalid evidence span for {}@{}",
                        locator.artifact_id, locator.revision_id
                    ));
                }
            }
        }
        {
            let mut statement = connection
                .prepare("SELECT blob_hash, blob_size, inline_blob FROM artifact_revisions")?;
            let rows = statement.query_map([], |row| {
                Ok(BlobRef {
                    hash: row.get(0)?,
                    size_bytes: row.get::<_, i64>(1)? as u64,
                    inline_bytes: row.get(2)?,
                })
            })?;
            for row in rows {
                let blob = row?;
                if let Err(error) = self.cas.verify(&blob) {
                    issues.push(error.to_string());
                }
            }
        }
        let cas_report = self.cas.verify_all_external()?;
        issues.extend(cas_report.issues);
        Ok(VerifyReport {
            ok: issues.is_empty(),
            schema_version,
            last_commit_seq,
            checked_artifact_revisions,
            checked_claim_revisions,
            checked_external_blobs: cas_report.checked_files,
            issues,
            warnings: Vec::new(),
        })
    }

    pub fn backup_create(&self, destination: impl AsRef<Path>) -> Result<BackupReport> {
        let destination = destination.as_ref();
        let parent = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        ensure_backup_destination_absent(destination)?;

        // The staging directory is deliberately a sibling of the final path:
        // publication is then a single same-filesystem rename. TempDir removes
        // every partial database/blob copy if any step before publication
        // fails.
        let staging = tempfile::Builder::new()
            .prefix(".memoree-backup-stage-")
            .tempdir_in(parent)?;
        let staging_path = staging.path();
        let staged_database_path = staging_path.join(MEMOREE_DATABASE_FILE);
        let staged_blobs_path = staging_path.join("blobs");

        let connection = self.connection.lock();
        let commit_seq: i64 = connection.query_row(
            "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'commit_seq'",
            [],
            |row| row.get(0),
        )?;
        let mut destination_connection = Connection::open(&staged_database_path)?;
        {
            let backup = rusqlite::backup::Backup::new(&connection, &mut destination_connection)?;
            backup.run_to_completion(64, Duration::from_millis(5), None)?;
        }
        drop(destination_connection);

        let copied_external_blobs = self.cas.copy_external_to(&staged_blobs_path)?;

        // Verify through the same Store API a restore will use. This catches a
        // broken SQLite snapshot, missing/replaced external blobs, invalid
        // evidence references, and FTS membership errors before anything is
        // published at the requested destination.
        {
            let staged_store = Store::open(staging_path)?;
            let verification = staged_store.verify()?;
            if !verification.ok {
                return Err(MemoryError::Integrity(format!(
                    "staged backup verification failed: {}",
                    verification.issues.join("; ")
                )));
            }
            if verification.last_commit_seq != commit_seq {
                return Err(MemoryError::Integrity(format!(
                    "staged backup commit sequence {} does not match snapshot sequence {commit_seq}",
                    verification.last_commit_seq
                )));
            }
        }

        // Flush regular files before making the directory visible. Directory
        // fsync is best-effort because not every supported filesystem accepts
        // it; all content file fsync failures remain fatal.
        sync_backup_tree(staging_path)?;
        atomic_publish_backup(staging_path, destination)?;
        sync_directory_best_effort(parent);

        let database_path = destination.join(MEMOREE_DATABASE_FILE);
        let blobs_path = destination.join("blobs");
        Ok(BackupReport {
            destination: destination.display().to_string(),
            database: database_path.display().to_string(),
            blobs: blobs_path.display().to_string(),
            commit_seq,
            copied_external_blobs,
            created_at: Utc::now(),
        })
    }
}

fn preflight_schema_version(connection: &Connection) -> Result<Option<i64>> {
    let object_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;
    let has_meta: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'meta'
         )",
        [],
        |row| row.get(0),
    )?;
    if !has_meta {
        if object_count == 0 {
            return Ok(None);
        }
        return Err(MemoryError::Config(
            "database is non-empty but has no recognized memory schema metadata".into(),
        ));
    }
    let version: String = connection
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .map_err(|error| {
            MemoryError::Config(format!(
                "database schema metadata is missing or unreadable: {error}"
            ))
        })?;
    let version = version.parse::<i64>().map_err(|error| {
        MemoryError::Config(format!("database schema version is invalid: {error}"))
    })?;
    if !(1..=SCHEMA_VERSION).contains(&version) {
        return Err(MemoryError::Config(format!(
            "database schema version {version} is unsupported (expected 1..={SCHEMA_VERSION})"
        )));
    }
    Ok(Some(version))
}

fn migrate_schema(connection: &mut Connection) -> Result<()> {
    let version: i64 = connection.query_row(
        "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'schema_version'",
        [],
        |row| row.get(0),
    )?;
    if version == SCHEMA_VERSION {
        return Ok(());
    }
    if version != 1 {
        return Err(MemoryError::Config(format!(
            "database schema version {version} cannot be migrated to {SCHEMA_VERSION}"
        )));
    }

    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    // v1 contradiction edges did not freeze revisions. Reconstruct the exact
    // assessment at relation creation, then open a fresh current assessment
    // when later revision drift occurred and both endpoints remain live.
    transaction.execute_batch(
        "INSERT INTO conflict_cases (
             case_id, relation_id, source_claim_id, source_revision_id,
             target_claim_id, target_revision_id, state,
             opened_at, updated_at, state_reason, opened_commit_seq
         )
         SELECT 'ccase_migrated_v1_' || r.id, r.id, r.source_id,
                (SELECT cr.id FROM claim_revisions cr
                  WHERE cr.claim_id = r.source_id AND cr.commit_seq <= r.commit_seq
                  ORDER BY cr.commit_seq DESC LIMIT 1),
                r.target_id,
                (SELECT cr.id FROM claim_revisions cr
                  WHERE cr.claim_id = r.target_id AND cr.commit_seq <= r.commit_seq
                  ORDER BY cr.commit_seq DESC LIMIT 1),
                CASE
                  WHEN sc.status IN ('retracted', 'superseded')
                    OR tc.status IN ('retracted', 'superseded') THEN 'resolved'
                  WHEN sc.current_revision_id <> (
                    SELECT cr.id FROM claim_revisions cr
                     WHERE cr.claim_id = r.source_id AND cr.commit_seq <= r.commit_seq
                     ORDER BY cr.commit_seq DESC LIMIT 1)
                    OR tc.current_revision_id <> (
                    SELECT cr.id FROM claim_revisions cr
                     WHERE cr.claim_id = r.target_id AND cr.commit_seq <= r.commit_seq
                     ORDER BY cr.commit_seq DESC LIMIT 1) THEN 'stale'
                  ELSE 'open'
                END,
                r.created_at, r.created_at,
                CASE
                  WHEN sc.status IN ('retracted', 'superseded')
                    OR tc.status IN ('retracted', 'superseded')
                    THEN 'migrated terminal claim lifecycle'
                  WHEN sc.current_revision_id <> (
                    SELECT cr.id FROM claim_revisions cr
                     WHERE cr.claim_id = r.source_id AND cr.commit_seq <= r.commit_seq
                     ORDER BY cr.commit_seq DESC LIMIT 1)
                    OR tc.current_revision_id <> (
                    SELECT cr.id FROM claim_revisions cr
                     WHERE cr.claim_id = r.target_id AND cr.commit_seq <= r.commit_seq
                     ORDER BY cr.commit_seq DESC LIMIT 1)
                    THEN 'migrated revision drift'
                  ELSE NULL
                END,
                r.commit_seq
           FROM relations r
           JOIN claims sc ON sc.id = r.source_id
           JOIN claims tc ON tc.id = r.target_id
          WHERE r.relation = 'contradicts'
            AND r.source_type = 'claim' AND r.target_type = 'claim';

         INSERT INTO conflict_cases (
             case_id, relation_id, source_claim_id, source_revision_id,
             target_claim_id, target_revision_id, state,
             opened_at, updated_at, state_reason, opened_commit_seq
         )
         SELECT 'ccase_migrated_v1_current_' || r.id, r.id,
                r.source_id, sc.current_revision_id,
                r.target_id, tc.current_revision_id, 'open',
                CASE WHEN scr.created_at >= tcr.created_at
                     THEN scr.created_at ELSE tcr.created_at END,
                CASE WHEN scr.created_at >= tcr.created_at
                     THEN scr.created_at ELSE tcr.created_at END,
                'automatically reassessed during schema v1 migration',
                MAX(scr.commit_seq, tcr.commit_seq)
           FROM relations r
           JOIN claims sc ON sc.id = r.source_id
           JOIN claims tc ON tc.id = r.target_id
           JOIN claim_revisions scr ON scr.id = sc.current_revision_id
           JOIN claim_revisions tcr ON tcr.id = tc.current_revision_id
          WHERE r.relation = 'contradicts'
            AND r.source_type = 'claim' AND r.target_type = 'claim'
            AND sc.status NOT IN ('retracted', 'superseded')
            AND tc.status NOT IN ('retracted', 'superseded')
            AND NOT EXISTS (
                SELECT 1 FROM conflict_cases cc
                 WHERE cc.relation_id = r.id AND cc.state = 'open'
                   AND cc.source_revision_id = sc.current_revision_id
                   AND cc.target_revision_id = tc.current_revision_id
            );

         INSERT INTO conflict_events (
             event_id, case_id, event_type, source_revision_id,
             target_revision_id, reason, operation_commit_seq, created_at
         )
         SELECT 'cevt_migrated_' || case_id, case_id,
                CASE state WHEN 'open' THEN 'opened' ELSE state END,
                source_revision_id, target_revision_id,
                CASE state
                  WHEN 'open' THEN 'migrated from schema v1 as open'
                  WHEN 'stale' THEN 'migrated from schema v1 after revision drift'
                  ELSE 'migrated from schema v1 after terminal lifecycle'
                END,
                opened_commit_seq, opened_at
           FROM conflict_cases;

         UPDATE claims
            SET status = CASE
              WHEN EXISTS (
                SELECT 1 FROM conflict_cases cc
                 WHERE cc.state = 'open'
                   AND ((cc.source_claim_id = claims.id
                         AND cc.source_revision_id = claims.current_revision_id)
                     OR (cc.target_claim_id = claims.id
                         AND cc.target_revision_id = claims.current_revision_id))
              ) THEN 'conflicted' ELSE 'active' END
          WHERE status IN ('active', 'conflicted');

         UPDATE meta SET value = '3' WHERE key = 'schema_version';",
    )?;
    transaction.commit()?;
    Ok(())
}

fn migrate_schema_v2_to_v3(connection: &mut Connection) -> Result<()> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(
        "ALTER TABLE conflict_cases RENAME TO conflict_cases_v2;
         ALTER TABLE conflict_events RENAME TO conflict_events_v2;",
    )?;
    transaction.execute_batch(CONFLICT_SCHEMA_V3_TABLES)?;
    transaction.execute_batch(
        "INSERT INTO conflict_cases (
             case_id, relation_id, source_claim_id, source_revision_id,
             target_claim_id, target_revision_id, state,
             opened_at, updated_at, state_reason, opened_commit_seq
         )
         SELECT 'ccase_migrated_v2_' || relation_id, relation_id,
                source_claim_id, source_revision_id,
                target_claim_id, target_revision_id, state,
                opened_at, updated_at, terminal_reason, relation_commit_seq
           FROM conflict_cases_v2;

         INSERT INTO conflict_events (
             event_id, case_id, event_type, source_revision_id,
             target_revision_id, reason, operation_commit_seq, created_at
         )
         SELECT ce.event_id, 'ccase_migrated_v2_' || ce.relation_id,
                ce.event_type, ce.source_revision_id, ce.target_revision_id,
                ce.reason, ce.operation_commit_seq, ce.created_at
           FROM conflict_events_v2 ce;

         INSERT INTO conflict_cases (
             case_id, relation_id, source_claim_id, source_revision_id,
             target_claim_id, target_revision_id, state,
             opened_at, updated_at, state_reason, opened_commit_seq
         )
         SELECT 'ccase_migrated_v2_current_' || r.id, r.id,
                r.source_id, sc.current_revision_id,
                r.target_id, tc.current_revision_id, 'open',
                CASE WHEN scr.created_at >= tcr.created_at
                     THEN scr.created_at ELSE tcr.created_at END,
                CASE WHEN scr.created_at >= tcr.created_at
                     THEN scr.created_at ELSE tcr.created_at END,
                'automatically reassessed during schema v2 migration',
                MAX(scr.commit_seq, tcr.commit_seq)
           FROM relations r
           JOIN claims sc ON sc.id = r.source_id
           JOIN claims tc ON tc.id = r.target_id
           JOIN claim_revisions scr ON scr.id = sc.current_revision_id
           JOIN claim_revisions tcr ON tcr.id = tc.current_revision_id
          WHERE r.relation = 'contradicts'
            AND r.source_type = 'claim' AND r.target_type = 'claim'
            AND sc.status NOT IN ('retracted', 'superseded')
            AND tc.status NOT IN ('retracted', 'superseded')
            AND NOT EXISTS (
                SELECT 1 FROM conflict_cases cc
                 WHERE cc.relation_id = r.id AND cc.state = 'open'
                   AND cc.source_revision_id = sc.current_revision_id
                   AND cc.target_revision_id = tc.current_revision_id
            );

         INSERT INTO conflict_events (
             event_id, case_id, event_type, source_revision_id,
             target_revision_id, reason, operation_commit_seq, created_at
         )
         SELECT 'cevt_migrated_v2_current_' || case_id, case_id, 'opened',
                source_revision_id, target_revision_id,
                'automatically reassessed during schema v2 migration',
                opened_commit_seq, opened_at
           FROM conflict_cases
          WHERE case_id LIKE 'ccase_migrated_v2_current_%';

         UPDATE claims
            SET status = CASE
              WHEN EXISTS (
                SELECT 1 FROM conflict_cases cc
                 WHERE cc.state = 'open'
                   AND ((cc.source_claim_id = claims.id
                         AND cc.source_revision_id = claims.current_revision_id)
                     OR (cc.target_claim_id = claims.id
                         AND cc.target_revision_id = claims.current_revision_id))
              ) THEN 'conflicted' ELSE 'active' END
          WHERE status IN ('active', 'conflicted');

         DROP TABLE conflict_events_v2;
         DROP TABLE conflict_cases_v2;
         UPDATE meta SET value = '3' WHERE key = 'schema_version';",
    )?;
    transaction.commit()?;
    Ok(())
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    let existed = path.try_exists()?;
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if !existed {
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        } else {
            let mode = fs::metadata(path)?.permissions().mode() & 0o777;
            if mode & 0o077 != 0 {
                let is_empty = fs::read_dir(path)?.next().is_none();
                let is_existing_store = path.join(MEMOREE_DATABASE_FILE).is_file();
                if is_empty || is_existing_store {
                    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
                } else {
                    return Err(MemoryError::Config(format!(
                        "refusing to use non-private, non-empty directory {} ({mode:o}); choose a dedicated directory or set it to 700",
                        path.display()
                    )));
                }
            }
        }
    }
    Ok(())
}

fn set_sqlite_file_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let with_suffix = |suffix: &str| {
            let mut value = path.as_os_str().to_owned();
            value.push(suffix);
            PathBuf::from(value)
        };
        for candidate in [path.to_path_buf(), with_suffix("-wal"), with_suffix("-shm")] {
            if candidate.exists() {
                fs::set_permissions(candidate, fs::Permissions::from_mode(0o600))?;
            }
        }
    }
    Ok(())
}

fn ensure_backup_destination_absent(destination: &Path) -> Result<()> {
    match fs::symlink_metadata(destination) {
        Ok(_) => Err(MemoryError::InvalidRequest(format!(
            "backup destination already exists: {}",
            destination.display()
        ))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(MemoryError::Io(error)),
    }
}

fn sync_backup_tree(path: &Path) -> Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            sync_backup_tree(&entry.path())?;
        } else if file_type.is_file() {
            File::open(entry.path())?.sync_all()?;
        } else {
            return Err(MemoryError::Integrity(format!(
                "staged backup contains unsupported filesystem entry: {}",
                entry.path().display()
            )));
        }
    }
    sync_directory_best_effort(path);
    Ok(())
}

fn sync_directory_best_effort(path: &Path) {
    if let Ok(directory) = File::open(path) {
        let _ = directory.sync_all();
    }
}

#[cfg(target_vendor = "apple")]
fn atomic_publish_backup(staging: &Path, destination: &Path) -> Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt as _;

    let staging = CString::new(staging.as_os_str().as_bytes())
        .map_err(|_| MemoryError::Config("backup staging path contains a NUL byte".into()))?;
    let destination_c = CString::new(destination.as_os_str().as_bytes())
        .map_err(|_| MemoryError::Config("backup destination contains a NUL byte".into()))?;
    // SAFETY: Both pointers refer to live, NUL-terminated path buffers for the
    // duration of the call. RENAME_EXCL guarantees the final path is never
    // replaced if another process creates it after our initial check.
    let result = unsafe {
        libc::renameatx_np(
            libc::AT_FDCWD,
            staging.as_ptr(),
            libc::AT_FDCWD,
            destination_c.as_ptr(),
            libc::RENAME_EXCL,
        )
    };
    finish_atomic_publish(result, destination)
}

#[cfg(target_os = "linux")]
fn atomic_publish_backup(staging: &Path, destination: &Path) -> Result<()> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt as _;

    let staging = CString::new(staging.as_os_str().as_bytes())
        .map_err(|_| MemoryError::Config("backup staging path contains a NUL byte".into()))?;
    let destination_c = CString::new(destination.as_os_str().as_bytes())
        .map_err(|_| MemoryError::Config("backup destination contains a NUL byte".into()))?;
    // SAFETY: The path buffers are valid and live for the syscall. renameat2
    // with RENAME_NOREPLACE is an atomic, same-filesystem publication that
    // refuses to overwrite a concurrently-created destination.
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            staging.as_ptr(),
            libc::AT_FDCWD,
            destination_c.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    finish_atomic_publish(result as libc::c_int, destination)
}

#[cfg(not(any(target_vendor = "apple", target_os = "linux")))]
fn atomic_publish_backup(_staging: &Path, _destination: &Path) -> Result<()> {
    // `std::fs::rename` cannot promise an atomic no-replace directory publish
    // on every supported target. Failing closed keeps the backup contract true
    // instead of introducing a destination-replacement race.
    Err(MemoryError::Config(
        "atomic no-replace backup publication is supported only on Apple and Linux targets".into(),
    ))
}

#[cfg(any(target_vendor = "apple", target_os = "linux"))]
fn finish_atomic_publish(result: libc::c_int, destination: &Path) -> Result<()> {
    if result == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.kind() == std::io::ErrorKind::AlreadyExists
        || matches!(
            error.raw_os_error(),
            Some(libc::EEXIST) | Some(libc::ENOTEMPTY)
        )
    {
        Err(MemoryError::Config(format!(
            "backup destination already exists: {}",
            destination.display()
        )))
    } else {
        Err(MemoryError::Io(error))
    }
}

#[derive(Debug)]
struct RawArtifact {
    artifact_id: String,
    revision_id: String,
    revision_number: i64,
    kind: String,
    title: String,
    media_type: String,
    blob: BlobRef,
    provenance_json: String,
    actor: Option<String>,
    status: String,
    context: AmbientContext,
    created_at: DateTime<Utc>,
    revision_created_at: DateTime<Utc>,
    commit_seq: i64,
    forgotten_reason: Option<String>,
}

impl RawArtifact {
    fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            artifact_id: row.get(0)?,
            revision_id: row.get(1)?,
            revision_number: row.get(2)?,
            kind: row.get(3)?,
            title: row.get(4)?,
            media_type: row.get(5)?,
            blob: BlobRef {
                hash: row.get(6)?,
                size_bytes: row.get::<_, i64>(7)? as u64,
                inline_bytes: row.get(8)?,
            },
            provenance_json: row.get(9)?,
            actor: row.get(10)?,
            status: row.get(11)?,
            context: AmbientContext {
                workspace_id: row.get(12)?,
                project_id: row.get(13)?,
                task_id: row.get(14)?,
                component: row.get(15)?,
                pins: Vec::new(),
            },
            created_at: row.get(16)?,
            revision_created_at: row.get(17)?,
            commit_seq: row.get(18)?,
            forgotten_reason: row.get(19)?,
        })
    }

    fn into_record(self, cas: &Cas, include_content: bool) -> Result<ArtifactRecord> {
        let content = if include_content {
            let bytes = cas.get(&self.blob)?;
            Some(
                if self.media_type.to_ascii_lowercase().starts_with("text/") {
                    match String::from_utf8(bytes) {
                        Ok(text) if json_string_encoded_len(&text) <= MAX_ENCODED_CONTENT_BYTES => {
                            ArtifactContent::Text(text)
                        }
                        Ok(text) => ArtifactContent::Base64(
                            base64::engine::general_purpose::STANDARD.encode(text.into_bytes()),
                        ),
                        Err(error) => ArtifactContent::Base64(
                            base64::engine::general_purpose::STANDARD.encode(error.into_bytes()),
                        ),
                    }
                } else {
                    ArtifactContent::Base64(base64::engine::general_purpose::STANDARD.encode(bytes))
                },
            )
        } else {
            None
        };
        Ok(ArtifactRecord {
            artifact_id: self.artifact_id,
            revision_id: self.revision_id,
            revision_number: self.revision_number,
            kind: self.kind,
            title: self.title,
            media_type: self.media_type,
            content,
            blob_hash: self.blob.hash,
            size_bytes: self.blob.size_bytes,
            status: self.status,
            context: self.context,
            provenance: serde_json::from_str(&self.provenance_json)?,
            actor: self.actor,
            created_at: self.created_at,
            revision_created_at: self.revision_created_at,
            commit_seq: self.commit_seq,
            forgotten_reason: self.forgotten_reason,
        })
    }
}

fn artifact_select() -> &'static str {
    "SELECT a.id, ar.id, ar.revision_number, a.kind, ar.title, ar.media_type,
            ar.blob_hash, ar.blob_size, ar.inline_blob, ar.provenance_json,
            ar.actor, a.status, a.workspace_id, a.project_id, a.task_id,
            a.component, a.created_at, ar.created_at, ar.commit_seq,
            a.forgotten_reason
     FROM artifacts a JOIN artifact_revisions ar ON ar.artifact_id = a.id"
}

fn load_artifact_raw(
    connection: &Connection,
    artifact_id: &str,
    revision_id: Option<&str>,
) -> Result<Option<RawArtifact>> {
    let (sql, second): (String, Option<&str>) = match revision_id {
        Some(revision_id) => (
            format!("{} WHERE a.id = ?1 AND ar.id = ?2", artifact_select()),
            Some(revision_id),
        ),
        None => (
            format!(
                "{} WHERE a.id = ?1 AND ar.id = a.current_revision_id",
                artifact_select()
            ),
            None,
        ),
    };
    if let Some(second) = second {
        Ok(connection
            .query_row(&sql, params![artifact_id, second], RawArtifact::from_row)
            .optional()?)
    } else {
        Ok(connection
            .query_row(&sql, [artifact_id], RawArtifact::from_row)
            .optional()?)
    }
}

#[derive(Debug)]
struct RawClaim {
    claim_id: String,
    revision_id: String,
    revision_number: i64,
    claim_type: String,
    status: String,
    statement: String,
    confidence: Option<f64>,
    evidence_json: String,
    valid_from: Option<DateTime<Utc>>,
    valid_until: Option<DateTime<Utc>>,
    context: AmbientContext,
    actor: Option<String>,
    created_at: DateTime<Utc>,
    revision_created_at: DateTime<Utc>,
    commit_seq: i64,
    retraction_reason: Option<String>,
}

impl RawClaim {
    fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            claim_id: row.get(0)?,
            revision_id: row.get(1)?,
            revision_number: row.get(2)?,
            claim_type: row.get(3)?,
            status: row.get(4)?,
            statement: row.get(5)?,
            confidence: row.get(6)?,
            evidence_json: row.get(7)?,
            valid_from: row.get(8)?,
            valid_until: row.get(9)?,
            context: AmbientContext {
                workspace_id: row.get(10)?,
                project_id: row.get(11)?,
                task_id: row.get(12)?,
                component: row.get(13)?,
                pins: Vec::new(),
            },
            actor: row.get(14)?,
            created_at: row.get(15)?,
            revision_created_at: row.get(16)?,
            commit_seq: row.get(17)?,
            retraction_reason: row.get(18)?,
        })
    }

    fn into_record(self) -> Result<ClaimRecord> {
        Ok(ClaimRecord {
            claim_id: self.claim_id,
            revision_id: self.revision_id,
            revision_number: self.revision_number,
            claim_type: parse_enum(&self.claim_type)?,
            status: parse_enum(&self.status)?,
            statement: self.statement,
            confidence: self.confidence,
            evidence: serde_json::from_str(&self.evidence_json)?,
            valid_from: self.valid_from,
            valid_until: self.valid_until,
            context: self.context,
            actor: self.actor,
            created_at: self.created_at,
            revision_created_at: self.revision_created_at,
            commit_seq: self.commit_seq,
            retraction_reason: self.retraction_reason,
        })
    }
}

fn claim_select() -> &'static str {
    "SELECT c.id, cr.id, cr.revision_number, c.claim_type, c.status,
            cr.statement, cr.confidence, cr.evidence_json, c.valid_from,
            c.valid_until, c.workspace_id, c.project_id, c.task_id, c.component,
            cr.actor, c.created_at, cr.created_at, cr.commit_seq,
            c.retraction_reason
     FROM claims c JOIN claim_revisions cr ON cr.claim_id = c.id"
}

fn load_claim_raw(
    connection: &Connection,
    claim_id: &str,
    revision_id: Option<&str>,
) -> Result<Option<RawClaim>> {
    let (sql, second): (String, Option<&str>) = match revision_id {
        Some(revision_id) => (
            format!("{} WHERE c.id = ?1 AND cr.id = ?2", claim_select()),
            Some(revision_id),
        ),
        None => (
            format!(
                "{} WHERE c.id = ?1 AND cr.id = c.current_revision_id",
                claim_select()
            ),
            None,
        ),
    };
    if let Some(second) = second {
        Ok(connection
            .query_row(&sql, params![claim_id, second], RawClaim::from_row)
            .optional()?)
    } else {
        Ok(connection
            .query_row(&sql, [claim_id], RawClaim::from_row)
            .optional()?)
    }
}

#[derive(Debug)]
struct RawRelation {
    relation_id: String,
    source_type: String,
    source_id: String,
    relation: String,
    target_type: String,
    target_id: String,
    metadata_json: String,
    context: AmbientContext,
    created_at: DateTime<Utc>,
    commit_seq: i64,
}

impl RawRelation {
    fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            relation_id: row.get(0)?,
            source_type: row.get(1)?,
            source_id: row.get(2)?,
            relation: row.get(3)?,
            target_type: row.get(4)?,
            target_id: row.get(5)?,
            metadata_json: row.get(6)?,
            context: AmbientContext {
                workspace_id: row.get(7)?,
                project_id: row.get(8)?,
                task_id: row.get(9)?,
                component: row.get(10)?,
                pins: Vec::new(),
            },
            created_at: row.get(11)?,
            commit_seq: row.get(12)?,
        })
    }

    fn into_record(self) -> Result<RelationRecord> {
        Ok(RelationRecord {
            relation_id: self.relation_id,
            source_type: parse_enum(&self.source_type)?,
            source_id: self.source_id,
            relation: parse_enum(&self.relation)?,
            target_type: parse_enum(&self.target_type)?,
            target_id: self.target_id,
            metadata: serde_json::from_str(&self.metadata_json)?,
            context: self.context,
            created_at: self.created_at,
            commit_seq: self.commit_seq,
        })
    }
}

#[derive(Debug)]
struct RawConflictCase {
    case_id: String,
    relation_id: String,
    source_claim_id: String,
    source_revision_id: String,
    target_claim_id: String,
    target_revision_id: String,
    state: String,
    opened_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    state_reason: Option<String>,
    opened_commit_seq: i64,
    case_sequence: i64,
    metadata_json: String,
    context: AmbientContext,
}

impl RawConflictCase {
    fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            case_id: row.get(0)?,
            relation_id: row.get(1)?,
            source_claim_id: row.get(2)?,
            source_revision_id: row.get(3)?,
            target_claim_id: row.get(4)?,
            target_revision_id: row.get(5)?,
            state: row.get(6)?,
            opened_at: row.get(7)?,
            updated_at: row.get(8)?,
            state_reason: row.get(9)?,
            opened_commit_seq: row.get(10)?,
            case_sequence: row.get(11)?,
            metadata_json: row.get(12)?,
            context: AmbientContext {
                workspace_id: row.get(13)?,
                project_id: row.get(14)?,
                task_id: row.get(15)?,
                component: row.get(16)?,
                pins: Vec::new(),
            },
        })
    }

    fn into_item(self, connection: &Connection) -> Result<ConflictListItem> {
        let source_frozen = load_claim_raw(
            connection,
            &self.source_claim_id,
            Some(&self.source_revision_id),
        )?
        .ok_or_else(|| {
            MemoryError::Config(format!(
                "conflict {} has missing source revision {}",
                self.relation_id, self.source_revision_id
            ))
        })?
        .into_record()?;
        let source_current = load_claim_raw(connection, &self.source_claim_id, None)?
            .ok_or_else(|| {
                MemoryError::Config(format!(
                    "conflict {} has missing source claim {}",
                    self.relation_id, self.source_claim_id
                ))
            })?
            .into_record()?;
        let target_frozen = load_claim_raw(
            connection,
            &self.target_claim_id,
            Some(&self.target_revision_id),
        )?
        .ok_or_else(|| {
            MemoryError::Config(format!(
                "conflict {} has missing target revision {}",
                self.relation_id, self.target_revision_id
            ))
        })?
        .into_record()?;
        let target_current = load_claim_raw(connection, &self.target_claim_id, None)?
            .ok_or_else(|| {
                MemoryError::Config(format!(
                    "conflict {} has missing target claim {}",
                    self.relation_id, self.target_claim_id
                ))
            })?
            .into_record()?;

        Ok(ConflictListItem {
            case_id: self.case_id,
            relation_id: self.relation_id,
            state: parse_enum(&self.state)?,
            source: ConflictClaimSnapshot {
                claim_id: self.source_claim_id,
                frozen_is_current: source_frozen.revision_id == source_current.revision_id,
                frozen: source_frozen,
                current: source_current,
            },
            target: ConflictClaimSnapshot {
                claim_id: self.target_claim_id,
                frozen_is_current: target_frozen.revision_id == target_current.revision_id,
                frozen: target_frozen,
                current: target_current,
            },
            metadata: serde_json::from_str(&self.metadata_json)?,
            context: self.context,
            opened_at: self.opened_at,
            updated_at: self.updated_at,
            state_reason: self.state_reason,
            opened_commit_seq: self.opened_commit_seq,
            case_sequence: self.case_sequence,
        })
    }
}

fn load_relation_by_edge(
    connection: &Connection,
    source_type: &str,
    source_id: &str,
    relation: &str,
    target_type: &str,
    target_id: &str,
) -> Result<Option<RawRelation>> {
    Ok(connection
        .query_row(
            "SELECT id, source_type, source_id, relation, target_type, target_id,
                    metadata_json, workspace_id, project_id, task_id, component,
                    created_at, commit_seq
             FROM relations
             WHERE source_type = ?1 AND source_id = ?2 AND relation = ?3
               AND target_type = ?4 AND target_id = ?5",
            params![source_type, source_id, relation, target_type, target_id],
            RawRelation::from_row,
        )
        .optional()?)
}

struct ArtifactSearchRow {
    id: String,
    revision_id: String,
    title: String,
    excerpt: String,
    status: String,
    context: AmbientContext,
    kind: String,
    provenance_json: String,
    revision_created_at: DateTime<Utc>,
    is_current_revision: bool,
    rank: f64,
}

fn search_artifact_row(row: &Row<'_>) -> rusqlite::Result<ArtifactSearchRow> {
    Ok(ArtifactSearchRow {
        id: row.get(0)?,
        revision_id: row.get(1)?,
        title: row.get(2)?,
        excerpt: row.get(3)?,
        status: row.get(4)?,
        context: AmbientContext {
            workspace_id: row.get(5)?,
            project_id: row.get(6)?,
            task_id: row.get(7)?,
            component: row.get(8)?,
            pins: Vec::new(),
        },
        kind: row.get(9)?,
        provenance_json: row.get(10)?,
        revision_created_at: row.get(11)?,
        is_current_revision: row.get(12)?,
        rank: row.get(13)?,
    })
}

impl ArtifactSearchRow {
    fn into_candidate(self) -> Result<SearchCandidate> {
        let profile = artifact_recency_profile(&self.kind);
        let lexical_score = normalized_rank(self.rank);
        Ok(SearchCandidate {
            hit: SearchHit {
                entity_type: EntityType::Artifact,
                entity_id: self.id.clone(),
                revision_id: self.revision_id.clone(),
                status: self.status.clone(),
                title: self.title,
                excerpt: self.excerpt,
                citation: format!("memoree://artifact/{}@{}", self.id, self.revision_id),
                context: self.context,
                score: lexical_score,
                ranking: placeholder_ranking(
                    lexical_score,
                    self.revision_created_at,
                    RecencyTimestampBasis::RevisionCreatedAt,
                    profile.class,
                ),
                matched_by: vec!["fts5_bm25".into()],
                provenance: serde_json::from_str(&self.provenance_json)?,
            },
            effective_at: self.revision_created_at,
            effective_at_basis: RecencyTimestampBasis::RevisionCreatedAt,
            profile,
            recency_eligible: self.status == "active" && self.is_current_revision,
        })
    }
}

struct ClaimSearchRow {
    id: String,
    revision_id: String,
    claim_type: String,
    statement: String,
    status: String,
    context: AmbientContext,
    evidence_json: String,
    confidence: Option<f64>,
    valid_from: Option<DateTime<Utc>>,
    valid_until: Option<DateTime<Utc>>,
    is_current_revision: bool,
    evaluated_at: DateTime<Utc>,
    temporal_state: String,
    revision_created_at: DateTime<Utc>,
    rank: f64,
}

fn search_claim_row(row: &Row<'_>) -> rusqlite::Result<ClaimSearchRow> {
    Ok(ClaimSearchRow {
        id: row.get(0)?,
        revision_id: row.get(1)?,
        claim_type: row.get(2)?,
        statement: row.get(3)?,
        status: row.get(4)?,
        context: AmbientContext {
            workspace_id: row.get(5)?,
            project_id: row.get(6)?,
            task_id: row.get(7)?,
            component: row.get(8)?,
            pins: Vec::new(),
        },
        evidence_json: row.get(9)?,
        confidence: row.get(10)?,
        valid_from: row.get(11)?,
        valid_until: row.get(12)?,
        is_current_revision: row.get(13)?,
        evaluated_at: row.get(14)?,
        temporal_state: row.get(15)?,
        revision_created_at: row.get(16)?,
        rank: row.get(17)?,
    })
}

impl ClaimSearchRow {
    fn into_candidate(self) -> Result<SearchCandidate> {
        let evidence: Vec<EvidenceLocator> = serde_json::from_str(&self.evidence_json)?;
        let mut provenance = BTreeMap::new();
        provenance.insert("claim_type".into(), Value::String(self.claim_type.clone()));
        provenance.insert("evidence".into(), serde_json::to_value(evidence)?);
        if let Some(confidence) = self.confidence {
            provenance.insert("confidence".into(), json!(confidence));
        }
        provenance.insert("valid_from".into(), serde_json::to_value(self.valid_from)?);
        provenance.insert(
            "valid_until".into(),
            serde_json::to_value(self.valid_until)?,
        );
        provenance.insert("evaluated_at".into(), json!(self.evaluated_at));
        provenance.insert(
            "temporal_state".into(),
            Value::String(self.temporal_state.clone()),
        );
        provenance.insert(
            "is_current_revision".into(),
            Value::Bool(self.is_current_revision),
        );
        provenance.insert(
            "is_current".into(),
            Value::Bool(
                self.is_current_revision
                    && matches!(self.status.as_str(), "active" | "conflicted")
                    && self.temporal_state == "current",
            ),
        );
        let is_current = self.is_current_revision
            && matches!(self.status.as_str(), "active" | "conflicted")
            && self.temporal_state == "current";
        let (effective_at, effective_at_basis) = self
            .valid_from
            .map(|value| (value, RecencyTimestampBasis::ValidFrom))
            .unwrap_or((
                self.revision_created_at,
                RecencyTimestampBasis::RevisionCreatedAt,
            ));
        let profile = claim_recency_profile(&self.claim_type);
        let lexical_score = normalized_rank(self.rank);
        Ok(SearchCandidate {
            hit: SearchHit {
                entity_type: EntityType::Claim,
                entity_id: self.id.clone(),
                revision_id: self.revision_id.clone(),
                status: self.status,
                title: format!("{} claim", self.claim_type),
                excerpt: self.statement,
                citation: format!("memoree://claim/{}@{}", self.id, self.revision_id),
                context: self.context,
                score: lexical_score,
                ranking: placeholder_ranking(
                    lexical_score,
                    effective_at,
                    effective_at_basis,
                    profile.class,
                ),
                matched_by: vec!["fts5_bm25".into()],
                provenance,
            },
            effective_at,
            effective_at_basis,
            profile,
            recency_eligible: is_current,
        })
    }
}

#[derive(Clone)]
struct SearchCandidate {
    hit: SearchHit,
    effective_at: DateTime<Utc>,
    effective_at_basis: RecencyTimestampBasis,
    profile: RecencyProfile,
    recency_eligible: bool,
}

#[derive(Clone, Copy)]
struct RecencyProfile {
    class: RecencyDecayClass,
    half_life_days: f64,
    max_bonus_ratio: f64,
}

fn placeholder_ranking(
    lexical_score: f64,
    effective_at: DateTime<Utc>,
    effective_at_basis: RecencyTimestampBasis,
    decay_class: RecencyDecayClass,
) -> SearchRanking {
    SearchRanking {
        policy_version: RECENCY_POLICY_VERSION.into(),
        recency_enabled: false,
        recency_eligible: false,
        lexical_score,
        recency_bonus: 0.0,
        lexical_position: 0,
        final_position: 0,
        max_promotion: RECENCY_MAX_PROMOTION,
        effective_at,
        effective_at_basis,
        evaluated_at: effective_at,
        decay_class,
    }
}

fn rerank_with_recency(
    mut candidates: Vec<SearchCandidate>,
    enabled: bool,
    evaluated_at: DateTime<Utc>,
) -> Vec<SearchHit> {
    for (index, candidate) in candidates.iter_mut().enumerate() {
        let eligible = candidate.recency_eligible && candidate.effective_at <= evaluated_at;
        let lexical_score = candidate.hit.score;
        let recency_bonus = if enabled && eligible {
            let age_seconds = evaluated_at
                .signed_duration_since(candidate.effective_at)
                .num_seconds()
                .max(0) as f64;
            let age_days = age_seconds / 86_400.0;
            let freshness = 2.0_f64.powf(-age_days / candidate.profile.half_life_days);
            lexical_score * candidate.profile.max_bonus_ratio * freshness
        } else {
            0.0
        };
        candidate.hit.score = lexical_score + recency_bonus;
        candidate.hit.ranking = SearchRanking {
            policy_version: RECENCY_POLICY_VERSION.into(),
            recency_enabled: enabled,
            recency_eligible: eligible,
            lexical_score,
            recency_bonus,
            lexical_position: index + 1,
            final_position: index + 1,
            max_promotion: RECENCY_MAX_PROMOTION,
            effective_at: candidate.effective_at,
            effective_at_basis: candidate.effective_at_basis,
            evaluated_at,
            decay_class: candidate.profile.class,
        };
        if enabled {
            candidate.hit.matched_by.push(RECENCY_POLICY_VERSION.into());
        }
    }

    if enabled {
        let mut reranked: Vec<SearchCandidate> = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let lexical_index = candidate.hit.ranking.lexical_position - 1;
            let promotion_floor = lexical_index.saturating_sub(RECENCY_MAX_PROMOTION);
            let mut insertion_index = reranked.len();
            while insertion_index > promotion_floor
                && candidate.hit.score > reranked[insertion_index - 1].hit.score
            {
                insertion_index -= 1;
            }
            reranked.insert(insertion_index, candidate);
        }
        candidates = reranked;
    }

    candidates
        .into_iter()
        .enumerate()
        .map(|(index, mut candidate)| {
            candidate.hit.ranking.final_position = index + 1;
            candidate.hit
        })
        .collect()
}

fn artifact_recency_profile(kind: &str) -> RecencyProfile {
    let kind = kind.to_ascii_lowercase();
    if ["log", "session", "trace", "observation"]
        .iter()
        .any(|token| kind.contains(token))
    {
        RecencyProfile {
            class: RecencyDecayClass::Ephemeral,
            half_life_days: 30.0,
            max_bonus_ratio: 0.10,
        }
    } else if ["decision", "adr"].iter().any(|token| kind.contains(token)) {
        RecencyProfile {
            class: RecencyDecayClass::Decision,
            half_life_days: 730.0,
            max_bonus_ratio: 0.025,
        }
    } else if ["constraint", "policy", "spec"]
        .iter()
        .any(|token| kind.contains(token))
    {
        RecencyProfile {
            class: RecencyDecayClass::Constraint,
            half_life_days: 3_650.0,
            max_bonus_ratio: 0.01,
        }
    } else if ["procedure", "runbook"]
        .iter()
        .any(|token| kind.contains(token))
    {
        RecencyProfile {
            class: RecencyDecayClass::Procedure,
            half_life_days: 365.0,
            max_bonus_ratio: 0.04,
        }
    } else {
        RecencyProfile {
            class: RecencyDecayClass::General,
            half_life_days: 180.0,
            max_bonus_ratio: 0.05,
        }
    }
}

fn claim_recency_profile(claim_type: &str) -> RecencyProfile {
    match claim_type {
        "observation" => RecencyProfile {
            class: RecencyDecayClass::Observation,
            half_life_days: 30.0,
            max_bonus_ratio: 0.10,
        },
        "preference" => RecencyProfile {
            class: RecencyDecayClass::Preference,
            half_life_days: 180.0,
            max_bonus_ratio: 0.06,
        },
        "procedure" => RecencyProfile {
            class: RecencyDecayClass::Procedure,
            half_life_days: 365.0,
            max_bonus_ratio: 0.04,
        },
        "decision" => RecencyProfile {
            class: RecencyDecayClass::Decision,
            half_life_days: 730.0,
            max_bonus_ratio: 0.025,
        },
        "constraint" => RecencyProfile {
            class: RecencyDecayClass::Constraint,
            half_life_days: 3_650.0,
            max_bonus_ratio: 0.01,
        },
        _ => RecencyProfile {
            class: RecencyDecayClass::General,
            half_life_days: 180.0,
            max_bonus_ratio: 0.05,
        },
    }
}

fn normalized_rank(rank: f64) -> f64 {
    // FTS5's bm25() returns better matches as more-negative values.
    if rank.is_finite() {
        (-rank).max(0.0)
    } else {
        0.0
    }
}

fn horizon_filter_sql(alias: &str, include_pins: bool) -> String {
    let pins = if include_pins {
        format!(" OR {alias}.id IN (SELECT value FROM json_each(?7))")
    } else {
        String::new()
    };
    format!(
        "(
          ?3 = 'personal'
          OR (?3 = 'workspace' AND {alias}.workspace_id = ?4)
          OR (?3 = 'ambient' AND
              {alias}.workspace_id = ?4 AND {alias}.project_id = ?5
              AND (?6 IS NULL OR {alias}.task_id IS NULL OR {alias}.task_id = ?6))
          {pins}
        )"
    )
}

fn normalized_artifact_pins(pins: &[String]) -> (Vec<String>, Vec<String>) {
    let mut artifacts = Vec::new();
    let mut exact_revisions = Vec::new();
    for pin in pins {
        let value = pin
            .strip_prefix("memoree://artifact/")
            .unwrap_or(pin.as_str());
        let (artifact_id, revision_id) = value
            .split_once('@')
            .map_or((value, None), |(artifact, revision)| {
                (artifact, Some(revision))
            });
        match revision_id {
            Some(revision_id) if !artifact_id.is_empty() && !revision_id.is_empty() => {
                exact_revisions.push(format!("{artifact_id}@{revision_id}"));
            }
            None if !artifact_id.is_empty() => artifacts.push(artifact_id.to_owned()),
            _ => {}
        }
    }
    artifacts.sort();
    artifacts.dedup();
    exact_revisions.sort();
    exact_revisions.dedup();
    (artifacts, exact_revisions)
}

fn fts_query(query: &str) -> Result<String> {
    if query.len() > MAX_QUERY_BYTES {
        return Err(MemoryError::InvalidRequest(format!(
            "search query must not exceed {MAX_QUERY_BYTES} bytes"
        )));
    }
    let mut tokens = Vec::new();
    let mut current = String::new();
    for character in query.chars() {
        if character.is_alphanumeric() || character == '_' {
            current.push(character);
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    if tokens.is_empty() {
        return Err(MemoryError::InvalidRequest(
            "search query must contain at least one word or identifier".into(),
        ));
    }
    if tokens.len() > 48 {
        return Err(MemoryError::InvalidRequest(
            "search query must not contain more than 48 words or identifiers".into(),
        ));
    }
    Ok(tokens
        .into_iter()
        .map(|token| format!("\"{}\"", token.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" OR "))
}

fn idempotency_replay<T: DeserializeOwned>(
    transaction: &Transaction<'_>,
    key: Option<&str>,
    request_hash: &str,
    operation: &str,
) -> Result<Option<MutationResult<T>>> {
    let Some(key) = key else {
        return Ok(None);
    };
    require_bounded("idempotency_key", key, 1024)?;
    let existing: Option<(String, String, String, i64)> = transaction
        .query_row(
            "SELECT request_hash, operation, response_json, commit_seq
             FROM idempotency WHERE key = ?1",
            [key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((existing_hash, existing_operation, response, commit_seq)) = existing else {
        return Ok(None);
    };
    if existing_hash != request_hash || existing_operation != operation {
        return Err(MemoryError::IdempotencyConflict(key.to_owned()));
    }
    Ok(Some(MutationResult {
        value: serde_json::from_str(&response)?,
        commit_seq,
        created: false,
    }))
}

fn record_idempotency<T: Serialize>(
    transaction: &Transaction<'_>,
    key: Option<&str>,
    request_hash: &str,
    operation: &str,
    response: &T,
    commit_seq: i64,
) -> Result<()> {
    let Some(key) = key else {
        return Ok(());
    };
    transaction.execute(
        "INSERT INTO idempotency (
            key, request_hash, operation, response_json, commit_seq, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            key,
            request_hash,
            operation,
            serde_json::to_string(response)?,
            commit_seq,
            Utc::now(),
        ],
    )?;
    Ok(())
}

fn next_commit_seq(transaction: &Transaction<'_>) -> Result<i64> {
    Ok(transaction.query_row(
        "UPDATE meta
         SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT)
         WHERE key = 'commit_seq'
         RETURNING CAST(value AS INTEGER)",
        [],
        |row| row.get(0),
    )?)
}

#[allow(clippy::too_many_arguments)]
fn append_event(
    transaction: &Transaction<'_>,
    commit_seq: i64,
    event_type: &str,
    entity_type: &str,
    entity_id: &str,
    revision_id: Option<&str>,
    actor: Option<&str>,
    payload: &Value,
) -> Result<()> {
    transaction.execute(
        "INSERT INTO events (
            commit_seq, event_id, event_type, entity_type, entity_id,
            revision_id, actor, payload_json, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            commit_seq,
            new_id("evt"),
            event_type,
            entity_type,
            entity_id,
            revision_id,
            actor,
            serde_json::to_string(payload)?,
            Utc::now(),
        ],
    )?;
    Ok(())
}

fn new_id(prefix: &str) -> String {
    format!("{prefix}_{}", Ulid::r#gen())
}

fn enum_string<T: Serialize>(value: &T) -> Result<String> {
    serde_json::to_value(value)?
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| MemoryError::Integrity("enum did not serialize as a string".into()))
}

fn parse_enum<T: DeserializeOwned>(value: &str) -> Result<T> {
    Ok(serde_json::from_value(Value::String(value.to_owned()))?)
}

fn require_nonempty(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        Err(MemoryError::InvalidRequest(format!(
            "{field} must not be empty"
        )))
    } else {
        Ok(())
    }
}

fn require_bounded(field: &str, value: &str, max_bytes: usize) -> Result<()> {
    require_nonempty(field, value)?;
    if value.len() > max_bytes {
        return Err(MemoryError::InvalidRequest(format!(
            "{field} must not exceed {max_bytes} bytes"
        )));
    }
    Ok(())
}

fn validate_optional_size(field: &str, value: Option<&str>, max_bytes: usize) -> Result<()> {
    if let Some(value) = value {
        require_bounded(field, value, max_bytes)?;
    }
    Ok(())
}

fn validate_serialized_size<T: Serialize>(field: &str, value: &T, max_bytes: usize) -> Result<()> {
    if serde_json::to_vec(value)?.len() > max_bytes {
        return Err(MemoryError::InvalidRequest(format!(
            "{field} must not exceed {max_bytes} encoded bytes"
        )));
    }
    Ok(())
}

fn validate_context(context: &AmbientContext) -> Result<()> {
    require_bounded("workspace_id", &context.workspace_id, MAX_CONTEXT_ID_BYTES)?;
    require_bounded("project_id", &context.project_id, MAX_CONTEXT_ID_BYTES)?;
    validate_optional_size("task_id", context.task_id.as_deref(), MAX_CONTEXT_ID_BYTES)?;
    validate_optional_size(
        "component",
        context.component.as_deref(),
        MAX_CONTEXT_ID_BYTES,
    )?;
    if context.pins.len() > MAX_CONTEXT_PINS {
        return Err(MemoryError::InvalidRequest(format!(
            "pins must not contain more than {MAX_CONTEXT_PINS} entries"
        )));
    }
    for pin in &context.pins {
        require_bounded("pin", pin, MAX_PIN_BYTES)?;
    }
    Ok(())
}

fn validate_artifact_input(kind: &str, title: &str, media_type: &str) -> Result<()> {
    require_bounded("kind", kind, MAX_KIND_BYTES)?;
    require_bounded("title", title, MAX_TITLE_BYTES)?;
    require_bounded("media_type", media_type, MAX_MEDIA_TYPE_BYTES)?;
    if !media_type.contains('/') {
        return Err(MemoryError::InvalidRequest(format!(
            "media_type {media_type:?} is not a MIME type"
        )));
    }
    Ok(())
}

fn content_bytes(content: &ArtifactContent) -> Result<Vec<u8>> {
    let bytes = match content {
        ArtifactContent::Text(text) => text.as_bytes().to_vec(),
        ArtifactContent::Base64(encoded) => base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|error| {
                MemoryError::InvalidRequest(format!("invalid base64 content: {error}"))
            })?,
    };
    if bytes.len() > MAX_ARTIFACT_BYTES {
        return Err(MemoryError::ContentTooLarge);
    }
    Ok(bytes)
}

fn json_string_encoded_len(value: &str) -> usize {
    value.chars().fold(2usize, |length, character| {
        length.saturating_add(match character {
            '"' | '\\' | '\u{0008}' | '\t' | '\n' | '\u{000c}' | '\r' => 2,
            '\u{0000}'..='\u{001f}' => 6,
            _ => character.len_utf8(),
        })
    })
}

fn searchable_text(media_type: &str, bytes: &[u8]) -> String {
    let media_type = media_type
        .split(';')
        .next()
        .unwrap_or(media_type)
        .trim()
        .to_ascii_lowercase();
    let textual = media_type.starts_with("text/")
        || media_type.ends_with("+json")
        || media_type.ends_with("+xml")
        || matches!(
            media_type.as_str(),
            "application/json"
                | "application/xml"
                | "application/yaml"
                | "application/x-yaml"
                | "application/toml"
                | "application/javascript"
                | "application/sql"
                | "application/graphql"
                | "image/svg+xml"
        );
    if textual {
        String::from_utf8_lossy(bytes).into_owned()
    } else {
        String::new()
    }
}

fn validate_claim(
    statement: &str,
    confidence: Option<f64>,
    valid_from: Option<DateTime<Utc>>,
    valid_until: Option<DateTime<Utc>>,
) -> Result<()> {
    require_bounded("statement", statement, MAX_CLAIM_STATEMENT_BYTES)?;
    if let Some(confidence) = confidence
        && (!confidence.is_finite() || !(0.0..=1.0).contains(&confidence))
    {
        return Err(MemoryError::InvalidRequest(
            "confidence must be between 0 and 1".into(),
        ));
    }
    if let (Some(from), Some(until)) = (valid_from, valid_until)
        && from >= until
    {
        return Err(MemoryError::InvalidRequest(
            "valid_from must be before valid_until".into(),
        ));
    }
    Ok(())
}

fn validate_evidence(transaction: &Transaction<'_>, evidence: &[EvidenceLocator]) -> Result<()> {
    if evidence.len() > MAX_EVIDENCE_ITEMS {
        return Err(MemoryError::InvalidRequest(format!(
            "evidence must not contain more than {MAX_EVIDENCE_ITEMS} entries"
        )));
    }
    for locator in evidence {
        require_bounded(
            "evidence.artifact_id",
            &locator.artifact_id,
            MAX_CONTEXT_ID_BYTES,
        )?;
        require_bounded(
            "evidence.revision_id",
            &locator.revision_id,
            MAX_CONTEXT_ID_BYTES,
        )?;
        let size: Option<i64> = transaction
            .query_row(
                "SELECT blob_size FROM artifact_revisions
                 WHERE artifact_id = ?1 AND id = ?2",
                params![locator.artifact_id, locator.revision_id],
                |row| row.get(0),
            )
            .optional()?;
        let size = size.ok_or_else(|| {
            MemoryError::NotFound(format!(
                "evidence {} revision {}",
                locator.artifact_id, locator.revision_id
            ))
        })? as u64;
        if locator.start_byte.is_some() != locator.end_byte.is_some() {
            return Err(MemoryError::InvalidRequest(
                "evidence byte spans require both start_byte and end_byte".into(),
            ));
        }
        if let (Some(start), Some(end)) = (locator.start_byte, locator.end_byte)
            && (start >= end || end > size)
        {
            return Err(MemoryError::InvalidRequest(format!(
                "evidence span {start}..{end} is outside artifact size {size}"
            )));
        }
    }
    Ok(())
}

fn ensure_entity_in_write_scope(
    transaction: &Transaction<'_>,
    ambient: &AmbientContext,
    entity_type: EntityType,
    id: &str,
) -> Result<()> {
    let (table, kind) = match entity_type {
        EntityType::Artifact => ("artifacts", "artifact"),
        EntityType::Claim => ("claims", "claim"),
    };
    let owner = transaction
        .query_row(
            &format!(
                "SELECT workspace_id, project_id, task_id, component FROM {table} WHERE id = ?1"
            ),
            [id],
            |row| {
                Ok(AmbientContext {
                    workspace_id: row.get(0)?,
                    project_id: row.get(1)?,
                    task_id: row.get(2)?,
                    component: row.get(3)?,
                    pins: Vec::new(),
                })
            },
        )
        .optional()?
        .ok_or_else(|| MemoryError::NotFound(format!("{kind} {id}")))?;
    ensure_write_scope(ambient, &owner, kind, id)
}

fn ensure_entity_in_read_scope(
    connection: &Connection,
    ambient: &AmbientContext,
    entity_type: EntityType,
    id: &str,
    horizon: Horizon,
) -> Result<()> {
    let (table, kind) = match entity_type {
        EntityType::Artifact => ("artifacts", "artifact"),
        EntityType::Claim => ("claims", "claim"),
    };
    let owner = connection
        .query_row(
            &format!(
                "SELECT workspace_id, project_id, task_id, component FROM {table} WHERE id = ?1"
            ),
            [id],
            |row| {
                Ok(AmbientContext {
                    workspace_id: row.get(0)?,
                    project_id: row.get(1)?,
                    task_id: row.get(2)?,
                    component: row.get(3)?,
                    pins: Vec::new(),
                })
            },
        )
        .optional()?
        .ok_or_else(|| MemoryError::NotFound(format!("{kind} {id}")))?;

    let horizon_visible = match horizon {
        Horizon::Ambient => {
            let same_project = ambient.workspace_id == owner.workspace_id
                && ambient.project_id == owner.project_id;
            let task_visible = match ambient.task_id.as_deref() {
                None => true,
                Some(task_id) => {
                    owner.task_id.is_none() || owner.task_id.as_deref() == Some(task_id)
                }
            };
            same_project && task_visible
        }
        Horizon::Workspace => ambient.workspace_id == owner.workspace_id,
        Horizon::Personal => true,
    };
    let explicitly_pinned = matches!(entity_type, EntityType::Artifact)
        && ambient.pins.iter().any(|pin| {
            let value = pin.strip_prefix("memoree://artifact/").unwrap_or(pin);
            value
                .split_once('@')
                .map_or(value, |(artifact, _)| artifact)
                == id
        });
    if horizon_visible || explicitly_pinned {
        return Ok(());
    }

    Err(MemoryError::ScopeViolation(format!(
        "{kind} {id} belongs to workspace={}/project={}/task={}; {:?} relation-read scope starts at workspace={}/project={}/task={}",
        owner.workspace_id,
        owner.project_id,
        owner.task_id.as_deref().unwrap_or("<project>"),
        horizon,
        ambient.workspace_id,
        ambient.project_id,
        ambient.task_id.as_deref().unwrap_or("<project>"),
    )))
}

/// Mutations use the ambient project/task boundary and deliberately ignore
/// pins. Pins and exact gets can make an entity readable, but never writable.
fn ensure_write_scope(
    ambient: &AmbientContext,
    owner: &AmbientContext,
    entity_kind: &str,
    entity_id: &str,
) -> Result<()> {
    let same_project =
        ambient.workspace_id == owner.workspace_id && ambient.project_id == owner.project_id;
    let task_visible = match ambient.task_id.as_deref() {
        None => true,
        Some(task_id) => owner.task_id.is_none() || owner.task_id.as_deref() == Some(task_id),
    };
    if same_project && task_visible {
        return Ok(());
    }

    Err(MemoryError::ScopeViolation(format!(
        "{entity_kind} {entity_id} belongs to workspace={}/project={}/task={}; ambient write scope is workspace={}/project={}/task={}",
        owner.workspace_id,
        owner.project_id,
        owner.task_id.as_deref().unwrap_or("<project>"),
        ambient.workspace_id,
        ambient.project_id,
        ambient.task_id.as_deref().unwrap_or("<project>"),
    )))
}

fn validate_relation_semantics(
    transaction: &Transaction<'_>,
    input: &RelationPutInput,
) -> Result<()> {
    if matches!(
        input.relation,
        RelationType::Contradicts | RelationType::Supersedes
    ) {
        if !matches!(input.source_type, EntityType::Claim)
            || !matches!(input.target_type, EntityType::Claim)
        {
            return Err(MemoryError::InvalidRequest(format!(
                "{} relations require claim-to-claim endpoints",
                enum_string(&input.relation)?
            )));
        }
        for claim_id in [&input.source_id, &input.target_id] {
            let status: String = transaction.query_row(
                "SELECT status FROM claims WHERE id = ?1",
                [claim_id],
                |row| row.get(0),
            )?;
            if matches!(status.as_str(), "retracted" | "superseded") {
                return Err(MemoryError::InvalidRequest(format!(
                    "cannot relate terminal {status} claim {claim_id} as {}",
                    enum_string(&input.relation)?
                )));
            }
        }
    }
    Ok(())
}

fn apply_relation_semantics(
    transaction: &Transaction<'_>,
    input: &RelationPutInput,
    relation_id: &str,
    commit_seq: i64,
    now: DateTime<Utc>,
) -> Result<()> {
    match input.relation {
        RelationType::Supersedes => {
            // Direction is deliberate: source is the newer/current claim and
            // target is the claim it replaces.
            transaction.execute(
                "UPDATE claims SET status = 'superseded', updated_at = ?1
                 WHERE id = ?2 AND status IN ('active', 'conflicted')",
                params![now, input.target_id],
            )?;
            let mut affected_claims = transition_open_conflicts(
                transaction,
                &input.target_id,
                ConflictState::Resolved,
                "claim superseded",
                commit_seq,
                now,
            )?;
            affected_claims.push(input.source_id.clone());
            affected_claims.push(input.target_id.clone());
            recompute_claim_statuses(transaction, &affected_claims, now)?;
        }
        RelationType::Contradicts => {
            open_conflict_assessment(
                transaction,
                relation_id,
                &input.source_id,
                &input.target_id,
                "contradiction relation created",
                commit_seq,
                now,
            )?;
            recompute_claim_statuses(
                transaction,
                &[input.source_id.clone(), input.target_id.clone()],
                now,
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn append_conflict_event(
    transaction: &Transaction<'_>,
    case_id: &str,
    event_type: &str,
    revisions: (&str, &str),
    reason: &str,
    operation_commit_seq: i64,
    now: DateTime<Utc>,
) -> Result<()> {
    transaction.execute(
        "INSERT INTO conflict_events (
            event_id, case_id, event_type, source_revision_id,
            target_revision_id, reason, operation_commit_seq, created_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            new_id("cevt"),
            case_id,
            event_type,
            revisions.0,
            revisions.1,
            reason,
            operation_commit_seq,
            now,
        ],
    )?;
    Ok(())
}

fn open_conflict_assessment(
    transaction: &Transaction<'_>,
    relation_id: &str,
    source_claim_id: &str,
    target_claim_id: &str,
    reason: &str,
    operation_commit_seq: i64,
    now: DateTime<Utc>,
) -> Result<Option<String>> {
    let source_revision_id: String = transaction.query_row(
        "SELECT current_revision_id FROM claims WHERE id = ?1",
        [source_claim_id],
        |row| row.get(0),
    )?;
    let target_revision_id: String = transaction.query_row(
        "SELECT current_revision_id FROM claims WHERE id = ?1",
        [target_claim_id],
        |row| row.get(0),
    )?;
    let case_id = new_id("ccase");
    let inserted = transaction.execute(
        "INSERT OR IGNORE INTO conflict_cases (
            case_id, relation_id, source_claim_id, source_revision_id,
            target_claim_id, target_revision_id, state,
            opened_at, updated_at, state_reason, opened_commit_seq
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'open', ?7, ?7, ?8, ?9)",
        params![
            case_id,
            relation_id,
            source_claim_id,
            source_revision_id,
            target_claim_id,
            target_revision_id,
            now,
            reason,
            operation_commit_seq,
        ],
    )?;
    if inserted == 0 {
        let existing: Option<(String, String)> = transaction
            .query_row(
                "SELECT case_id, state FROM conflict_cases
                  WHERE relation_id = ?1 AND source_revision_id = ?2
                    AND target_revision_id = ?3",
                params![relation_id, source_revision_id, target_revision_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        return match existing {
            Some((_, state)) if state == "open" => Ok(None),
            Some((existing_case, state)) => Err(MemoryError::Config(format!(
                "conflict assessment {existing_case} for current revisions is unexpectedly {state}"
            ))),
            None => Err(MemoryError::Config(format!(
                "relation {relation_id} already has a different open conflict assessment"
            ))),
        };
    }
    append_conflict_event(
        transaction,
        &case_id,
        "opened",
        (&source_revision_id, &target_revision_id),
        reason,
        operation_commit_seq,
        now,
    )?;
    Ok(Some(case_id))
}

fn reassess_live_conflicts(
    transaction: &Transaction<'_>,
    claim_id: &str,
    operation_commit_seq: i64,
    now: DateTime<Utc>,
) -> Result<Vec<String>> {
    let relations: Vec<(String, String, String)> = {
        let mut statement = transaction.prepare(
            "SELECT r.id, r.source_id, r.target_id
               FROM relations r
               JOIN claims sc ON sc.id = r.source_id
               JOIN claims tc ON tc.id = r.target_id
              WHERE r.relation = 'contradicts'
                AND r.source_type = 'claim' AND r.target_type = 'claim'
                AND (r.source_id = ?1 OR r.target_id = ?1)
                AND sc.status IN ('active', 'conflicted')
                AND tc.status IN ('active', 'conflicted')
              ORDER BY r.commit_seq",
        )?;
        statement
            .query_map([claim_id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .collect::<std::result::Result<_, _>>()?
    };
    let mut affected = BTreeSet::new();
    for (relation_id, source_id, target_id) in relations {
        open_conflict_assessment(
            transaction,
            &relation_id,
            &source_id,
            &target_id,
            "automatically reassessed after claim revision",
            operation_commit_seq,
            now,
        )?;
        affected.insert(source_id);
        affected.insert(target_id);
    }
    Ok(affected.into_iter().collect())
}

/// Move every currently open case involving `claim_id` to a terminal or stale
/// state. Returns all endpoint claim IDs whose derived presentation may change.
fn transition_open_conflicts(
    transaction: &Transaction<'_>,
    claim_id: &str,
    state: ConflictState,
    reason: &str,
    operation_commit_seq: i64,
    now: DateTime<Utc>,
) -> Result<Vec<String>> {
    if matches!(state, ConflictState::Open) {
        return Err(MemoryError::Config(
            "open is not a valid conflict transition target".into(),
        ));
    }
    let rows: Vec<(String, String, String, String, String)> = {
        let mut statement = transaction.prepare(
            "SELECT case_id, source_claim_id, source_revision_id,
                    target_claim_id, target_revision_id
               FROM conflict_cases
              WHERE state = 'open'
                AND (source_claim_id = ?1 OR target_claim_id = ?1)
              ORDER BY case_sequence",
        )?;
        statement
            .query_map([claim_id], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            })?
            .collect::<std::result::Result<_, _>>()?
    };
    let state_name = enum_string(&state)?;
    let mut affected = BTreeSet::new();
    for (case_id, source_id, source_revision, target_id, target_revision) in rows {
        transaction.execute(
            "UPDATE conflict_cases
                SET state = ?1, updated_at = ?2, state_reason = ?3
              WHERE case_id = ?4 AND state = 'open'",
            params![state_name, now, reason, case_id],
        )?;
        append_conflict_event(
            transaction,
            &case_id,
            &state_name,
            (&source_revision, &target_revision),
            reason,
            operation_commit_seq,
            now,
        )?;
        affected.insert(source_id);
        affected.insert(target_id);
    }
    Ok(affected.into_iter().collect())
}

fn recompute_claim_statuses(
    transaction: &Transaction<'_>,
    claim_ids: &[String],
    now: DateTime<Utc>,
) -> Result<()> {
    for claim_id in claim_ids.iter().collect::<BTreeSet<_>>() {
        transaction.execute(
            "UPDATE claims
                SET status = CASE WHEN EXISTS (
                    SELECT 1 FROM conflict_cases cc
                     WHERE cc.state = 'open'
                       AND ((cc.source_claim_id = claims.id
                             AND cc.source_revision_id = claims.current_revision_id)
                         OR (cc.target_claim_id = claims.id
                             AND cc.target_revision_id = claims.current_revision_id))
                ) THEN 'conflicted' ELSE 'active' END,
                    updated_at = ?1
              WHERE id = ?2 AND status IN ('active', 'conflicted')",
            params![now, claim_id],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context(project: &str) -> AmbientContext {
        context_with_task(project, Some("task"))
    }

    fn context_with_task(project: &str, task: Option<&str>) -> AmbientContext {
        scoped_context("workspace", project, task)
    }

    fn scoped_context(workspace: &str, project: &str, task: Option<&str>) -> AmbientContext {
        AmbientContext {
            workspace_id: workspace.into(),
            project_id: project.into(),
            task_id: task.map(str::to_owned),
            component: None,
            pins: Vec::new(),
        }
    }

    fn ranking_candidate(
        id: &str,
        lexical_score: f64,
        effective_at: DateTime<Utc>,
        profile: RecencyProfile,
    ) -> SearchCandidate {
        SearchCandidate {
            hit: SearchHit {
                entity_type: EntityType::Artifact,
                entity_id: id.into(),
                revision_id: format!("rev_{id}"),
                status: "active".into(),
                title: id.into(),
                excerpt: id.into(),
                citation: format!("memoree://artifact/{id}@rev_{id}"),
                context: scoped_context("workspace", "project", Some("task")),
                score: lexical_score,
                ranking: placeholder_ranking(
                    lexical_score,
                    effective_at,
                    RecencyTimestampBasis::RevisionCreatedAt,
                    profile.class,
                ),
                matched_by: vec!["fts5_bm25".into()],
                provenance: BTreeMap::new(),
            },
            effective_at,
            effective_at_basis: RecencyTimestampBasis::RevisionCreatedAt,
            profile,
            recency_eligible: true,
        }
    }

    #[test]
    fn bounded_recency_preserves_stronger_older_relevance() {
        use chrono::Duration;

        let evaluated_at = Utc::now();
        let profile = RecencyProfile {
            class: RecencyDecayClass::General,
            half_life_days: 180.0,
            max_bonus_ratio: 0.05,
        };
        let hits = rerank_with_recency(
            vec![
                ranking_candidate(
                    "older-more-relevant",
                    1.0,
                    evaluated_at - Duration::days(730),
                    profile,
                ),
                ranking_candidate("newer-less-relevant", 0.94, evaluated_at, profile),
            ],
            true,
            evaluated_at,
        );

        assert_eq!(hits[0].entity_id, "older-more-relevant");
        assert_eq!(hits[0].ranking.lexical_position, 1);
        assert_eq!(hits[0].ranking.final_position, 1);
        assert!(hits[1].ranking.recency_bonus > hits[0].ranking.recency_bonus);
    }

    #[test]
    fn bounded_recency_never_promotes_more_than_two_positions_or_changes_membership() {
        use chrono::Duration;

        let evaluated_at = Utc::now();
        let profile = RecencyProfile {
            class: RecencyDecayClass::Ephemeral,
            half_life_days: 30.0,
            max_bonus_ratio: 0.10,
        };
        let candidates = vec![
            ranking_candidate("one", 1.000, evaluated_at - Duration::days(365), profile),
            ranking_candidate("two", 0.999, evaluated_at - Duration::days(365), profile),
            ranking_candidate("three", 0.998, evaluated_at - Duration::days(365), profile),
            ranking_candidate("four", 0.997, evaluated_at - Duration::days(365), profile),
            ranking_candidate("five", 0.996, evaluated_at, profile),
        ];
        let expected_ids: std::collections::BTreeSet<_> = candidates
            .iter()
            .map(|candidate| candidate.hit.entity_id.clone())
            .collect();
        let hits = rerank_with_recency(candidates, true, evaluated_at);
        let actual_ids: std::collections::BTreeSet<_> =
            hits.iter().map(|hit| hit.entity_id.clone()).collect();

        assert_eq!(actual_ids, expected_ids);
        for hit in &hits {
            let promoted_by = hit
                .ranking
                .lexical_position
                .saturating_sub(hit.ranking.final_position);
            assert!(promoted_by <= RECENCY_MAX_PROMOTION);
        }
        let freshest = hits.iter().find(|hit| hit.entity_id == "five").unwrap();
        assert_eq!(freshest.ranking.lexical_position, 5);
        assert_eq!(freshest.ranking.final_position, 3);
    }

    #[test]
    fn bounded_recency_is_deterministic_for_a_fixed_evaluation_instant() {
        use chrono::Duration;

        let evaluated_at = Utc::now();
        let profile = RecencyProfile {
            class: RecencyDecayClass::Observation,
            half_life_days: 30.0,
            max_bonus_ratio: 0.10,
        };
        let candidates = vec![
            ranking_candidate("old", 1.0, evaluated_at - Duration::days(60), profile),
            ranking_candidate("new", 0.98, evaluated_at - Duration::days(1), profile),
        ];

        let first = rerank_with_recency(candidates.clone(), true, evaluated_at);
        let second = rerank_with_recency(candidates, true, evaluated_at);
        assert_eq!(
            serde_json::to_value(first).unwrap(),
            serde_json::to_value(second).unwrap()
        );
    }

    #[test]
    fn structured_text_media_is_body_searchable_without_binary_extraction() {
        assert_eq!(
            searchable_text(
                "application/json; charset=utf-8",
                br#"{"codename":"quartz"}"#
            ),
            r#"{"codename":"quartz"}"#
        );
        assert_eq!(
            searchable_text("application/vnd.example+json", b"semantic payload"),
            "semantic payload"
        );
        assert!(searchable_text("application/octet-stream", b"semantic payload").is_empty());
    }

    fn artifact(title: &str, body: &str) -> ArtifactPutInput {
        ArtifactPutInput {
            kind: "note".into(),
            title: title.into(),
            media_type: "text/plain; charset=utf-8".into(),
            content: ArtifactContent::Text(body.into()),
            provenance: BTreeMap::new(),
            actor: Some("test".into()),
        }
    }

    fn claim(statement: &str, evidence: Vec<EvidenceLocator>) -> ClaimAssertInput {
        ClaimAssertInput {
            claim_type: ClaimType::Decision,
            statement: statement.into(),
            confidence: Some(0.9),
            evidence,
            valid_from: None,
            valid_until: None,
            actor: Some("test".into()),
        }
    }

    #[test]
    fn artifact_revision_idempotency_and_search_are_synchronous() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let original = store
            .artifact_put(
                &context("one"),
                &artifact("Architecture", "SQLite provides durable memory"),
                Some("put-1"),
                "hash-1",
            )
            .unwrap();
        let replay = store
            .artifact_put(
                &context("one"),
                &artifact("Architecture", "SQLite provides durable memory"),
                Some("put-1"),
                "hash-1",
            )
            .unwrap();
        assert!(original.value.content.is_none());
        assert!(!replay.created);
        assert_eq!(replay.value.artifact_id, original.value.artifact_id);
        assert_eq!(replay.commit_seq, original.commit_seq);
        let serialized = serde_json::to_value(&original).unwrap();
        assert_eq!(serialized["commit_seq"], original.commit_seq);
        assert_eq!(serialized["revision_commit_seq"], original.value.commit_seq);

        let result = store
            .search(
                &context("one"),
                &SearchInput {
                    query: "durable memory".into(),
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 10,
                    include_historical: false,
                    min_commit_seq: Some(original.commit_seq),
                    recency: Default::default(),
                },
            )
            .unwrap();
        assert_eq!(result.hits.len(), 1);
        assert!(
            result.hits[0]
                .citation
                .contains(&original.value.revision_id)
        );

        let conflict = store.artifact_put(
            &context("one"),
            &artifact("Different", "content"),
            Some("put-1"),
            "different-hash",
        );
        assert!(matches!(conflict, Err(MemoryError::IdempotencyConflict(_))));
    }

    #[test]
    fn future_schema_is_rejected_before_wal_or_schema_mutation() {
        let temporary = tempfile::tempdir().unwrap();
        let database = temporary.path().join(MEMOREE_DATABASE_FILE);
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL) STRICT;
                 INSERT INTO meta(key, value) VALUES ('schema_version', '99');",
            )
            .unwrap();
        drop(connection);
        let before = fs::read(&database).unwrap();

        let result = Store::open(temporary.path());
        assert!(
            matches!(result, Err(MemoryError::Config(message)) if message.contains("schema version 99"))
        );
        assert_eq!(fs::read(&database).unwrap(), before);
        assert!(!temporary.path().join("memoree.sqlite3-wal").exists());
        assert!(!temporary.path().join("memoree.sqlite3-shm").exists());
    }

    #[test]
    fn schema_v1_migrates_conflicts_from_relation_commit_history_without_data_loss() {
        let temporary = tempfile::tempdir().unwrap();
        let owner = context("migration");
        let (left_id, left_frozen_revision, left_current_revision, relation_id, last_seq) = {
            let store = Store::open(temporary.path()).unwrap();
            let left = store
                .claim_assert(
                    &owner,
                    &claim("migration value is old", vec![]),
                    Some("migration-left"),
                    "migration-left",
                )
                .unwrap();
            let right = store
                .claim_assert(
                    &owner,
                    &claim("migration value is other", vec![]),
                    Some("migration-right"),
                    "migration-right",
                )
                .unwrap();
            let relation = store
                .relation_put(
                    &owner,
                    &RelationPutInput {
                        source_type: EntityType::Claim,
                        source_id: left.value.claim_id.clone(),
                        relation: RelationType::Contradicts,
                        target_type: EntityType::Claim,
                        target_id: right.value.claim_id,
                        metadata: BTreeMap::new(),
                    },
                    Some("migration-edge"),
                    "migration-edge",
                )
                .unwrap();
            let revised = store
                .claim_revise(
                    &owner,
                    &ClaimReviseInput {
                        claim_id: left.value.claim_id.clone(),
                        if_revision: left.value.revision_id.clone(),
                        statement: "migration value is current".into(),
                        confidence: Some(0.9),
                        evidence: vec![],
                        actor: None,
                    },
                    Some("migration-revise"),
                    "migration-revise",
                )
                .unwrap();
            (
                left.value.claim_id,
                left.value.revision_id,
                revised.value.revision_id,
                relation.value.relation_id,
                store.last_commit_seq().unwrap(),
            )
        };

        let database = temporary.path().join(MEMOREE_DATABASE_FILE);
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "DROP TABLE conflict_events;
                 DROP TABLE conflict_cases;
                 UPDATE meta SET value = '1' WHERE key = 'schema_version';",
            )
            .unwrap();
        drop(connection);

        let migrated = Store::open(temporary.path()).unwrap();
        assert_eq!(migrated.last_commit_seq().unwrap(), last_seq);
        let list = migrated
            .conflict_list(
                &owner,
                &ConflictListInput {
                    horizon: Horizon::Ambient,
                    reason: None,
                    include_stale: true,
                    limit: 100,
                    before_case_sequence: None,
                },
            )
            .unwrap();
        assert_eq!(list.conflicts.len(), 2);
        let current = &list.conflicts[0];
        assert_eq!(current.relation_id, relation_id);
        assert!(matches!(current.state, ConflictState::Open));
        assert_eq!(current.source.claim_id, left_id);
        assert_eq!(current.source.frozen.revision_id, left_current_revision);
        let historical = &list.conflicts[1];
        assert!(matches!(historical.state, ConflictState::Stale));
        assert_eq!(historical.source.frozen.revision_id, left_frozen_revision);
        assert_eq!(historical.source.current.revision_id, left_current_revision);
        let connection = migrated.connection.lock();
        let schema_version: i64 = connection
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let counts: (i64, i64, i64, i64) = connection
            .query_row(
                "SELECT (SELECT COUNT(*) FROM claims),
                        (SELECT COUNT(*) FROM claim_revisions),
                        (SELECT COUNT(*) FROM relations),
                        (SELECT COUNT(*) FROM events)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(schema_version, 3);
        assert_eq!(counts, (2, 3, 1, last_seq));
        drop(connection);
        assert!(migrated.verify().unwrap().ok);
    }

    #[test]
    fn schema_v2_migrates_stale_case_and_opens_current_assessment_without_data_loss() {
        let temporary = tempfile::tempdir().unwrap();
        let owner = context("migration-v2");
        let (relation_id, frozen_revision, current_revision, last_seq) = {
            let store = Store::open(temporary.path()).unwrap();
            let left = store
                .claim_assert(
                    &owner,
                    &claim("v2 left old", vec![]),
                    Some("v2-left"),
                    "v2-left",
                )
                .unwrap();
            let right = store
                .claim_assert(
                    &owner,
                    &claim("v2 right", vec![]),
                    Some("v2-right"),
                    "v2-right",
                )
                .unwrap();
            let relation = store
                .relation_put(
                    &owner,
                    &RelationPutInput {
                        source_type: EntityType::Claim,
                        source_id: left.value.claim_id.clone(),
                        relation: RelationType::Contradicts,
                        target_type: EntityType::Claim,
                        target_id: right.value.claim_id,
                        metadata: BTreeMap::new(),
                    },
                    Some("v2-edge"),
                    "v2-edge",
                )
                .unwrap();
            let revised = store
                .claim_revise(
                    &owner,
                    &ClaimReviseInput {
                        claim_id: left.value.claim_id,
                        if_revision: left.value.revision_id.clone(),
                        statement: "v2 left current".into(),
                        confidence: None,
                        evidence: vec![],
                        actor: None,
                    },
                    Some("v2-revise"),
                    "v2-revise",
                )
                .unwrap();
            (
                relation.value.relation_id,
                left.value.revision_id,
                revised.value.revision_id,
                store.last_commit_seq().unwrap(),
            )
        };

        let database = temporary.path().join(MEMOREE_DATABASE_FILE);
        let connection = Connection::open(&database).unwrap();
        connection
            .execute_batch(
                "PRAGMA foreign_keys = OFF;
                 CREATE TEMP TABLE saved_case AS
                   SELECT * FROM conflict_cases WHERE state = 'stale';
                 CREATE TEMP TABLE saved_events AS
                   SELECT ce.* FROM conflict_events ce
                   JOIN saved_case sc ON sc.case_id = ce.case_id;
                 DROP TABLE conflict_events;
                 DROP TABLE conflict_cases;
                 CREATE TABLE conflict_cases (
                   relation_id TEXT PRIMARY KEY REFERENCES relations(id) ON DELETE RESTRICT,
                   source_claim_id TEXT NOT NULL REFERENCES claims(id) ON DELETE RESTRICT,
                   source_revision_id TEXT NOT NULL REFERENCES claim_revisions(id) ON DELETE RESTRICT,
                   target_claim_id TEXT NOT NULL REFERENCES claims(id) ON DELETE RESTRICT,
                   target_revision_id TEXT NOT NULL REFERENCES claim_revisions(id) ON DELETE RESTRICT,
                   state TEXT NOT NULL CHECK(state IN ('open', 'stale', 'resolved')),
                   opened_at TEXT NOT NULL,
                   updated_at TEXT NOT NULL,
                   terminal_reason TEXT,
                   relation_commit_seq INTEGER NOT NULL UNIQUE
                 ) STRICT;
                 CREATE TABLE conflict_events (
                   event_id TEXT PRIMARY KEY,
                   relation_id TEXT NOT NULL REFERENCES conflict_cases(relation_id) ON DELETE RESTRICT,
                   event_type TEXT NOT NULL CHECK(event_type IN ('opened', 'stale', 'resolved')),
                   source_revision_id TEXT NOT NULL,
                   target_revision_id TEXT NOT NULL,
                   reason TEXT NOT NULL,
                   operation_commit_seq INTEGER NOT NULL,
                   created_at TEXT NOT NULL
                 ) STRICT;
                 INSERT INTO conflict_cases
                 SELECT sc.relation_id, sc.source_claim_id, sc.source_revision_id,
                        sc.target_claim_id, sc.target_revision_id, sc.state,
                        sc.opened_at, sc.updated_at, sc.state_reason,
                        (SELECT commit_seq FROM relations r WHERE r.id = sc.relation_id)
                   FROM saved_case sc;
                 INSERT INTO conflict_events
                 SELECT se.event_id, sc.relation_id, se.event_type,
                        se.source_revision_id, se.target_revision_id, se.reason,
                        se.operation_commit_seq, se.created_at
                   FROM saved_events se
                   JOIN saved_case sc ON sc.case_id = se.case_id;
                 UPDATE meta SET value = '2' WHERE key = 'schema_version';",
            )
            .unwrap();
        let preserved_event_ids: Vec<String> = connection
            .prepare("SELECT event_id FROM conflict_events ORDER BY event_id")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        drop(connection);

        let migrated = Store::open(temporary.path()).unwrap();
        assert_eq!(migrated.last_commit_seq().unwrap(), last_seq);
        let cases = migrated
            .conflict_list(
                &owner,
                &ConflictListInput {
                    horizon: Horizon::Ambient,
                    reason: None,
                    include_stale: true,
                    limit: 100,
                    before_case_sequence: None,
                },
            )
            .unwrap();
        assert_eq!(cases.conflicts.len(), 2);
        assert!(matches!(cases.conflicts[0].state, ConflictState::Open));
        assert_eq!(cases.conflicts[0].relation_id, relation_id);
        assert_eq!(
            cases.conflicts[0].source.frozen.revision_id,
            current_revision
        );
        assert!(matches!(cases.conflicts[1].state, ConflictState::Stale));
        assert_eq!(
            cases.conflicts[1].source.frozen.revision_id,
            frozen_revision
        );
        let connection = migrated.connection.lock();
        let schema_version: i64 = connection
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let migrated_event_ids: Vec<String> = connection
            .prepare(
                "SELECT event_id FROM conflict_events
                  WHERE event_id NOT LIKE 'cevt_migrated_v2_current_%'
                  ORDER BY event_id",
            )
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(schema_version, 3);
        assert_eq!(migrated_event_ids, preserved_event_ids);
        drop(connection);
        assert!(migrated.verify().unwrap().ok);
    }

    #[test]
    fn ambient_search_does_not_leak_sibling_projects() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        store
            .artifact_put(
                &context("one"),
                &artifact("One", "a unique narwhal memory"),
                Some("one"),
                "one",
            )
            .unwrap();
        let sibling = store
            .artifact_put(
                &context("two"),
                &artifact("Two", "another narwhal memory"),
                Some("two"),
                "two",
            )
            .unwrap();
        let ambient = store
            .search(
                &context("one"),
                &SearchInput {
                    query: "narwhal".into(),
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 10,
                    include_historical: false,
                    min_commit_seq: None,
                    recency: Default::default(),
                },
            )
            .unwrap();
        assert_eq!(ambient.hits.len(), 1);
        let mut pinned_context = context("one");
        pinned_context.pins.push(format!(
            "memoree://artifact/{}@{}",
            sibling.value.artifact_id, sibling.value.revision_id
        ));
        let pinned = store
            .search(
                &pinned_context,
                &SearchInput {
                    query: "narwhal".into(),
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 10,
                    include_historical: false,
                    min_commit_seq: None,
                    recency: Default::default(),
                },
            )
            .unwrap();
        assert_eq!(pinned.hits.len(), 2);
        let personal = store
            .search(
                &context("one"),
                &SearchInput {
                    horizon: Horizon::Personal,
                    ..SearchInput {
                        query: "narwhal".into(),
                        horizon: Horizon::Ambient,
                        reason: Some("explicit test".into()),
                        limit: 10,
                        include_historical: false,
                        min_commit_seq: None,
                        recency: Default::default(),
                    }
                },
            )
            .unwrap();
        assert_eq!(personal.hits.len(), 2);
    }

    #[test]
    fn exact_revision_pins_are_artifact_qualified_and_survive_broader_horizons() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let local = store
            .artifact_put(
                &context("one"),
                &artifact("Local", "pinprobe local"),
                Some("pin-local"),
                "pin-local",
            )
            .unwrap();
        let sibling_old = store
            .artifact_put(
                &context("two"),
                &artifact("Sibling old", "pinprobe old"),
                Some("pin-sibling-old"),
                "pin-sibling-old",
            )
            .unwrap();
        let sibling_new = store
            .artifact_revise(
                &context("two"),
                &ArtifactReviseInput {
                    artifact_id: sibling_old.value.artifact_id.clone(),
                    if_revision: sibling_old.value.revision_id.clone(),
                    title: Some("Sibling current".into()),
                    media_type: None,
                    content: ArtifactContent::Text("pinprobe current".into()),
                    provenance: BTreeMap::new(),
                    actor: Some("test".into()),
                },
                Some("pin-sibling-new"),
                "pin-sibling-new",
            )
            .unwrap();

        let mut pinned = context("one");
        pinned.pins.push(format!(
            "memoree://artifact/{}@{}",
            sibling_old.value.artifact_id, sibling_old.value.revision_id
        ));
        let ambient = store
            .search(
                &pinned,
                &SearchInput {
                    query: "pinprobe".into(),
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 10,
                    include_historical: false,
                    min_commit_seq: None,
                    recency: Default::default(),
                },
            )
            .unwrap();
        let ambient_revisions = ambient
            .hits
            .iter()
            .map(|hit| hit.revision_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(ambient_revisions.len(), 2);
        assert!(ambient_revisions.contains(local.value.revision_id.as_str()));
        assert!(ambient_revisions.contains(sibling_old.value.revision_id.as_str()));
        assert!(!ambient_revisions.contains(sibling_new.value.revision_id.as_str()));

        let broader = store
            .search(
                &pinned,
                &SearchInput {
                    query: "pinprobe".into(),
                    horizon: Horizon::Workspace,
                    reason: Some("explicit broader-horizon test".into()),
                    limit: 10,
                    include_historical: false,
                    min_commit_seq: None,
                    recency: Default::default(),
                },
            )
            .unwrap();
        let broader_revisions = broader
            .hits
            .iter()
            .map(|hit| hit.revision_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert!(ambient_revisions.is_subset(&broader_revisions));
        assert!(broader_revisions.contains(sibling_new.value.revision_id.as_str()));

        let mut mismatched = context("one");
        mismatched.pins.push(format!(
            "memoree://artifact/{}@{}",
            local.value.artifact_id, sibling_old.value.revision_id
        ));
        let mismatch = store
            .search(
                &mismatched,
                &SearchInput {
                    query: "pinprobe".into(),
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 10,
                    include_historical: false,
                    min_commit_seq: None,
                    recency: Default::default(),
                },
            )
            .unwrap();
        assert_eq!(mismatch.hits.len(), 1);
        assert_eq!(mismatch.hits[0].revision_id, local.value.revision_id);
    }

    #[test]
    fn revisions_are_immutable_and_stale_heads_are_rejected() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let first = store
            .artifact_put(
                &context("one"),
                &artifact("Decision", "first durable version"),
                Some("put"),
                "put",
            )
            .unwrap();
        let revise = ArtifactReviseInput {
            artifact_id: first.value.artifact_id.clone(),
            if_revision: first.value.revision_id.clone(),
            title: Some("Revised decision".into()),
            media_type: None,
            content: ArtifactContent::Text("second durable version".into()),
            provenance: BTreeMap::new(),
            actor: Some("test".into()),
        };
        let second = store
            .artifact_revise(&context("one"), &revise, Some("revise"), "revise")
            .unwrap();
        assert!(second.value.content.is_none());
        assert_ne!(first.value.revision_id, second.value.revision_id);
        assert!(matches!(
            store.artifact_revise(&context("one"), &revise, Some("stale"), "stale"),
            Err(MemoryError::RevisionConflict { .. })
        ));
        let old = store
            .artifact_get(&ArtifactGetInput {
                artifact_id: first.value.artifact_id.clone(),
                revision_id: Some(first.value.revision_id.clone()),
                include_content: true,
            })
            .unwrap();
        assert!(
            matches!(old.content, Some(ArtifactContent::Text(ref text)) if text == "first durable version")
        );
        let connection = store.connection.lock();
        assert!(
            connection
                .execute(
                    "UPDATE artifact_revisions SET title = 'mutated' WHERE id = ?1",
                    [&first.value.revision_id]
                )
                .is_err()
        );
    }

    #[test]
    fn claim_history_is_global_newest_first_and_exclusively_paginated() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let owner = scoped_context("foreign-workspace", "foreign-project", Some("foreign-task"));
        let first = store
            .claim_assert(
                &owner,
                &claim("first claim revision", vec![]),
                Some("claim-history-first"),
                "claim-history-first",
            )
            .unwrap();
        let second = store
            .claim_revise(
                &owner,
                &ClaimReviseInput {
                    claim_id: first.value.claim_id.clone(),
                    if_revision: first.value.revision_id.clone(),
                    statement: "second claim revision".into(),
                    confidence: Some(0.8),
                    evidence: vec![],
                    actor: Some("second-actor".into()),
                },
                Some("claim-history-second"),
                "claim-history-second",
            )
            .unwrap();
        let third = store
            .claim_revise(
                &owner,
                &ClaimReviseInput {
                    claim_id: first.value.claim_id.clone(),
                    if_revision: second.value.revision_id.clone(),
                    statement: "third claim revision".into(),
                    confidence: Some(0.7),
                    evidence: vec![],
                    actor: Some("third-actor".into()),
                },
                Some("claim-history-third"),
                "claim-history-third",
            )
            .unwrap();

        let first_page = store
            .claim_history(&ClaimHistoryInput {
                claim_id: first.value.claim_id.clone(),
                limit: 2,
                before_revision_number: None,
            })
            .unwrap();
        assert!(first_page.truncated);
        assert_eq!(first_page.next_before_revision_number, Some(2));
        assert_eq!(
            first_page
                .revisions
                .iter()
                .map(|record| record.revision_number)
                .collect::<Vec<_>>(),
            vec![3, 2]
        );
        assert_eq!(first_page.revisions[0].revision_id, third.value.revision_id);
        assert_eq!(
            first_page.revisions[0].actor.as_deref(),
            Some("third-actor")
        );
        assert_eq!(
            first_page.revisions[1].revision_id,
            second.value.revision_id
        );
        assert_eq!(first_page.revisions[1].confidence, Some(0.8));

        let second_page = store
            .claim_history(&ClaimHistoryInput {
                claim_id: first.value.claim_id.clone(),
                limit: 2,
                before_revision_number: first_page.next_before_revision_number,
            })
            .unwrap();
        assert!(!second_page.truncated);
        assert!(second_page.next_before_revision_number.is_none());
        assert_eq!(second_page.revisions.len(), 1);
        assert_eq!(second_page.revisions[0].revision_number, 1);
        assert_eq!(second_page.revisions[0].statement, "first claim revision");

        let exhausted = store
            .claim_history(&ClaimHistoryInput {
                claim_id: first.value.claim_id.clone(),
                limit: 2,
                before_revision_number: Some(1),
            })
            .unwrap();
        assert!(exhausted.revisions.is_empty());
        assert!(!exhausted.truncated);

        let retract = store
            .claim_retract(
                &owner,
                &ClaimRetractInput {
                    claim_id: first.value.claim_id.clone(),
                    reason: "history lifecycle test".into(),
                },
                Some("claim-history-retract"),
                "claim-history-retract",
            )
            .unwrap();
        let after_retract = store
            .claim_history(&ClaimHistoryInput {
                claim_id: first.value.claim_id,
                limit: 100,
                before_revision_number: None,
            })
            .unwrap();
        assert_eq!(after_retract.revisions.len(), 3);
        assert!(
            after_retract
                .revisions
                .iter()
                .all(|record| matches!(record.status, ClaimStatus::Retracted))
        );
        assert!(
            after_retract
                .revisions
                .iter()
                .all(|record| record.commit_seq < retract.commit_seq)
        );
    }

    #[test]
    fn claim_history_rejects_invalid_bounds_and_missing_claims() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let owner = context("claim-history-bounds");
        let claim = store
            .claim_assert(
                &owner,
                &claim("claim history bounds", vec![]),
                Some("claim-history-bounds"),
                "claim-history-bounds",
            )
            .unwrap();
        let input = |claim_id: String, limit, before_revision_number| ClaimHistoryInput {
            claim_id,
            limit,
            before_revision_number,
        };

        assert!(matches!(
            store.claim_history(&input("missing".into(), 50, None)),
            Err(MemoryError::NotFound(_))
        ));
        assert!(matches!(
            store.claim_history(&input(claim.value.claim_id.clone(), 0, None)),
            Err(MemoryError::InvalidRequest(_))
        ));
        assert!(matches!(
            store.claim_history(&input(
                claim.value.claim_id.clone(),
                MAX_HISTORY_ITEMS + 1,
                None
            )),
            Err(MemoryError::InvalidRequest(_))
        ));
        assert!(matches!(
            store.claim_history(&input(claim.value.claim_id, 50, Some(0))),
            Err(MemoryError::InvalidRequest(_))
        ));
        assert!(matches!(
            store.claim_history(&input("x".repeat(MAX_CONTEXT_ID_BYTES + 1), 50, None)),
            Err(MemoryError::InvalidRequest(_))
        ));
    }

    #[test]
    fn claim_conflict_and_supersession_semantics_are_explicit() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let evidence_artifact = store
            .artifact_put(
                &context("one"),
                &artifact("Evidence", "database benchmark evidence"),
                Some("evidence"),
                "evidence",
            )
            .unwrap();
        let evidence = vec![EvidenceLocator {
            artifact_id: evidence_artifact.value.artifact_id.clone(),
            revision_id: evidence_artifact.value.revision_id.clone(),
            start_byte: Some(0),
            end_byte: Some(8),
        }];
        let left = store
            .claim_assert(
                &context("one"),
                &claim("SQLite is the selected database", evidence.clone()),
                Some("left"),
                "left",
            )
            .unwrap();
        let right = store
            .claim_assert(
                &context("one"),
                &claim("Postgres is the selected database", evidence.clone()),
                Some("right"),
                "right",
            )
            .unwrap();
        let contradiction = RelationPutInput {
            source_type: EntityType::Claim,
            source_id: left.value.claim_id.clone(),
            relation: RelationType::Contradicts,
            target_type: EntityType::Claim,
            target_id: right.value.claim_id.clone(),
            metadata: BTreeMap::new(),
        };
        store
            .relation_put(
                &context("one"),
                &contradiction,
                Some("conflict"),
                "conflict",
            )
            .unwrap();
        let left_after = store
            .claim_get(&ClaimGetInput {
                claim_id: left.value.claim_id.clone(),
                revision_id: None,
            })
            .unwrap();
        let right_after = store
            .claim_get(&ClaimGetInput {
                claim_id: right.value.claim_id.clone(),
                revision_id: None,
            })
            .unwrap();
        assert!(matches!(left_after.status, ClaimStatus::Conflicted));
        assert!(matches!(right_after.status, ClaimStatus::Conflicted));
        assert_eq!(
            store
                .conflicts_for_claims(
                    &context("one"),
                    Horizon::Ambient,
                    std::slice::from_ref(&left.value.claim_id),
                )
                .unwrap()
                .len(),
            1
        );

        let replacement = store
            .claim_assert(
                &context("one"),
                &claim("SQLite is now the authoritative selection", evidence),
                Some("replacement"),
                "replacement",
            )
            .unwrap();
        store
            .relation_put(
                &context("one"),
                &RelationPutInput {
                    source_type: EntityType::Claim,
                    source_id: replacement.value.claim_id.clone(),
                    relation: RelationType::Supersedes,
                    target_type: EntityType::Claim,
                    target_id: left.value.claim_id.clone(),
                    metadata: BTreeMap::new(),
                },
                Some("supersede"),
                "supersede",
            )
            .unwrap();
        let old = store
            .claim_get(&ClaimGetInput {
                claim_id: left.value.claim_id,
                revision_id: None,
            })
            .unwrap();
        assert!(matches!(old.status, ClaimStatus::Superseded));

        let current_search = store
            .search(
                &context("one"),
                &SearchInput {
                    query: "SQLite authoritative selection".into(),
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 10,
                    include_historical: false,
                    min_commit_seq: None,
                    recency: Default::default(),
                },
            )
            .unwrap();
        assert!(
            current_search
                .hits
                .iter()
                .all(|hit| hit.entity_id != old.claim_id)
        );

        let supersession = store
            .relation_list(
                &context("one"),
                &RelationListInput {
                    entity_type: EntityType::Claim,
                    entity_id: replacement.value.claim_id,
                    direction: crate::protocol::RelationDirection::Outgoing,
                    relation: Some(RelationType::Supersedes),
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 100,
                    before_commit_seq: None,
                },
            )
            .unwrap();
        assert_eq!(supersession.relations.len(), 1);
        assert_eq!(supersession.relations[0].target_id, old.claim_id);

        let right_after_resolution = store
            .claim_get(&ClaimGetInput {
                claim_id: right.value.claim_id,
                revision_id: None,
            })
            .unwrap();
        assert!(matches!(right_after_resolution.status, ClaimStatus::Active));
        let connection = store.connection.lock();
        let lifecycle: (String, i64) = connection
            .query_row(
                "SELECT state, (SELECT COUNT(*) FROM conflict_events ce
                                 WHERE ce.case_id = cc.case_id)
                   FROM conflict_cases cc",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(lifecycle, ("resolved".into(), 2));
    }

    #[test]
    fn claim_revision_stales_exact_conflict_and_exposes_frozen_and_current_snapshots() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let owner = context("revision-conflict");
        let left = store
            .claim_assert(
                &owner,
                &claim("the deployment is blue", vec![]),
                Some("revision-left"),
                "revision-left",
            )
            .unwrap();
        let right = store
            .claim_assert(
                &owner,
                &claim("the deployment is green", vec![]),
                Some("revision-right"),
                "revision-right",
            )
            .unwrap();
        let contradiction = store
            .relation_put(
                &owner,
                &RelationPutInput {
                    source_type: EntityType::Claim,
                    source_id: left.value.claim_id.clone(),
                    relation: RelationType::Contradicts,
                    target_type: EntityType::Claim,
                    target_id: right.value.claim_id.clone(),
                    metadata: BTreeMap::from([("review".into(), json!("required"))]),
                },
                Some("revision-contradiction"),
                "revision-contradiction",
            )
            .unwrap();

        let list = |include_stale| ConflictListInput {
            horizon: Horizon::Ambient,
            reason: None,
            include_stale,
            limit: 100,
            before_case_sequence: None,
        };
        let open = store.conflict_list(&owner, &list(false)).unwrap();
        assert_eq!(open.conflicts.len(), 1);
        assert!(matches!(open.conflicts[0].state, ConflictState::Open));
        assert_eq!(
            open.conflicts[0].relation_id,
            contradiction.value.relation_id
        );
        assert_eq!(
            open.conflicts[0].source.frozen.revision_id,
            left.value.revision_id
        );
        assert!(open.conflicts[0].source.frozen_is_current);
        assert_eq!(open.conflicts[0].metadata["review"], "required");
        let original_case_id = open.conflicts[0].case_id.clone();

        let revise_input = ClaimReviseInput {
            claim_id: left.value.claim_id.clone(),
            if_revision: left.value.revision_id.clone(),
            statement: "the deployment color is not yet verified".into(),
            confidence: Some(0.6),
            evidence: vec![],
            actor: Some("test".into()),
        };
        let revised = store
            .claim_revise(
                &owner,
                &revise_input,
                Some("revision-left-revise"),
                "revision-left-revise",
            )
            .unwrap();
        assert!(matches!(revised.value.status, ClaimStatus::Conflicted));
        let right_current = store
            .claim_get(&ClaimGetInput {
                claim_id: right.value.claim_id.clone(),
                revision_id: None,
            })
            .unwrap();
        assert!(matches!(right_current.status, ClaimStatus::Conflicted));
        let reassessed = store.conflict_list(&owner, &list(false)).unwrap();
        assert_eq!(reassessed.conflicts.len(), 1);
        assert_ne!(reassessed.conflicts[0].case_id, original_case_id);
        assert_eq!(
            reassessed.conflicts[0].source.frozen.revision_id,
            revised.value.revision_id
        );
        assert!(reassessed.conflicts[0].source.frozen_is_current);

        let history = store.conflict_list(&owner, &list(true)).unwrap();
        assert_eq!(history.conflicts.len(), 2);
        assert!(matches!(history.conflicts[0].state, ConflictState::Open));
        let stale = &history.conflicts[1];
        assert!(matches!(stale.state, ConflictState::Stale));
        assert_eq!(stale.case_id, original_case_id);
        assert_eq!(stale.source.frozen.revision_id, left.value.revision_id);
        assert_eq!(stale.source.current.revision_id, revised.value.revision_id);
        assert!(!stale.source.frozen_is_current);
        assert!(stale.target.frozen_is_current);
        assert!(stale.state_reason.as_deref().unwrap().contains("revision"));
        let replay = store
            .claim_revise(
                &owner,
                &revise_input,
                Some("revision-left-revise"),
                "revision-left-revise",
            )
            .unwrap();
        assert_eq!(replay.value.revision_id, revised.value.revision_id);
        assert!(!replay.created);
        assert_eq!(
            store
                .conflict_list(&owner, &list(true))
                .unwrap()
                .conflicts
                .len(),
            2
        );
        assert_eq!(
            store
                .conflicts_for_claims(
                    &owner,
                    Horizon::Ambient,
                    std::slice::from_ref(&left.value.claim_id),
                )
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .relations_for(EntityType::Claim, &left.value.claim_id)
                .unwrap()
                .len(),
            1
        );
        let connection = store.connection.lock();
        let events: Vec<String> = connection
            .prepare(
                "SELECT ce.event_type FROM conflict_events ce
                  JOIN conflict_cases cc ON cc.case_id = ce.case_id
                 WHERE cc.relation_id = ?1 ORDER BY ce.created_at, ce.rowid",
            )
            .unwrap()
            .query_map([&contradiction.value.relation_id], |row| row.get(0))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(events, vec!["opened", "stale", "opened"]);
        drop(connection);
        assert!(store.verify().unwrap().ok);
    }

    #[test]
    fn retract_resolves_open_conflicts_and_recomputes_surviving_claim() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let owner = context("retract-conflict");
        let left = store
            .claim_assert(
                &owner,
                &claim("feature flag is enabled", vec![]),
                Some("retract-left"),
                "retract-left",
            )
            .unwrap();
        let right = store
            .claim_assert(
                &owner,
                &claim("feature flag is disabled", vec![]),
                Some("retract-right"),
                "retract-right",
            )
            .unwrap();
        store
            .relation_put(
                &owner,
                &RelationPutInput {
                    source_type: EntityType::Claim,
                    source_id: left.value.claim_id.clone(),
                    relation: RelationType::Contradicts,
                    target_type: EntityType::Claim,
                    target_id: right.value.claim_id.clone(),
                    metadata: BTreeMap::new(),
                },
                Some("retract-edge"),
                "retract-edge",
            )
            .unwrap();
        store
            .claim_retract(
                &owner,
                &ClaimRetractInput {
                    claim_id: left.value.claim_id,
                    reason: "bad observation".into(),
                },
                Some("retract-claim"),
                "retract-claim",
            )
            .unwrap();
        let survivor = store
            .claim_get(&ClaimGetInput {
                claim_id: right.value.claim_id,
                revision_id: None,
            })
            .unwrap();
        assert!(matches!(survivor.status, ClaimStatus::Active));
        let connection = store.connection.lock();
        let state: String = connection
            .query_row("SELECT state FROM conflict_cases", [], |row| row.get(0))
            .unwrap();
        assert_eq!(state, "resolved");
    }

    #[test]
    fn repeated_claim_revisions_preserve_stale_cases_and_keep_one_current_open_assessment() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let owner = context("repeated-reassessment");
        let left = store
            .claim_assert(
                &owner,
                &claim("value version zero", vec![]),
                Some("repeat-left"),
                "repeat-left",
            )
            .unwrap();
        let right = store
            .claim_assert(
                &owner,
                &claim("contradictory value", vec![]),
                Some("repeat-right"),
                "repeat-right",
            )
            .unwrap();
        store
            .relation_put(
                &owner,
                &RelationPutInput {
                    source_type: EntityType::Claim,
                    source_id: left.value.claim_id.clone(),
                    relation: RelationType::Contradicts,
                    target_type: EntityType::Claim,
                    target_id: right.value.claim_id,
                    metadata: BTreeMap::new(),
                },
                Some("repeat-edge"),
                "repeat-edge",
            )
            .unwrap();
        let first = store
            .claim_revise(
                &owner,
                &ClaimReviseInput {
                    claim_id: left.value.claim_id.clone(),
                    if_revision: left.value.revision_id.clone(),
                    statement: "value version one".into(),
                    confidence: None,
                    evidence: vec![],
                    actor: None,
                },
                Some("repeat-first"),
                "repeat-first",
            )
            .unwrap();
        let second = store
            .claim_revise(
                &owner,
                &ClaimReviseInput {
                    claim_id: left.value.claim_id,
                    if_revision: first.value.revision_id.clone(),
                    statement: "value version two".into(),
                    confidence: None,
                    evidence: vec![],
                    actor: None,
                },
                Some("repeat-second"),
                "repeat-second",
            )
            .unwrap();
        assert!(matches!(first.value.status, ClaimStatus::Conflicted));
        assert!(matches!(second.value.status, ClaimStatus::Conflicted));

        let actionable = store
            .conflict_list(
                &owner,
                &ConflictListInput {
                    horizon: Horizon::Ambient,
                    reason: None,
                    include_stale: false,
                    limit: 100,
                    before_case_sequence: None,
                },
            )
            .unwrap();
        assert_eq!(actionable.conflicts.len(), 1);
        assert_eq!(
            actionable.conflicts[0].source.frozen.revision_id,
            second.value.revision_id
        );

        let history = store
            .conflict_list(
                &owner,
                &ConflictListInput {
                    horizon: Horizon::Ambient,
                    reason: None,
                    include_stale: true,
                    limit: 100,
                    before_case_sequence: None,
                },
            )
            .unwrap();
        assert_eq!(history.conflicts.len(), 3);
        assert_eq!(
            history
                .conflicts
                .iter()
                .filter(|case| matches!(case.state, ConflictState::Open))
                .count(),
            1
        );
        assert_eq!(
            history
                .conflicts
                .iter()
                .map(|case| case.case_id.as_str())
                .collect::<BTreeSet<_>>()
                .len(),
            3
        );
        let frozen_revisions = history
            .conflicts
            .iter()
            .map(|case| case.source.frozen.revision_id.as_str())
            .collect::<BTreeSet<_>>();
        assert!(frozen_revisions.contains(left.value.revision_id.as_str()));
        assert!(frozen_revisions.contains(first.value.revision_id.as_str()));
        assert!(frozen_revisions.contains(second.value.revision_id.as_str()));
        assert!(store.verify().unwrap().ok);
    }

    #[test]
    fn conflict_listing_obeys_task_scope_and_exclusive_workspace_pagination() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let task_a = context_with_task("conflict-list-scope", Some("task-a"));
        let task_b = context_with_task("conflict-list-scope", Some("task-b"));
        let mut relation_ids = Vec::new();
        for (owner, prefix) in [(&task_a, "a"), (&task_b, "b")] {
            let left = store
                .claim_assert(
                    owner,
                    &claim(&format!("{prefix} left"), vec![]),
                    Some(&format!("{prefix}-left")),
                    &format!("{prefix}-left"),
                )
                .unwrap();
            let right = store
                .claim_assert(
                    owner,
                    &claim(&format!("{prefix} right"), vec![]),
                    Some(&format!("{prefix}-right")),
                    &format!("{prefix}-right"),
                )
                .unwrap();
            relation_ids.push(
                store
                    .relation_put(
                        owner,
                        &RelationPutInput {
                            source_type: EntityType::Claim,
                            source_id: left.value.claim_id,
                            relation: RelationType::Contradicts,
                            target_type: EntityType::Claim,
                            target_id: right.value.claim_id,
                            metadata: BTreeMap::new(),
                        },
                        Some(&format!("{prefix}-edge")),
                        &format!("{prefix}-edge"),
                    )
                    .unwrap()
                    .value
                    .relation_id,
            );
        }

        let ambient = store
            .conflict_list(
                &task_a,
                &ConflictListInput {
                    horizon: Horizon::Ambient,
                    reason: None,
                    include_stale: false,
                    limit: 100,
                    before_case_sequence: None,
                },
            )
            .unwrap();
        assert_eq!(ambient.conflicts.len(), 1);
        assert_eq!(ambient.conflicts[0].relation_id, relation_ids[0]);

        let first = store
            .conflict_list(
                &task_a,
                &ConflictListInput {
                    horizon: Horizon::Workspace,
                    reason: Some("review the whole workspace".into()),
                    include_stale: false,
                    limit: 1,
                    before_case_sequence: None,
                },
            )
            .unwrap();
        assert_eq!(first.conflicts.len(), 1);
        assert_eq!(first.conflicts[0].relation_id, relation_ids[1]);
        assert!(first.truncated);
        let second = store
            .conflict_list(
                &task_a,
                &ConflictListInput {
                    horizon: Horizon::Workspace,
                    reason: Some("continue workspace review".into()),
                    include_stale: false,
                    limit: 1,
                    before_case_sequence: first.next_before_case_sequence,
                },
            )
            .unwrap();
        assert_eq!(second.conflicts.len(), 1);
        assert_eq!(second.conflicts[0].relation_id, relation_ids[0]);
        assert!(!second.truncated);
    }

    #[test]
    fn conflict_lookup_obeys_task_and_explicit_horizon_scope() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let project = context_with_task("conflict-scope", None);
        let task_a = context_with_task("conflict-scope", Some("task-a"));
        let task_b = context_with_task("conflict-scope", Some("task-b"));
        let shared = store
            .claim_assert(
                &project,
                &claim("shared scoped contradiction probe", vec![]),
                Some("conflict-shared"),
                "conflict-shared",
            )
            .unwrap();
        let private = store
            .claim_assert(
                &task_a,
                &claim("task-a private contradiction probe", vec![]),
                Some("conflict-private"),
                "conflict-private",
            )
            .unwrap();
        store
            .relation_put(
                &task_a,
                &RelationPutInput {
                    source_type: EntityType::Claim,
                    source_id: shared.value.claim_id.clone(),
                    relation: RelationType::Contradicts,
                    target_type: EntityType::Claim,
                    target_id: private.value.claim_id,
                    metadata: BTreeMap::new(),
                },
                Some("conflict-scoped-edge"),
                "conflict-scoped-edge",
            )
            .unwrap();

        let ids = [shared.value.claim_id];
        assert_eq!(
            store
                .conflicts_for_claims(&task_a, Horizon::Ambient, &ids)
                .unwrap()
                .len(),
            1
        );
        assert!(
            store
                .conflicts_for_claims(&task_b, Horizon::Ambient, &ids)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .conflicts_for_claims(&task_b, Horizon::Workspace, &ids)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn relation_listing_is_directional_filtered_and_cursor_paginated() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let mut owner = context("relations");
        owner
            .pins
            .push("memoree://artifact/transient@arev_transient".into());
        let anchor = store
            .artifact_put(
                &owner,
                &artifact("Anchor", "relation anchor"),
                Some("relation-anchor"),
                "relation-anchor",
            )
            .unwrap();
        let first_target = store
            .artifact_put(
                &owner,
                &artifact("First", "first target"),
                Some("relation-first"),
                "relation-first",
            )
            .unwrap();
        let incoming_source = store
            .artifact_put(
                &owner,
                &artifact("Incoming", "incoming source"),
                Some("relation-incoming"),
                "relation-incoming",
            )
            .unwrap();
        let second_target = store
            .artifact_put(
                &owner,
                &artifact("Second", "second target"),
                Some("relation-second"),
                "relation-second",
            )
            .unwrap();

        let first = store
            .relation_put(
                &owner,
                &RelationPutInput {
                    source_type: EntityType::Artifact,
                    source_id: anchor.value.artifact_id.clone(),
                    relation: RelationType::Supports,
                    target_type: EntityType::Artifact,
                    target_id: first_target.value.artifact_id.clone(),
                    metadata: BTreeMap::new(),
                },
                Some("relation-edge-first"),
                "relation-edge-first",
            )
            .unwrap();
        assert!(first.value.context.pins.is_empty());
        let incoming = store
            .relation_put(
                &owner,
                &RelationPutInput {
                    source_type: EntityType::Artifact,
                    source_id: incoming_source.value.artifact_id.clone(),
                    relation: RelationType::References,
                    target_type: EntityType::Artifact,
                    target_id: anchor.value.artifact_id.clone(),
                    metadata: BTreeMap::new(),
                },
                Some("relation-edge-incoming"),
                "relation-edge-incoming",
            )
            .unwrap();
        let second = store
            .relation_put(
                &owner,
                &RelationPutInput {
                    source_type: EntityType::Artifact,
                    source_id: anchor.value.artifact_id.clone(),
                    relation: RelationType::Supports,
                    target_type: EntityType::Artifact,
                    target_id: second_target.value.artifact_id.clone(),
                    metadata: BTreeMap::new(),
                },
                Some("relation-edge-second"),
                "relation-edge-second",
            )
            .unwrap();

        let first_page = store
            .relation_list(
                &owner,
                &RelationListInput {
                    entity_type: EntityType::Artifact,
                    entity_id: anchor.value.artifact_id.clone(),
                    direction: crate::protocol::RelationDirection::Both,
                    relation: None,
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 2,
                    before_commit_seq: None,
                },
            )
            .unwrap();
        assert!(first_page.truncated);
        assert_eq!(first_page.relations.len(), 2);
        assert_eq!(
            first_page.relations[0].relation_id,
            second.value.relation_id
        );
        assert_eq!(
            first_page.relations[1].relation_id,
            incoming.value.relation_id
        );
        assert!(
            first_page
                .relations
                .iter()
                .all(|item| item.context.pins.is_empty())
        );

        let second_page = store
            .relation_list(
                &owner,
                &RelationListInput {
                    entity_type: EntityType::Artifact,
                    entity_id: anchor.value.artifact_id.clone(),
                    direction: crate::protocol::RelationDirection::Both,
                    relation: None,
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 2,
                    before_commit_seq: first_page.next_before_commit_seq,
                },
            )
            .unwrap();
        assert!(!second_page.truncated);
        assert_eq!(second_page.relations.len(), 1);
        assert_eq!(
            second_page.relations[0].relation_id,
            first.value.relation_id
        );

        let outgoing_support = store
            .relation_list(
                &owner,
                &RelationListInput {
                    entity_type: EntityType::Artifact,
                    entity_id: anchor.value.artifact_id.clone(),
                    direction: crate::protocol::RelationDirection::Outgoing,
                    relation: Some(RelationType::Supports),
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 100,
                    before_commit_seq: None,
                },
            )
            .unwrap();
        assert_eq!(outgoing_support.relations.len(), 2);
        assert!(
            outgoing_support
                .relations
                .iter()
                .all(|item| matches!(item.relation, RelationType::Supports))
        );

        let incoming_only = store
            .relation_list(
                &owner,
                &RelationListInput {
                    entity_type: EntityType::Artifact,
                    entity_id: anchor.value.artifact_id,
                    direction: crate::protocol::RelationDirection::Incoming,
                    relation: None,
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 100,
                    before_commit_seq: None,
                },
            )
            .unwrap();
        assert_eq!(incoming_only.relations.len(), 1);
        assert_eq!(
            incoming_only.relations[0].relation_id,
            incoming.value.relation_id
        );
    }

    #[test]
    fn relation_listing_enforces_horizons_and_pins_do_not_grant_graph_access() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let local = context("local");
        let sibling = context("sibling");
        let sibling_anchor = store
            .artifact_put(
                &sibling,
                &artifact("Sibling anchor", "sibling graph anchor"),
                Some("sibling-anchor"),
                "sibling-anchor",
            )
            .unwrap();
        let sibling_target = store
            .artifact_put(
                &sibling,
                &artifact("Sibling target", "sibling graph target"),
                Some("sibling-target"),
                "sibling-target",
            )
            .unwrap();
        store
            .relation_put(
                &sibling,
                &RelationPutInput {
                    source_type: EntityType::Artifact,
                    source_id: sibling_anchor.value.artifact_id.clone(),
                    relation: RelationType::References,
                    target_type: EntityType::Artifact,
                    target_id: sibling_target.value.artifact_id,
                    metadata: BTreeMap::new(),
                },
                Some("sibling-edge"),
                "sibling-edge",
            )
            .unwrap();

        let list = |horizon, reason| RelationListInput {
            entity_type: EntityType::Artifact,
            entity_id: sibling_anchor.value.artifact_id.clone(),
            direction: crate::protocol::RelationDirection::Both,
            relation: None,
            horizon,
            reason,
            limit: 100,
            before_commit_seq: None,
        };
        assert!(matches!(
            store.relation_list(&local, &list(Horizon::Ambient, None)),
            Err(MemoryError::ScopeViolation(_))
        ));

        let mut pinned_local = local.clone();
        pinned_local.pins.push(format!(
            "memoree://artifact/{}@{}",
            sibling_anchor.value.artifact_id, sibling_anchor.value.revision_id
        ));
        let pinned_ambient = store
            .relation_list(&pinned_local, &list(Horizon::Ambient, None))
            .unwrap();
        assert!(pinned_ambient.relations.is_empty());
        assert!(pinned_ambient.broaden_hint.is_some());

        let workspace = store
            .relation_list(
                &local,
                &list(Horizon::Workspace, Some("inspect sibling graph".into())),
            )
            .unwrap();
        assert_eq!(workspace.relations.len(), 1);

        let other_workspace = scoped_context("other-workspace", "foreign", Some("task"));
        let personal_anchor = store
            .artifact_put(
                &other_workspace,
                &artifact("Personal anchor", "personal graph anchor"),
                Some("personal-anchor"),
                "personal-anchor",
            )
            .unwrap();
        let personal_target = store
            .artifact_put(
                &other_workspace,
                &artifact("Personal target", "personal graph target"),
                Some("personal-target"),
                "personal-target",
            )
            .unwrap();
        store
            .relation_put(
                &other_workspace,
                &RelationPutInput {
                    source_type: EntityType::Artifact,
                    source_id: personal_anchor.value.artifact_id.clone(),
                    relation: RelationType::DerivedFrom,
                    target_type: EntityType::Artifact,
                    target_id: personal_target.value.artifact_id,
                    metadata: BTreeMap::new(),
                },
                Some("personal-edge"),
                "personal-edge",
            )
            .unwrap();
        let personal = store
            .relation_list(
                &local,
                &RelationListInput {
                    entity_type: EntityType::Artifact,
                    entity_id: personal_anchor.value.artifact_id,
                    direction: crate::protocol::RelationDirection::Both,
                    relation: None,
                    horizon: Horizon::Personal,
                    reason: Some("inspect personal graph".into()),
                    limit: 100,
                    before_commit_seq: None,
                },
            )
            .unwrap();
        assert_eq!(personal.relations.len(), 1);

        let other_task = context_with_task("local", Some("other-task"));
        let task_anchor = store
            .artifact_put(
                &other_task,
                &artifact("Task anchor", "task graph anchor"),
                Some("task-anchor"),
                "task-anchor",
            )
            .unwrap();
        let task_target = store
            .artifact_put(
                &other_task,
                &artifact("Task target", "task graph target"),
                Some("task-target"),
                "task-target",
            )
            .unwrap();
        store
            .relation_put(
                &other_task,
                &RelationPutInput {
                    source_type: EntityType::Artifact,
                    source_id: task_anchor.value.artifact_id.clone(),
                    relation: RelationType::Supports,
                    target_type: EntityType::Artifact,
                    target_id: task_target.value.artifact_id,
                    metadata: BTreeMap::new(),
                },
                Some("task-edge"),
                "task-edge",
            )
            .unwrap();
        let task_list = RelationListInput {
            entity_type: EntityType::Artifact,
            entity_id: task_anchor.value.artifact_id,
            direction: crate::protocol::RelationDirection::Both,
            relation: None,
            horizon: Horizon::Ambient,
            reason: None,
            limit: 100,
            before_commit_seq: None,
        };
        assert!(matches!(
            store.relation_list(&local, &task_list),
            Err(MemoryError::ScopeViolation(_))
        ));
        let project_level = context_with_task("local", None);
        assert_eq!(
            store
                .relation_list(&project_level, &task_list)
                .unwrap()
                .relations
                .len(),
            1
        );
    }

    #[test]
    fn relation_listing_rejects_invalid_bounds_and_missing_anchors() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let owner = context("relations");
        let anchor = store
            .artifact_put(
                &owner,
                &artifact("Anchor", "bounds anchor"),
                Some("bounds-anchor"),
                "bounds-anchor",
            )
            .unwrap();
        let input = |entity_id: String, limit, before_commit_seq| RelationListInput {
            entity_type: EntityType::Artifact,
            entity_id,
            direction: crate::protocol::RelationDirection::Both,
            relation: None,
            horizon: Horizon::Ambient,
            reason: None,
            limit,
            before_commit_seq,
        };

        assert!(matches!(
            store.relation_list(&owner, &input("missing".into(), 100, None)),
            Err(MemoryError::NotFound(_))
        ));
        assert!(matches!(
            store.relation_list(&owner, &input(anchor.value.artifact_id.clone(), 0, None)),
            Err(MemoryError::InvalidRequest(_))
        ));
        assert!(matches!(
            store.relation_list(
                &owner,
                &input(
                    anchor.value.artifact_id.clone(),
                    MAX_RELATION_LIST_ITEMS + 1,
                    None
                )
            ),
            Err(MemoryError::InvalidRequest(_))
        ));
        assert!(matches!(
            store.relation_list(&owner, &input(anchor.value.artifact_id, 100, Some(0))),
            Err(MemoryError::InvalidRequest(_))
        ));
        assert!(matches!(
            store.relation_list(
                &owner,
                &input("x".repeat(MAX_CONTEXT_ID_BYTES + 1), 100, None)
            ),
            Err(MemoryError::InvalidRequest(_))
        ));
    }

    #[test]
    fn mutation_scope_rejects_siblings_and_does_not_treat_pins_as_write_access() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let local = context("local");
        let foreign = context("foreign");

        let foreign_artifact = store
            .artifact_put(
                &foreign,
                &artifact("Foreign", "foreign scoped evidence"),
                Some("foreign-artifact"),
                "foreign-artifact",
            )
            .unwrap();
        let foreign_claim = store
            .claim_assert(
                &foreign,
                &claim("foreign scoped claim", vec![]),
                Some("foreign-claim"),
                "foreign-claim",
            )
            .unwrap();
        let local_claim = store
            .claim_assert(
                &local,
                &claim("local scoped claim", vec![]),
                Some("local-claim"),
                "local-claim",
            )
            .unwrap();

        let mut pinned_local = local.clone();
        pinned_local.pins.push(format!(
            "memoree://artifact/{}@{}",
            foreign_artifact.value.artifact_id, foreign_artifact.value.revision_id
        ));
        let revise_artifact = ArtifactReviseInput {
            artifact_id: foreign_artifact.value.artifact_id.clone(),
            if_revision: foreign_artifact.value.revision_id.clone(),
            title: None,
            media_type: None,
            content: ArtifactContent::Text("attempted foreign revision".into()),
            provenance: BTreeMap::new(),
            actor: Some("test".into()),
        };
        assert!(matches!(
            store.artifact_revise(
                &pinned_local,
                &revise_artifact,
                Some("foreign-revise"),
                "foreign-revise"
            ),
            Err(MemoryError::ScopeViolation(_))
        ));
        assert!(matches!(
            store.artifact_forget(
                &pinned_local,
                &ArtifactForgetInput {
                    artifact_id: foreign_artifact.value.artifact_id.clone(),
                    reason: "attempted foreign forget".into(),
                },
                Some("foreign-forget"),
                "foreign-forget"
            ),
            Err(MemoryError::ScopeViolation(_))
        ));

        let revise_claim = ClaimReviseInput {
            claim_id: foreign_claim.value.claim_id.clone(),
            if_revision: foreign_claim.value.revision_id.clone(),
            statement: "attempted foreign claim revision".into(),
            confidence: None,
            evidence: vec![],
            actor: Some("test".into()),
        };
        assert!(matches!(
            store.claim_revise(
                &local,
                &revise_claim,
                Some("foreign-claim-revise"),
                "foreign-claim-revise"
            ),
            Err(MemoryError::ScopeViolation(_))
        ));
        assert!(matches!(
            store.claim_retract(
                &local,
                &ClaimRetractInput {
                    claim_id: foreign_claim.value.claim_id.clone(),
                    reason: "attempted foreign retract".into(),
                },
                Some("foreign-retract"),
                "foreign-retract"
            ),
            Err(MemoryError::ScopeViolation(_))
        ));
        assert!(matches!(
            store.relation_put(
                &local,
                &RelationPutInput {
                    source_type: EntityType::Claim,
                    source_id: local_claim.value.claim_id.clone(),
                    relation: RelationType::Supersedes,
                    target_type: EntityType::Claim,
                    target_id: foreign_claim.value.claim_id.clone(),
                    metadata: BTreeMap::new(),
                },
                Some("foreign-link"),
                "foreign-link"
            ),
            Err(MemoryError::ScopeViolation(_))
        ));

        // Exact reads are deliberately global, and every rejected mutation left
        // the foreign entities untouched.
        assert_eq!(
            store
                .artifact_get(&ArtifactGetInput {
                    artifact_id: foreign_artifact.value.artifact_id,
                    revision_id: None,
                    include_content: false,
                })
                .unwrap()
                .status,
            "active"
        );
        assert!(matches!(
            store
                .claim_get(&ClaimGetInput {
                    claim_id: foreign_claim.value.claim_id,
                    revision_id: None,
                })
                .unwrap()
                .status,
            ClaimStatus::Active
        ));

        let other_task = context_with_task("local", Some("other-task"));
        let task_artifact = store
            .artifact_put(
                &other_task,
                &artifact("Other task", "task scoped evidence"),
                Some("task-artifact"),
                "task-artifact",
            )
            .unwrap();
        assert!(matches!(
            store.artifact_forget(
                &local,
                &ArtifactForgetInput {
                    artifact_id: task_artifact.value.artifact_id,
                    reason: "wrong task".into(),
                },
                Some("wrong-task"),
                "wrong-task"
            ),
            Err(MemoryError::ScopeViolation(_))
        ));
    }

    #[test]
    fn evidence_spans_must_have_positive_length() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let ambient = context("one");
        let artifact = store
            .artifact_put(
                &ambient,
                &artifact("Evidence", "some evidence"),
                Some("zero-span-artifact"),
                "zero-span-artifact",
            )
            .unwrap();
        let input = claim(
            "zero length evidence is invalid",
            vec![EvidenceLocator {
                artifact_id: artifact.value.artifact_id,
                revision_id: artifact.value.revision_id,
                start_byte: Some(4),
                end_byte: Some(4),
            }],
        );

        assert!(matches!(
            store.claim_assert(&ambient, &input, Some("zero-span"), "zero-span"),
            Err(MemoryError::InvalidRequest(_))
        ));
    }

    #[test]
    fn temporal_claims_are_current_only_inside_their_validity_window() {
        use chrono::Duration;

        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let ambient = context("temporal");
        let now = Utc::now();

        let mut empty_window = claim("chronometric empty window", vec![]);
        empty_window.valid_from = Some(now);
        empty_window.valid_until = Some(now);
        assert!(matches!(
            store.claim_assert(
                &ambient,
                &empty_window,
                Some("temporal-empty"),
                "temporal-empty"
            ),
            Err(MemoryError::InvalidRequest(_))
        ));

        let mut current_input = claim("chronometric narwhal current", vec![]);
        current_input.valid_from = Some(now - Duration::days(2));
        let current = store
            .claim_assert(
                &ambient,
                &current_input,
                Some("temporal-current"),
                "temporal-current",
            )
            .unwrap();
        let mut future_input = claim("chronometric narwhal future", vec![]);
        future_input.valid_from = Some(now + Duration::days(1));
        let future = store
            .claim_assert(
                &ambient,
                &future_input,
                Some("temporal-future"),
                "temporal-future",
            )
            .unwrap();
        let mut expired_input = claim("chronometric narwhal expired", vec![]);
        expired_input.valid_until = Some(now - Duration::days(1));
        let expired = store
            .claim_assert(
                &ambient,
                &expired_input,
                Some("temporal-expired"),
                "temporal-expired",
            )
            .unwrap();

        let current_only = store
            .search(
                &ambient,
                &SearchInput {
                    query: "chronometric narwhal".into(),
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 10,
                    include_historical: false,
                    min_commit_seq: None,
                    recency: Default::default(),
                },
            )
            .unwrap();
        assert_eq!(current_only.hits.len(), 1);
        assert_eq!(current_only.hits[0].entity_id, current.value.claim_id);
        assert_eq!(current_only.hits[0].provenance["temporal_state"], "current");
        assert_eq!(current_only.hits[0].provenance["is_current"], true);
        assert!(matches!(
            current_only.hits[0].ranking.effective_at_basis,
            RecencyTimestampBasis::ValidFrom
        ));
        assert_eq!(
            current_only.hits[0].ranking.effective_at,
            current_input.valid_from.unwrap()
        );
        assert!(current_only.hits[0].ranking.recency_bonus > 0.0);

        let historical = store
            .search(
                &ambient,
                &SearchInput {
                    query: "chronometric narwhal".into(),
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 10,
                    include_historical: true,
                    min_commit_seq: None,
                    recency: Default::default(),
                },
            )
            .unwrap();
        assert_eq!(historical.hits.len(), 3);
        let future_hit = historical
            .hits
            .iter()
            .find(|hit| hit.entity_id == future.value.claim_id)
            .unwrap();
        assert_eq!(future_hit.provenance["temporal_state"], "future");
        assert_eq!(future_hit.provenance["is_current_revision"], true);
        assert_eq!(future_hit.provenance["is_current"], false);
        assert!(future_hit.provenance["valid_from"].is_string());
        assert!(matches!(
            future_hit.ranking.effective_at_basis,
            RecencyTimestampBasis::ValidFrom
        ));
        assert!(!future_hit.ranking.recency_eligible);
        assert_eq!(future_hit.ranking.recency_bonus, 0.0);
        assert_eq!(
            future_hit.ranking.evaluated_at,
            historical.hits[0].ranking.evaluated_at
        );
        let expired_hit = historical
            .hits
            .iter()
            .find(|hit| hit.entity_id == expired.value.claim_id)
            .unwrap();
        assert_eq!(expired_hit.provenance["temporal_state"], "expired");
        assert_eq!(expired_hit.provenance["is_current"], false);
        assert!(expired_hit.provenance["valid_until"].is_string());

        let revised = store
            .claim_revise(
                &ambient,
                &ClaimReviseInput {
                    claim_id: current.value.claim_id.clone(),
                    if_revision: current.value.revision_id.clone(),
                    statement: "chronometric narwhal current revised".into(),
                    confidence: Some(0.95),
                    evidence: vec![],
                    actor: Some("test".into()),
                },
                Some("temporal-revise"),
                "temporal-revise",
            )
            .unwrap();
        let historical = store
            .search(
                &ambient,
                &SearchInput {
                    query: "chronometric narwhal".into(),
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 10,
                    include_historical: true,
                    min_commit_seq: None,
                    recency: Default::default(),
                },
            )
            .unwrap();
        let old_revision = historical
            .hits
            .iter()
            .find(|hit| hit.revision_id == current.value.revision_id)
            .unwrap();
        assert_eq!(old_revision.provenance["is_current_revision"], false);
        assert_eq!(old_revision.provenance["is_current"], false);
        let new_revision = historical
            .hits
            .iter()
            .find(|hit| hit.revision_id == revised.value.revision_id)
            .unwrap();
        assert_eq!(new_revision.provenance["is_current_revision"], true);
        assert_eq!(new_revision.provenance["is_current"], true);
    }

    #[test]
    fn verification_and_backup_cover_external_blobs() {
        let temporary = tempfile::tempdir().unwrap();
        let data = temporary.path().join("data");
        let backup = temporary.path().join("backup");
        let store = Store::open(&data).unwrap();
        let body = "memory artifact ".repeat(2_000);
        let artifact = store
            .artifact_put(
                &context("one"),
                &artifact("Large", &body),
                Some("large"),
                "large",
            )
            .unwrap();
        assert!(
            !store
                .artifact_get(&ArtifactGetInput {
                    artifact_id: artifact.value.artifact_id.clone(),
                    revision_id: None,
                    include_content: false,
                })
                .unwrap()
                .blob_hash
                .is_empty()
        );
        let verified = store.verify().unwrap();
        assert!(verified.ok, "{:?}", verified.issues);
        assert_eq!(verified.checked_external_blobs, 1);
        let report = store.backup_create(&backup).unwrap();
        assert_eq!(report.commit_seq, artifact.commit_seq);
        let restored = Store::open(&backup).unwrap();
        let restored_artifact = restored
            .artifact_get(&ArtifactGetInput {
                artifact_id: artifact.value.artifact_id,
                revision_id: None,
                include_content: true,
            })
            .unwrap();
        assert!(
            matches!(restored_artifact.content, Some(ArtifactContent::Text(text)) if text == body)
        );
        assert!(restored.verify().unwrap().ok);
    }

    #[test]
    fn backup_refuses_existing_destinations_without_touching_them() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path().join("data")).unwrap();

        let nonempty = temporary.path().join("nonempty-backup");
        fs::create_dir(&nonempty).unwrap();
        let sentinel = nonempty.join("keep-me");
        fs::write(&sentinel, b"original").unwrap();
        let error = store
            .backup_create(&nonempty)
            .expect_err("an existing destination must never be replaced");
        assert!(matches!(error, MemoryError::InvalidRequest(_)));
        assert_eq!(fs::read(&sentinel).unwrap(), b"original");
        assert!(!nonempty.join(MEMOREE_DATABASE_FILE).exists());

        let empty = temporary.path().join("empty-backup");
        fs::create_dir(&empty).unwrap();
        let error = store
            .backup_create(&empty)
            .expect_err("even an empty final destination is ambiguous");
        assert!(matches!(error, MemoryError::InvalidRequest(_)));
        assert_eq!(fs::read_dir(&empty).unwrap().count(), 0);
    }

    #[test]
    fn failed_backup_removes_staging_and_never_publishes() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path().join("data")).unwrap();
        let body = "external memory artifact ".repeat(2_000);
        let artifact = store
            .artifact_put(
                &context("one"),
                &artifact("Corrupt source", &body),
                Some("corrupt-source"),
                "corrupt-source",
            )
            .unwrap();
        let hash = artifact.value.blob_hash;
        let source_blob = store
            .cas()
            .root()
            .join(&hash[..2])
            .join(&hash[2..4])
            .join(&hash);
        fs::write(source_blob, b"deliberately corrupt").unwrap();

        let destination = temporary.path().join("must-not-appear");
        let error = store
            .backup_create(&destination)
            .expect_err("a corrupt staged backup must not be published");
        assert!(matches!(error, MemoryError::Integrity(_)));
        assert!(!destination.exists());

        let staging_entries: Vec<_> = fs::read_dir(temporary.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(".memory-backup-stage-"))
            .collect();
        assert!(
            staging_entries.is_empty(),
            "partial staging directories remain: {staging_entries:?}"
        );
    }

    #[test]
    fn staged_verification_failure_is_cleaned_up() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path().join("data")).unwrap();
        store
            .artifact_put(
                &context("one"),
                &artifact("Broken projection", "this revision must remain indexed"),
                Some("broken-projection"),
                "broken-projection",
            )
            .unwrap();
        store
            .connection
            .lock()
            .execute("DELETE FROM artifact_fts", [])
            .unwrap();

        let destination = temporary.path().join("unverified-backup");
        let error = store
            .backup_create(&destination)
            .expect_err("a snapshot that fails restore-side verification must not publish");
        assert!(
            matches!(error, MemoryError::Integrity(message) if message.contains("staged backup verification failed"))
        );
        assert!(!destination.exists());
        assert!(fs::read_dir(temporary.path()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".memory-backup-stage-")
        }));
    }
}
