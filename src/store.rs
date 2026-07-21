//! SQLite authority and synchronous FTS5 retrieval.
//!
//! Logical entities have mutable heads, but their revision rows are immutable.
//! The FTS tables are derived projections populated in the same transaction as
//! each revision, which provides read-your-writes without an index worker.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use chrono::{DateTime, Utc};
use fs2::FileExt as _;
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
    EntityType, EvidenceLocator, FeedbackExportInput, FeedbackGetInput, FeedbackListInput,
    FeedbackOutcome, FeedbackRecordInput, Horizon, MAX_ARTIFACT_BYTES, MAX_CLAIM_STATEMENT_BYTES,
    MAX_CONFLICT_LIST_ITEMS, MAX_CONTEXT_ID_BYTES, MAX_CONTEXT_PINS, MAX_ENCODED_CONTENT_BYTES,
    MAX_EVIDENCE_ITEMS, MAX_EXTERNAL_ID_BYTES, MAX_FEEDBACK_ITEMS, MAX_FEEDBACK_NOTE_BYTES,
    MAX_HISTORY_ITEMS, MAX_METADATA_BYTES, MAX_PIN_BYTES, MAX_PROJECTION_ITEMS,
    MAX_PROJECTION_SPANS, MAX_PROJECTION_TEXT_BYTES, MAX_QUERY_BYTES, MAX_RECALL_EXCERPT_BYTES,
    MAX_RELATION_LIST_ITEMS, MAX_SEARCH_ITEMS, MAX_SOURCE_CURSOR_BYTES, MAX_TITLE_BYTES,
    ProjectionDropInput, ProjectionListInput, ProjectionPutInput, ProjectionRetrievalStatus,
    ProjectionSpan, QueryAnalysis, QueryScriptProfile, RecencyDecayClass, RecencyTimestampBasis,
    RelationListInput, RelationListItem, RelationListResult, RelationPutInput, RelationType,
    RerankerRetrievalStatus, RetrievalIntentHint, RetrievalProfile, SearchHit, SearchInput,
    SearchRanking, SearchResult, SemanticRetrievalStatus, SourceCheckpointInput, SourceGetInput,
    SourceHealth, SourceIngestInput, SourceRegisterInput, SourceWithdrawInput,
};
use crate::semantic::{
    EligibleSemanticRevision, RERANKER_ORDERING_CANDIDATE_LIMIT, RERANKER_POLICY_VERSION,
    RerankerInstallReport, RerankerManager, SEMANTIC_POLICY_VERSION, SemanticDocument, SemanticHit,
    SemanticInstallReport, SemanticManager,
};

pub const SCHEMA_VERSION: i64 = 5;
pub const MEMOREE_DATABASE_FILE: &str = "memoree.sqlite3";
const SCHEMA_MIGRATION_LOCK_FILE: &str = "schema-migration.lock";
const MIGRATION_BACKUP_DIRECTORY: &str = "migration-backups";
const MAX_KIND_BYTES: usize = 128;
const MAX_MEDIA_TYPE_BYTES: usize = 512;
const MAX_ACTOR_BYTES: usize = 1024;
const MAX_REASON_BYTES: usize = 64 * 1024;
const MAX_RELATION_LIST_ENCODED_BYTES: usize = 8 * 1024 * 1024;
const MAX_CONFLICT_LIST_ENCODED_BYTES: usize = 12 * 1024 * 1024;
const RECENCY_POLICY_VERSION: &str = "bounded_recency_v1";
const RECENCY_MAX_PROMOTION: usize = 2;
const LEXICAL_POLICY_VERSION: &str = "lexical_qualification_v1";
const TRIGRAM_POLICY_VERSION: &str = "trigram_typo_v1";
const FUSION_POLICY_VERSION: &str = "tiered_rrf_v2";
const PROJECTION_POLICY_VERSION: &str = "cited_projection_candidate_v1";
const RRF_K: f64 = 60.0;
const ARTIFACT_CHUNKER_VERSION: i64 = 1;
const ARTIFACT_CHUNK_TARGET_BYTES: usize = 2 * 1024;
const ARTIFACT_CHUNK_MAX_BYTES: usize = 4 * 1024;
const ARTIFACT_CHUNK_MIN_TAIL_BYTES: usize = 256;
const SEMANTIC_WINDOW_MAX_BYTES: usize = 384;
const SEMANTIC_WINDOW_PREFERRED_MIN_BYTES: usize = 324;
const SEMANTIC_WINDOW_OVERLAP_BYTES: usize = 64;
const SEMANTIC_MIN_CANDIDATE_POOL: usize = 32;

#[derive(Clone, Copy, PartialEq, Eq)]
enum SearchQualificationPolicy {
    Broad,
    Qualified,
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;
INSERT OR IGNORE INTO meta(key, value) VALUES
    ('schema_version', '5'),
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

CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    task_id TEXT,
    component TEXT,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    locator TEXT,
    metadata_json TEXT NOT NULL,
    health TEXT NOT NULL CHECK(health IN ('unknown', 'healthy', 'degraded', 'error')),
    cursor TEXT,
    health_message TEXT,
    last_observed_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    commit_seq INTEGER NOT NULL,
    UNIQUE(workspace_id, project_id, task_id, name)
) STRICT;

CREATE TABLE IF NOT EXISTS source_items (
    source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE RESTRICT,
    external_id TEXT NOT NULL,
    external_revision TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    artifact_id TEXT NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT,
    artifact_revision_id TEXT NOT NULL REFERENCES artifact_revisions(id) ON DELETE RESTRICT,
    state TEXT NOT NULL CHECK(state IN ('live', 'withdrawn')),
    observed_at TEXT NOT NULL,
    withdrawn_at TEXT,
    withdrawal_reason TEXT,
    updated_at TEXT NOT NULL,
    commit_seq INTEGER NOT NULL,
    PRIMARY KEY(source_id, external_id)
) STRICT;

CREATE TABLE IF NOT EXISTS retrieval_projections (
    id TEXT PRIMARY KEY,
    artifact_id TEXT NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT,
    revision_id TEXT NOT NULL REFERENCES artifact_revisions(id) ON DELETE RESTRICT,
    projection_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    text TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    generator TEXT NOT NULL,
    generator_version TEXT NOT NULL,
    generator_digest TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('active', 'dropped')),
    dropped_reason TEXT,
    actor TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    commit_seq INTEGER NOT NULL,
    UNIQUE(revision_id, projection_key)
) STRICT;

CREATE TABLE IF NOT EXISTS retrieval_feedback (
    id TEXT PRIMARY KEY,
    workspace_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    task_id TEXT,
    component TEXT,
    outcome TEXT NOT NULL CHECK(outcome IN ('miss', 'useful', 'incorrect', 'stale')),
    query_fingerprint TEXT NOT NULL,
    retained_query TEXT,
    citations_json TEXT NOT NULL,
    note TEXT,
    actor TEXT,
    created_at TEXT NOT NULL,
    commit_seq INTEGER NOT NULL UNIQUE
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
CREATE INDEX IF NOT EXISTS sources_context_idx
    ON sources(workspace_id, project_id, task_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS source_items_artifact_idx
    ON source_items(artifact_id, state);
CREATE INDEX IF NOT EXISTS projections_artifact_idx
    ON retrieval_projections(artifact_id, revision_id, status);
CREATE INDEX IF NOT EXISTS feedback_context_idx
    ON retrieval_feedback(workspace_id, project_id, task_id, commit_seq DESC);

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
CREATE VIRTUAL TABLE IF NOT EXISTS artifact_trigram_fts USING fts5(
    revision_id UNINDEXED,
    artifact_id UNINDEXED,
    title,
    body,
    tokenize = 'trigram case_sensitive 0 remove_diacritics 1'
);
CREATE VIRTUAL TABLE IF NOT EXISTS claim_trigram_fts USING fts5(
    revision_id UNINDEXED,
    claim_id UNINDEXED,
    statement,
    tokenize = 'trigram case_sensitive 0 remove_diacritics 1'
);
CREATE VIRTUAL TABLE IF NOT EXISTS artifact_trigram_vocab
USING fts5vocab(artifact_trigram_fts, 'row');
CREATE VIRTUAL TABLE IF NOT EXISTS claim_trigram_vocab
USING fts5vocab(claim_trigram_fts, 'row');
-- This is a private, rebuildable projection. Body rows contain exact slices
-- of the immutable artifact bytes; title rows are deliberately spanless so a
-- title-only match can never manufacture an arbitrary body citation.
CREATE VIRTUAL TABLE IF NOT EXISTS artifact_chunk_fts USING fts5(
    revision_id UNINDEXED,
    artifact_id UNINDEXED,
    row_kind UNINDEXED,
    ordinal UNINDEXED,
    start_byte UNINDEXED,
    end_byte UNINDEXED,
    chunker_version UNINDEXED,
    title,
    body,
    tokenize = 'unicode61 remove_diacritics 2'
);
-- Candidate-only derived text. Rows always map back to one immutable raw
-- artifact revision and can never qualify a search result by themselves.
CREATE VIRTUAL TABLE IF NOT EXISTS projection_fts USING fts5(
    projection_id UNINDEXED,
    revision_id UNINDEXED,
    artifact_id UNINDEXED,
    text,
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
CREATE TRIGGER IF NOT EXISTS artifact_revision_trigram_index
AFTER INSERT ON artifact_revisions BEGIN
    INSERT INTO artifact_trigram_fts(revision_id, artifact_id, title, body)
    VALUES (new.id, new.artifact_id, new.title, new.search_text);
END;
CREATE TRIGGER IF NOT EXISTS claim_revision_trigram_index
AFTER INSERT ON claim_revisions BEGIN
    INSERT INTO claim_trigram_fts(revision_id, claim_id, statement)
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
    semantic: SemanticManager,
    reranker: RerankerManager,
    schema_migration: Option<SchemaMigrationReport>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProbeEvidenceResolution {
    pub start_byte: u64,
    pub end_byte: u64,
    pub source_revision_hash: String,
    pub locator_policy_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProbeEvidenceResolutionSet {
    pub windows: Vec<ProbeEvidenceResolution>,
    pub complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SchemaMigrationReport {
    pub from_schema: i64,
    pub to_schema: i64,
    pub backup_destination: String,
    pub copied_external_blobs: usize,
    pub completed_at: DateTime<Utc>,
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
pub struct SourceRecord {
    pub source_id: String,
    pub name: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locator: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
    pub health: SourceHealth,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_observed_at: Option<DateTime<Utc>>,
    pub context: AmbientContext,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub commit_seq: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceItemRecord {
    pub source_id: String,
    pub external_id: String,
    pub external_revision: String,
    pub artifact_id: String,
    pub artifact_revision_id: String,
    pub state: String,
    pub observed_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub withdrawn_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub withdrawal_reason: Option<String>,
    pub updated_at: DateTime<Utc>,
    pub commit_seq: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceIngestRecord {
    pub source: SourceRecord,
    pub item: SourceItemRecord,
    pub artifact: ArtifactRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SourceWithdrawRecord {
    pub source: SourceRecord,
    pub item: SourceItemRecord,
    pub artifact: ArtifactRecord,
    pub erasure_performed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProjectionRecord {
    pub projection_id: String,
    pub artifact_id: String,
    pub revision_id: String,
    pub projection_key: String,
    pub kind: String,
    pub text: String,
    pub evidence_spans: Vec<ProjectionSpan>,
    pub generator: String,
    pub generator_version: String,
    pub generator_digest: String,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dropped_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub commit_seq: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProjectionListResult {
    pub artifact_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
    pub projections: Vec<ProjectionRecord>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FeedbackRecord {
    pub feedback_id: String,
    pub outcome: FeedbackOutcome,
    pub query_fingerprint: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retained_query: Option<String>,
    #[serde(default)]
    pub citations: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    pub context: AmbientContext,
    pub created_at: DateTime<Utc>,
    pub commit_seq: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FeedbackListResult {
    pub feedback: Vec<FeedbackRecord>,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_before_commit_seq: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FeedbackEvalCase {
    pub feedback_id: String,
    pub query: String,
    pub outcome: FeedbackOutcome,
    pub citations: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub commit_seq: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct FeedbackExportResult {
    pub schema_version: u32,
    pub format: String,
    pub cases: Vec<FeedbackEvalCase>,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_before_commit_seq: Option<i64>,
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
    pub fn inspect_schema_version(data_dir: impl AsRef<Path>) -> Result<Option<i64>> {
        let database = data_dir.as_ref().join(MEMOREE_DATABASE_FILE);
        if !database.is_file() {
            return Ok(None);
        }
        let connection =
            Connection::open_with_flags(database, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        preflight_schema_version(&connection)
    }

    /// Open a self-contained data directory (`memoree.sqlite3` plus `blobs/`).
    pub fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let data_dir = data_dir.as_ref();
        ensure_private_directory(data_dir)?;
        Self::open_paths(data_dir.join(MEMOREE_DATABASE_FILE), data_dir.join("blobs"))
    }

    pub fn open_paths(db_path: impl AsRef<Path>, blob_dir: impl AsRef<Path>) -> Result<Self> {
        let db_path = db_path.as_ref().to_path_buf();
        let parent = db_path
            .parent()
            .ok_or_else(|| MemoryError::Config("database path has no parent".into()))?;
        ensure_private_directory(parent)?;
        let migration_lock = open_private_migration_lock(&parent.join(SCHEMA_MIGRATION_LOCK_FILE))?;
        migration_lock.lock_exclusive().map_err(|error| {
            MemoryError::Config(format!(
                "could not acquire schema migration lock for {}: {error}",
                db_path.display()
            ))
        })?;
        let mut connection = Connection::open(&db_path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        let existing_schema_version = preflight_schema_version(&connection)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        let schema_migration = match existing_schema_version {
            Some(version) if version < SCHEMA_VERSION => Some(create_pre_migration_backup(
                &connection,
                &db_path,
                blob_dir.as_ref(),
                version,
            )?),
            _ => None,
        };
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
            semantic: SemanticManager::new(parent),
            reranker: RerankerManager::new(parent),
            db_path,
            schema_migration,
        })
    }

    pub fn database_path(&self) -> &Path {
        &self.db_path
    }

    pub fn cas(&self) -> &Cas {
        &self.cas
    }

    pub fn schema_migration(&self) -> Option<&SchemaMigrationReport> {
        self.schema_migration.as_ref()
    }

    pub fn schema_version(&self) -> Result<i64> {
        let connection = self.connection.lock();
        Ok(connection.query_row(
            "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )?)
    }

    pub fn last_commit_seq(&self) -> Result<i64> {
        let connection = self.connection.lock();
        Ok(connection.query_row(
            "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'commit_seq'",
            [],
            |row| row.get(0),
        )?)
    }

    pub fn source_register(
        &self,
        context: &AmbientContext,
        input: &SourceRegisterInput,
        idempotency_key: Option<&str>,
        request_hash: &str,
    ) -> Result<MutationResult<SourceRecord>> {
        validate_context(context)?;
        require_bounded("source name", &input.name, MAX_TITLE_BYTES)?;
        require_bounded("source kind", &input.kind, MAX_KIND_BYTES)?;
        validate_optional_size(
            "source locator",
            input.locator.as_deref(),
            MAX_METADATA_BYTES,
        )?;
        validate_serialized_size("source metadata", &input.metadata, MAX_METADATA_BYTES)?;
        validate_optional_size("actor", input.actor.as_deref(), MAX_ACTOR_BYTES)?;

        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(replay) = idempotency_replay::<SourceRecord>(
            &transaction,
            idempotency_key,
            request_hash,
            "source.register",
        )? {
            return Ok(replay);
        }
        let source_id = new_id("src");
        let now = Utc::now();
        let commit_seq = next_commit_seq(&transaction)?;
        transaction.execute(
            "INSERT INTO sources (
                id, workspace_id, project_id, task_id, component, name, kind,
                locator, metadata_json, health, created_at, updated_at, commit_seq
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'unknown', ?10, ?10, ?11)",
            params![
                source_id,
                context.workspace_id,
                context.project_id,
                context.task_id,
                context.component,
                input.name,
                input.kind,
                input.locator,
                serde_json::to_string(&input.metadata)?,
                now,
                commit_seq,
            ],
        )?;
        let record = SourceRecord {
            source_id: source_id.clone(),
            name: input.name.clone(),
            kind: input.kind.clone(),
            locator: input.locator.clone(),
            metadata: input.metadata.clone(),
            health: SourceHealth::Unknown,
            cursor: None,
            health_message: None,
            last_observed_at: None,
            context: context.clone(),
            created_at: now,
            updated_at: now,
            commit_seq,
        };
        append_event(
            &transaction,
            commit_seq,
            "source.register",
            "source",
            &source_id,
            None,
            input.actor.as_deref(),
            &json!({"kind": input.kind, "locator": input.locator}),
        )?;
        record_idempotency(
            &transaction,
            idempotency_key,
            request_hash,
            "source.register",
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

    pub fn source_get(
        &self,
        context: &AmbientContext,
        input: &SourceGetInput,
    ) -> Result<SourceRecord> {
        validate_context(context)?;
        require_bounded("source_id", &input.source_id, MAX_CONTEXT_ID_BYTES)?;
        let connection = self.connection.lock();
        let record = load_source(&connection, &input.source_id)?
            .ok_or_else(|| MemoryError::NotFound(format!("source {}", input.source_id)))?;
        ensure_write_scope(context, &record.context, "source", &input.source_id)?;
        Ok(record)
    }

    pub fn source_checkpoint(
        &self,
        context: &AmbientContext,
        input: &SourceCheckpointInput,
        idempotency_key: Option<&str>,
        request_hash: &str,
    ) -> Result<MutationResult<SourceRecord>> {
        validate_context(context)?;
        require_bounded("source_id", &input.source_id, MAX_CONTEXT_ID_BYTES)?;
        validate_optional_size("cursor", input.cursor.as_deref(), MAX_SOURCE_CURSOR_BYTES)?;
        validate_optional_size("health message", input.message.as_deref(), MAX_REASON_BYTES)?;
        validate_optional_size("actor", input.actor.as_deref(), MAX_ACTOR_BYTES)?;
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let previous = load_source(&transaction, &input.source_id)?
            .ok_or_else(|| MemoryError::NotFound(format!("source {}", input.source_id)))?;
        ensure_write_scope(context, &previous.context, "source", &input.source_id)?;
        if let Some(replay) = idempotency_replay::<SourceRecord>(
            &transaction,
            idempotency_key,
            request_hash,
            "source.checkpoint",
        )? {
            return Ok(replay);
        }
        let now = Utc::now();
        let observed_at = input.observed_at.unwrap_or(now);
        let commit_seq = next_commit_seq(&transaction)?;
        let health = enum_string(&input.health)?;
        transaction.execute(
            "UPDATE sources
                SET health = ?1, cursor = COALESCE(?2, cursor), health_message = ?3,
                    last_observed_at = ?4, updated_at = ?5, commit_seq = ?6
              WHERE id = ?7",
            params![
                health,
                input.cursor,
                input.message,
                observed_at,
                now,
                commit_seq,
                input.source_id,
            ],
        )?;
        let record = load_source(&transaction, &input.source_id)?.ok_or_else(|| {
            MemoryError::Integrity(format!(
                "source {} vanished during checkpoint",
                input.source_id
            ))
        })?;
        append_event(
            &transaction,
            commit_seq,
            "source.checkpoint",
            "source",
            &input.source_id,
            None,
            input.actor.as_deref(),
            &json!({"health": input.health, "cursor_changed": input.cursor.is_some()}),
        )?;
        record_idempotency(
            &transaction,
            idempotency_key,
            request_hash,
            "source.checkpoint",
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

    pub fn source_ingest(
        &self,
        context: &AmbientContext,
        input: &SourceIngestInput,
        idempotency_key: Option<&str>,
        request_hash: &str,
    ) -> Result<MutationResult<SourceIngestRecord>> {
        validate_context(context)?;
        require_bounded("source_id", &input.source_id, MAX_CONTEXT_ID_BYTES)?;
        require_bounded("external_id", &input.external_id, MAX_EXTERNAL_ID_BYTES)?;
        require_bounded(
            "external_revision",
            &input.external_revision,
            MAX_EXTERNAL_ID_BYTES,
        )?;
        validate_artifact_input(&input.kind, &input.title, &input.media_type)?;
        validate_serialized_size("provenance", &input.provenance, MAX_METADATA_BYTES)?;
        validate_optional_size("cursor", input.cursor.as_deref(), MAX_SOURCE_CURSOR_BYTES)?;
        validate_optional_size("actor", input.actor.as_deref(), MAX_ACTOR_BYTES)?;
        let bytes = content_bytes(&input.content)?;
        let search_text = searchable_text(&input.media_type, &bytes);
        let blob = self.cas.put(&bytes)?;
        let observed_at = input.observed_at.unwrap_or_else(Utc::now);
        let mut provenance = input.provenance.clone();
        provenance.insert(
            "memoree_source".into(),
            json!({
                "source_id": input.source_id,
                "external_id": input.external_id,
                "external_revision": input.external_revision,
                "observed_at": observed_at,
            }),
        );
        validate_serialized_size("enriched provenance", &provenance, MAX_METADATA_BYTES)?;
        let payload_digest = source_payload_digest(
            &input.kind,
            &input.title,
            &input.media_type,
            &blob.hash,
            &input.provenance,
        )?;

        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let source = load_source(&transaction, &input.source_id)?
            .ok_or_else(|| MemoryError::NotFound(format!("source {}", input.source_id)))?;
        ensure_write_scope(context, &source.context, "source", &input.source_id)?;
        if let Some(replay) = idempotency_replay::<SourceIngestRecord>(
            &transaction,
            idempotency_key,
            request_hash,
            "source.ingest",
        )? {
            return Ok(replay);
        }

        let previous_item = load_source_item(&transaction, &input.source_id, &input.external_id)?;
        if let Some(previous) = &previous_item
            && previous.state == "live"
            && previous.external_revision == input.external_revision
        {
            if previous.payload_digest != payload_digest {
                return Err(MemoryError::RevisionConflict {
                    entity_type: "source_item",
                    entity_id: format!("{}:{}", input.source_id, input.external_id),
                    current_revision: previous.external_revision.clone(),
                    requested_revision: input.external_revision.clone(),
                });
            }
            let artifact = load_artifact_raw(
                &transaction,
                &previous.artifact_id,
                Some(&previous.artifact_revision_id),
            )?
            .ok_or_else(|| {
                MemoryError::Integrity(format!(
                    "source item {}:{} references a missing artifact revision",
                    input.source_id, input.external_id
                ))
            })?
            .into_record(&self.cas, false)?;
            let value = SourceIngestRecord {
                source,
                item: previous.clone().into_record(),
                artifact,
            };
            record_idempotency(
                &transaction,
                idempotency_key,
                request_hash,
                "source.ingest",
                &value,
                previous.commit_seq,
            )?;
            transaction.commit()?;
            return Ok(MutationResult {
                value,
                commit_seq: previous.commit_seq,
                created: false,
            });
        }

        let now = Utc::now();
        let commit_seq = next_commit_seq(&transaction)?;
        let provenance_json = serde_json::to_string(&provenance)?;
        let (artifact_id, revision_id, revision_number, artifact_created_at, created) =
            if let Some(previous) = &previous_item
                && previous.state == "live"
            {
                let head = load_artifact_raw(&transaction, &previous.artifact_id, None)?
                    .ok_or_else(|| {
                        MemoryError::Integrity(format!(
                            "source item {}:{} references missing artifact {}",
                            input.source_id, input.external_id, previous.artifact_id
                        ))
                    })?;
                ensure_write_scope(context, &head.context, "artifact", &head.artifact_id)?;
                if head.status != "active" {
                    return Err(MemoryError::Integrity(format!(
                        "live source item {}:{} points to forgotten artifact {}",
                        input.source_id, input.external_id, head.artifact_id
                    )));
                }
                if head.kind != input.kind {
                    return Err(MemoryError::InvalidRequest(format!(
                        "source item kind is immutable (stored {}, received {})",
                        head.kind, input.kind
                    )));
                }
                let revision_id = new_id("arev");
                let revision_number = head.revision_number + 1;
                transaction.execute(
                    "INSERT INTO artifact_revisions (
                        id, artifact_id, revision_number, title, media_type, blob_hash,
                        blob_size, inline_blob, search_text, provenance_json, actor,
                        created_at, commit_seq
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                    params![
                        revision_id,
                        head.artifact_id,
                        revision_number,
                        input.title,
                        input.media_type,
                        blob.hash,
                        i64::try_from(blob.size_bytes).map_err(|_| MemoryError::ContentTooLarge)?,
                        blob.inline_bytes,
                        search_text,
                        provenance_json,
                        input.actor,
                        now,
                        commit_seq,
                    ],
                )?;
                index_artifact_revision_chunks(
                    &transaction,
                    &revision_id,
                    &head.artifact_id,
                    &input.title,
                    &search_text,
                )?;
                transaction.execute(
                    "UPDATE artifacts SET current_revision_id = ?1, updated_at = ?2 WHERE id = ?3",
                    params![revision_id, now, head.artifact_id],
                )?;
                (
                    head.artifact_id,
                    revision_id,
                    revision_number,
                    head.created_at,
                    false,
                )
            } else {
                let artifact_id = new_id("art");
                let revision_id = new_id("arev");
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
                        provenance_json,
                        input.actor,
                        now,
                        commit_seq,
                    ],
                )?;
                index_artifact_revision_chunks(
                    &transaction,
                    &revision_id,
                    &artifact_id,
                    &input.title,
                    &search_text,
                )?;
                (artifact_id, revision_id, 1, now, true)
            };

        transaction.execute(
            "INSERT INTO source_items (
                source_id, external_id, external_revision, payload_digest,
                artifact_id, artifact_revision_id, state, observed_at,
                updated_at, commit_seq
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'live', ?7, ?8, ?9)
             ON CONFLICT(source_id, external_id) DO UPDATE SET
                external_revision = excluded.external_revision,
                payload_digest = excluded.payload_digest,
                artifact_id = excluded.artifact_id,
                artifact_revision_id = excluded.artifact_revision_id,
                state = 'live', observed_at = excluded.observed_at,
                withdrawn_at = NULL, withdrawal_reason = NULL,
                updated_at = excluded.updated_at, commit_seq = excluded.commit_seq",
            params![
                input.source_id,
                input.external_id,
                input.external_revision,
                payload_digest,
                artifact_id,
                revision_id,
                observed_at,
                now,
                commit_seq,
            ],
        )?;
        transaction.execute(
            "UPDATE sources
                SET health = 'healthy', cursor = COALESCE(?1, cursor),
                    health_message = NULL, last_observed_at = ?2,
                    updated_at = ?3, commit_seq = ?4
              WHERE id = ?5",
            params![input.cursor, observed_at, now, commit_seq, input.source_id],
        )?;
        append_event(
            &transaction,
            commit_seq,
            "source.ingest",
            "source_item",
            &format!("{}:{}", input.source_id, input.external_id),
            Some(&revision_id),
            input.actor.as_deref(),
            &json!({
                "source_id": input.source_id,
                "external_id": input.external_id,
                "external_revision": input.external_revision,
                "artifact_id": artifact_id,
                "artifact_revision_id": revision_id,
            }),
        )?;
        let source = load_source(&transaction, &input.source_id)?.ok_or_else(|| {
            MemoryError::Integrity(format!("source {} vanished during ingest", input.source_id))
        })?;
        let item = load_source_item(&transaction, &input.source_id, &input.external_id)?
            .ok_or_else(|| MemoryError::Integrity("source item vanished during ingest".into()))?
            .into_record();
        let artifact = ArtifactRecord {
            artifact_id: artifact_id.clone(),
            revision_id: revision_id.clone(),
            revision_number,
            kind: input.kind.clone(),
            title: input.title.clone(),
            media_type: input.media_type.clone(),
            content: None,
            blob_hash: blob.hash,
            size_bytes: blob.size_bytes,
            status: "active".into(),
            context: context.clone(),
            provenance,
            actor: input.actor.clone(),
            created_at: artifact_created_at,
            revision_created_at: now,
            commit_seq,
            forgotten_reason: None,
        };
        let value = SourceIngestRecord {
            source,
            item,
            artifact,
        };
        record_idempotency(
            &transaction,
            idempotency_key,
            request_hash,
            "source.ingest",
            &value,
            commit_seq,
        )?;
        transaction.commit()?;
        Ok(MutationResult {
            value,
            commit_seq,
            created,
        })
    }

    pub fn source_withdraw(
        &self,
        context: &AmbientContext,
        input: &SourceWithdrawInput,
        idempotency_key: Option<&str>,
        request_hash: &str,
    ) -> Result<MutationResult<SourceWithdrawRecord>> {
        validate_context(context)?;
        require_bounded("source_id", &input.source_id, MAX_CONTEXT_ID_BYTES)?;
        require_bounded("external_id", &input.external_id, MAX_EXTERNAL_ID_BYTES)?;
        require_bounded("reason", &input.reason, MAX_REASON_BYTES)?;
        validate_optional_size("actor", input.actor.as_deref(), MAX_ACTOR_BYTES)?;
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let source = load_source(&transaction, &input.source_id)?
            .ok_or_else(|| MemoryError::NotFound(format!("source {}", input.source_id)))?;
        ensure_write_scope(context, &source.context, "source", &input.source_id)?;
        if let Some(replay) = idempotency_replay::<SourceWithdrawRecord>(
            &transaction,
            idempotency_key,
            request_hash,
            "source.withdraw",
        )? {
            return Ok(replay);
        }
        let previous = load_source_item(&transaction, &input.source_id, &input.external_id)?
            .ok_or_else(|| {
                MemoryError::NotFound(format!(
                    "source item {}:{}",
                    input.source_id, input.external_id
                ))
            })?;
        let head =
            load_artifact_raw(&transaction, &previous.artifact_id, None)?.ok_or_else(|| {
                MemoryError::Integrity(format!(
                    "source item {}:{} references missing artifact {}",
                    input.source_id, input.external_id, previous.artifact_id
                ))
            })?;
        ensure_write_scope(context, &head.context, "artifact", &head.artifact_id)?;
        if previous.state == "withdrawn" {
            let value = SourceWithdrawRecord {
                source,
                item: previous.clone().into_record(),
                artifact: head.into_record(&self.cas, false)?,
                erasure_performed: false,
            };
            record_idempotency(
                &transaction,
                idempotency_key,
                request_hash,
                "source.withdraw",
                &value,
                previous.commit_seq,
            )?;
            transaction.commit()?;
            return Ok(MutationResult {
                value,
                commit_seq: previous.commit_seq,
                created: false,
            });
        }
        let now = Utc::now();
        let observed_at = input.observed_at.unwrap_or(now);
        let commit_seq = next_commit_seq(&transaction)?;
        transaction.execute(
            "UPDATE artifacts
                SET status = 'forgotten', forgotten_reason = ?1, updated_at = ?2
              WHERE id = ?3",
            params![input.reason, now, previous.artifact_id],
        )?;
        transaction.execute(
            "UPDATE source_items
                SET state = 'withdrawn', withdrawn_at = ?1, withdrawal_reason = ?2,
                    observed_at = ?1, updated_at = ?3, commit_seq = ?4
              WHERE source_id = ?5 AND external_id = ?6",
            params![
                observed_at,
                input.reason,
                now,
                commit_seq,
                input.source_id,
                input.external_id,
            ],
        )?;
        transaction.execute(
            "UPDATE sources
                SET last_observed_at = ?1, updated_at = ?2, commit_seq = ?3
              WHERE id = ?4",
            params![observed_at, now, commit_seq, input.source_id],
        )?;
        append_event(
            &transaction,
            commit_seq,
            "source.withdraw",
            "source_item",
            &format!("{}:{}", input.source_id, input.external_id),
            Some(&previous.artifact_revision_id),
            input.actor.as_deref(),
            &json!({
                "reason": input.reason,
                "artifact_id": previous.artifact_id,
                "erasure_performed": false,
            }),
        )?;
        let mut artifact = head.into_record(&self.cas, false)?;
        artifact.status = "forgotten".into();
        artifact.forgotten_reason = Some(input.reason.clone());
        let value = SourceWithdrawRecord {
            source: load_source(&transaction, &input.source_id)?.ok_or_else(|| {
                MemoryError::Integrity(format!(
                    "source {} vanished during withdrawal",
                    input.source_id
                ))
            })?,
            item: load_source_item(&transaction, &input.source_id, &input.external_id)?
                .ok_or_else(|| {
                    MemoryError::Integrity("source item vanished during withdrawal".into())
                })?
                .into_record(),
            artifact,
            erasure_performed: false,
        };
        record_idempotency(
            &transaction,
            idempotency_key,
            request_hash,
            "source.withdraw",
            &value,
            commit_seq,
        )?;
        transaction.commit()?;
        Ok(MutationResult {
            value,
            commit_seq,
            created: true,
        })
    }

    pub fn projection_put(
        &self,
        context: &AmbientContext,
        input: &ProjectionPutInput,
        idempotency_key: Option<&str>,
        request_hash: &str,
    ) -> Result<MutationResult<ProjectionRecord>> {
        validate_context(context)?;
        require_bounded("artifact_id", &input.artifact_id, MAX_CONTEXT_ID_BYTES)?;
        require_bounded("revision_id", &input.revision_id, MAX_CONTEXT_ID_BYTES)?;
        require_bounded(
            "projection_key",
            &input.projection_key,
            MAX_EXTERNAL_ID_BYTES,
        )?;
        require_bounded("projection kind", &input.kind, MAX_KIND_BYTES)?;
        require_bounded("projection text", &input.text, MAX_PROJECTION_TEXT_BYTES)?;
        require_bounded("generator", &input.generator, MAX_TITLE_BYTES)?;
        require_bounded(
            "generator_version",
            &input.generator_version,
            MAX_EXTERNAL_ID_BYTES,
        )?;
        require_bounded(
            "generator_digest",
            &input.generator_digest,
            MAX_EXTERNAL_ID_BYTES,
        )?;
        validate_serialized_size("projection metadata", &input.metadata, MAX_METADATA_BYTES)?;
        validate_optional_size("actor", input.actor.as_deref(), MAX_ACTOR_BYTES)?;
        if input.evidence_spans.is_empty() || input.evidence_spans.len() > MAX_PROJECTION_SPANS {
            return Err(MemoryError::InvalidRequest(format!(
                "evidence_spans must contain 1..={MAX_PROJECTION_SPANS} items"
            )));
        }

        let raw = {
            let connection = self.connection.lock();
            load_artifact_raw(&connection, &input.artifact_id, Some(&input.revision_id))?
                .ok_or_else(|| {
                    MemoryError::NotFound(format!(
                        "artifact {} revision {}",
                        input.artifact_id, input.revision_id
                    ))
                })?
        };
        ensure_write_scope(context, &raw.context, "artifact", &input.artifact_id)?;
        if raw.status != "active" {
            return Err(MemoryError::InvalidRequest(format!(
                "artifact {} is not active",
                input.artifact_id
            )));
        }
        let current_revision = {
            let connection = self.connection.lock();
            load_artifact_raw(&connection, &input.artifact_id, None)?
                .ok_or_else(|| MemoryError::NotFound(format!("artifact {}", input.artifact_id)))?
                .revision_id
        };
        if current_revision != input.revision_id {
            return Err(MemoryError::RevisionConflict {
                entity_type: "artifact",
                entity_id: input.artifact_id.clone(),
                current_revision,
                requested_revision: input.revision_id.clone(),
            });
        }
        validate_projection_spans(&input.evidence_spans, raw.blob.size_bytes)?;
        let payload_digest = projection_payload_digest(input)?;

        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let transactional_raw = load_artifact_raw(&transaction, &input.artifact_id, None)?
            .ok_or_else(|| MemoryError::NotFound(format!("artifact {}", input.artifact_id)))?;
        ensure_write_scope(
            context,
            &transactional_raw.context,
            "artifact",
            &input.artifact_id,
        )?;
        if transactional_raw.status != "active" {
            return Err(MemoryError::InvalidRequest(format!(
                "artifact {} is not active",
                input.artifact_id
            )));
        }
        if transactional_raw.revision_id != input.revision_id {
            return Err(MemoryError::RevisionConflict {
                entity_type: "artifact",
                entity_id: input.artifact_id.clone(),
                current_revision: transactional_raw.revision_id,
                requested_revision: input.revision_id.clone(),
            });
        }
        validate_projection_spans(&input.evidence_spans, transactional_raw.blob.size_bytes)?;
        if let Some(replay) = idempotency_replay::<ProjectionRecord>(
            &transaction,
            idempotency_key,
            request_hash,
            "projection.put",
        )? {
            return Ok(replay);
        }
        let existing =
            load_projection_by_key(&transaction, &input.revision_id, &input.projection_key)?;
        if let Some(existing_projection) = existing.as_ref()
            && existing_projection.status == "active"
            && existing_projection.payload_digest == payload_digest
        {
            let record = existing_projection.clone().into_record()?;
            record_idempotency(
                &transaction,
                idempotency_key,
                request_hash,
                "projection.put",
                &record,
                record.commit_seq,
            )?;
            transaction.commit()?;
            return Ok(MutationResult {
                commit_seq: record.commit_seq,
                value: record,
                created: false,
            });
        }

        let projection_id = existing
            .as_ref()
            .map(|projection| projection.projection_id.clone())
            .unwrap_or_else(|| new_id("proj"));
        let created_at = existing
            .as_ref()
            .map(|projection| projection.created_at)
            .unwrap_or_else(Utc::now);
        let now = Utc::now();
        let commit_seq = next_commit_seq(&transaction)?;
        transaction.execute(
            "INSERT INTO retrieval_projections (
                id, artifact_id, revision_id, projection_key, kind, text,
                evidence_json, generator, generator_version, generator_digest,
                payload_digest, metadata_json, status, actor, created_at,
                updated_at, commit_seq
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                       'active', ?13, ?14, ?15, ?16)
             ON CONFLICT(revision_id, projection_key) DO UPDATE SET
                kind = excluded.kind, text = excluded.text,
                evidence_json = excluded.evidence_json,
                generator = excluded.generator,
                generator_version = excluded.generator_version,
                generator_digest = excluded.generator_digest,
                payload_digest = excluded.payload_digest,
                metadata_json = excluded.metadata_json,
                status = 'active', dropped_reason = NULL, actor = excluded.actor,
                updated_at = excluded.updated_at, commit_seq = excluded.commit_seq",
            params![
                projection_id,
                input.artifact_id,
                input.revision_id,
                input.projection_key,
                input.kind,
                input.text,
                serde_json::to_string(&input.evidence_spans)?,
                input.generator,
                input.generator_version,
                input.generator_digest,
                payload_digest,
                serde_json::to_string(&input.metadata)?,
                input.actor,
                created_at,
                now,
                commit_seq,
            ],
        )?;
        transaction.execute(
            "DELETE FROM projection_fts WHERE projection_id = ?1",
            [&projection_id],
        )?;
        transaction.execute(
            "INSERT INTO projection_fts(projection_id, revision_id, artifact_id, text)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                projection_id,
                input.revision_id,
                input.artifact_id,
                input.text,
            ],
        )?;
        append_event(
            &transaction,
            commit_seq,
            "projection.put",
            "projection",
            &projection_id,
            Some(&input.revision_id),
            input.actor.as_deref(),
            &json!({
                "artifact_id": input.artifact_id,
                "projection_key": input.projection_key,
                "kind": input.kind,
                "generator": input.generator,
                "generator_version": input.generator_version,
            }),
        )?;
        let record = load_projection(&transaction, &projection_id)?
            .ok_or_else(|| MemoryError::Integrity("projection vanished during put".into()))?
            .into_record()?;
        record_idempotency(
            &transaction,
            idempotency_key,
            request_hash,
            "projection.put",
            &record,
            commit_seq,
        )?;
        transaction.commit()?;
        Ok(MutationResult {
            value: record,
            commit_seq,
            created: existing.is_none(),
        })
    }

    pub fn projection_list(
        &self,
        context: &AmbientContext,
        input: &ProjectionListInput,
    ) -> Result<ProjectionListResult> {
        validate_context(context)?;
        require_bounded("artifact_id", &input.artifact_id, MAX_CONTEXT_ID_BYTES)?;
        validate_optional_size(
            "revision_id",
            input.revision_id.as_deref(),
            MAX_CONTEXT_ID_BYTES,
        )?;
        if input.limit == 0 || input.limit > MAX_PROJECTION_ITEMS {
            return Err(MemoryError::InvalidRequest(format!(
                "projection list limit must be between 1 and {MAX_PROJECTION_ITEMS}"
            )));
        }
        let connection = self.connection.lock();
        let artifact = load_artifact_raw(&connection, &input.artifact_id, None)?
            .ok_or_else(|| MemoryError::NotFound(format!("artifact {}", input.artifact_id)))?;
        ensure_write_scope(context, &artifact.context, "artifact", &input.artifact_id)?;
        let mut statement = connection.prepare(
            "SELECT id, artifact_id, revision_id, projection_key, kind, text,
                    evidence_json, generator, generator_version, generator_digest,
                    payload_digest, metadata_json, status, dropped_reason, actor,
                    created_at, updated_at, commit_seq
               FROM retrieval_projections
              WHERE artifact_id = ?1 AND (?2 IS NULL OR revision_id = ?2)
              ORDER BY updated_at DESC, id
              LIMIT ?3",
        )?;
        let fetch_limit = i64::try_from(input.limit + 1)
            .map_err(|_| MemoryError::InvalidRequest("projection limit is too large".into()))?;
        let rows = statement.query_map(
            params![input.artifact_id, input.revision_id, fetch_limit],
            RawProjection::from_row,
        )?;
        let mut projections = Vec::new();
        for row in rows {
            projections.push(row?.into_record()?);
        }
        let truncated = projections.len() > input.limit;
        projections.truncate(input.limit);
        Ok(ProjectionListResult {
            artifact_id: input.artifact_id.clone(),
            revision_id: input.revision_id.clone(),
            projections,
            truncated,
        })
    }

    pub fn projection_drop(
        &self,
        context: &AmbientContext,
        input: &ProjectionDropInput,
        idempotency_key: Option<&str>,
        request_hash: &str,
    ) -> Result<MutationResult<ProjectionRecord>> {
        validate_context(context)?;
        require_bounded("projection_id", &input.projection_id, MAX_CONTEXT_ID_BYTES)?;
        require_bounded("reason", &input.reason, MAX_REASON_BYTES)?;
        validate_optional_size("actor", input.actor.as_deref(), MAX_ACTOR_BYTES)?;
        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let previous = load_projection(&transaction, &input.projection_id)?
            .ok_or_else(|| MemoryError::NotFound(format!("projection {}", input.projection_id)))?;
        let artifact = load_artifact_raw(&transaction, &previous.artifact_id, None)?
            .ok_or_else(|| MemoryError::NotFound(format!("artifact {}", previous.artifact_id)))?;
        ensure_write_scope(
            context,
            &artifact.context,
            "artifact",
            &previous.artifact_id,
        )?;
        if let Some(replay) = idempotency_replay::<ProjectionRecord>(
            &transaction,
            idempotency_key,
            request_hash,
            "projection.drop",
        )? {
            return Ok(replay);
        }
        if previous.status == "dropped" {
            let record = previous.into_record()?;
            record_idempotency(
                &transaction,
                idempotency_key,
                request_hash,
                "projection.drop",
                &record,
                record.commit_seq,
            )?;
            transaction.commit()?;
            return Ok(MutationResult {
                commit_seq: record.commit_seq,
                value: record,
                created: false,
            });
        }
        let now = Utc::now();
        let commit_seq = next_commit_seq(&transaction)?;
        transaction.execute(
            "UPDATE retrieval_projections
                SET status = 'dropped', dropped_reason = ?1, actor = ?2,
                    updated_at = ?3, commit_seq = ?4
              WHERE id = ?5",
            params![
                input.reason,
                input.actor,
                now,
                commit_seq,
                input.projection_id
            ],
        )?;
        transaction.execute(
            "DELETE FROM projection_fts WHERE projection_id = ?1",
            [&input.projection_id],
        )?;
        append_event(
            &transaction,
            commit_seq,
            "projection.drop",
            "projection",
            &input.projection_id,
            Some(&previous.revision_id),
            input.actor.as_deref(),
            &json!({"reason": input.reason}),
        )?;
        let record = load_projection(&transaction, &input.projection_id)?
            .ok_or_else(|| MemoryError::Integrity("projection vanished during drop".into()))?
            .into_record()?;
        record_idempotency(
            &transaction,
            idempotency_key,
            request_hash,
            "projection.drop",
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

    pub fn feedback_record(
        &self,
        context: &AmbientContext,
        input: &FeedbackRecordInput,
        idempotency_key: Option<&str>,
        request_hash: &str,
    ) -> Result<MutationResult<FeedbackRecord>> {
        validate_context(context)?;
        let analysis = analyze_query(&input.query)?;
        if input.citations.len() > MAX_EVIDENCE_ITEMS {
            return Err(MemoryError::InvalidRequest(format!(
                "feedback citations must not contain more than {MAX_EVIDENCE_ITEMS} items"
            )));
        }
        for citation in &input.citations {
            require_bounded("feedback citation", citation, MAX_PIN_BYTES)?;
        }
        validate_optional_size(
            "feedback note",
            input.note.as_deref(),
            MAX_FEEDBACK_NOTE_BYTES,
        )?;
        validate_optional_size("actor", input.actor.as_deref(), MAX_ACTOR_BYTES)?;

        let mut connection = self.connection.lock();
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(replay) = idempotency_replay::<FeedbackRecord>(
            &transaction,
            idempotency_key,
            request_hash,
            "feedback.record",
        )? {
            return Ok(replay);
        }
        let key = feedback_fingerprint_key(&transaction)?;
        let query_fingerprint = keyed_query_fingerprint(&key, &analysis.normalized_query);
        let feedback_id = new_id("fb");
        let now = Utc::now();
        let commit_seq = next_commit_seq(&transaction)?;
        let outcome = enum_string(&input.outcome)?;
        let retained_query = input.retain_query.then(|| input.query.clone());
        transaction.execute(
            "INSERT INTO retrieval_feedback (
                id, workspace_id, project_id, task_id, component, outcome,
                query_fingerprint, retained_query, citations_json, note, actor,
                created_at, commit_seq
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                feedback_id,
                context.workspace_id,
                context.project_id,
                context.task_id,
                context.component,
                outcome,
                query_fingerprint,
                retained_query,
                serde_json::to_string(&input.citations)?,
                input.note,
                input.actor,
                now,
                commit_seq,
            ],
        )?;
        let record = FeedbackRecord {
            feedback_id: feedback_id.clone(),
            outcome: input.outcome,
            query_fingerprint,
            retained_query,
            citations: input.citations.clone(),
            note: input.note.clone(),
            actor: input.actor.clone(),
            context: context.clone(),
            created_at: now,
            commit_seq,
        };
        append_event(
            &transaction,
            commit_seq,
            "feedback.record",
            "feedback",
            &feedback_id,
            None,
            input.actor.as_deref(),
            &json!({
                "outcome": input.outcome,
                "query_fingerprint": record.query_fingerprint,
                "query_retained": input.retain_query,
                "citation_count": input.citations.len(),
            }),
        )?;
        record_idempotency(
            &transaction,
            idempotency_key,
            request_hash,
            "feedback.record",
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

    pub fn feedback_get(
        &self,
        context: &AmbientContext,
        input: &FeedbackGetInput,
    ) -> Result<FeedbackRecord> {
        validate_context(context)?;
        require_bounded("feedback_id", &input.feedback_id, MAX_CONTEXT_ID_BYTES)?;
        let connection = self.connection.lock();
        let record = load_feedback(&connection, &input.feedback_id)?
            .ok_or_else(|| MemoryError::NotFound(format!("feedback {}", input.feedback_id)))?;
        ensure_write_scope(context, &record.context, "feedback", &input.feedback_id)?;
        Ok(record)
    }

    pub fn feedback_list(
        &self,
        context: &AmbientContext,
        input: &FeedbackListInput,
    ) -> Result<FeedbackListResult> {
        validate_context(context)?;
        if input.limit == 0 || input.limit > MAX_FEEDBACK_ITEMS {
            return Err(MemoryError::InvalidRequest(format!(
                "feedback list limit must be between 1 and {MAX_FEEDBACK_ITEMS}"
            )));
        }
        let connection = self.connection.lock();
        let fetch_limit = i64::try_from(input.limit + 1)
            .map_err(|_| MemoryError::InvalidRequest("feedback limit is too large".into()))?;
        let mut statement = connection.prepare(
            "SELECT id, outcome, query_fingerprint, retained_query, citations_json,
                    note, actor, workspace_id, project_id, task_id, component,
                    created_at, commit_seq
               FROM retrieval_feedback
              WHERE workspace_id = ?1 AND project_id = ?2
                AND (?3 IS NULL OR task_id IS NULL OR task_id = ?3)
                AND (?4 IS NULL OR commit_seq < ?4)
              ORDER BY commit_seq DESC LIMIT ?5",
        )?;
        let rows = statement.query_map(
            params![
                context.workspace_id,
                context.project_id,
                context.task_id,
                input.before_commit_seq,
                fetch_limit,
            ],
            feedback_row,
        )?;
        let mut feedback = Vec::new();
        for row in rows {
            feedback.push(row?);
        }
        let truncated = feedback.len() > input.limit;
        feedback.truncate(input.limit);
        let next_before_commit_seq = truncated
            .then(|| feedback.last().map(|record| record.commit_seq))
            .flatten();
        Ok(FeedbackListResult {
            feedback,
            truncated,
            next_before_commit_seq,
        })
    }

    pub fn feedback_export(
        &self,
        context: &AmbientContext,
        input: &FeedbackExportInput,
    ) -> Result<FeedbackExportResult> {
        validate_context(context)?;
        if input.limit == 0 || input.limit > MAX_FEEDBACK_ITEMS {
            return Err(MemoryError::InvalidRequest(format!(
                "feedback export limit must be between 1 and {MAX_FEEDBACK_ITEMS}"
            )));
        }
        let connection = self.connection.lock();
        let fetch_limit = i64::try_from(input.limit + 1)
            .map_err(|_| MemoryError::InvalidRequest("feedback limit is too large".into()))?;
        let mut statement = connection.prepare(
            "SELECT id, outcome, retained_query, citations_json, note, created_at, commit_seq
               FROM retrieval_feedback
              WHERE workspace_id = ?1 AND project_id = ?2
                AND (?3 IS NULL OR task_id IS NULL OR task_id = ?3)
                AND retained_query IS NOT NULL
                AND (?4 IS NULL OR commit_seq < ?4)
              ORDER BY commit_seq DESC LIMIT ?5",
        )?;
        let rows = statement.query_map(
            params![
                context.workspace_id,
                context.project_id,
                context.task_id,
                input.before_commit_seq,
                fetch_limit,
            ],
            |row| {
                let outcome = serde_json::from_value(Value::String(row.get::<_, String>(1)?))
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?;
                let citations_json: String = row.get(3)?;
                let citations = serde_json::from_str(&citations_json).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        3,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?;
                Ok(FeedbackEvalCase {
                    feedback_id: row.get(0)?,
                    outcome,
                    query: row.get(2)?,
                    citations,
                    note: row.get(4)?,
                    created_at: row.get(5)?,
                    commit_seq: row.get(6)?,
                })
            },
        )?;
        let mut cases = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        let truncated = cases.len() > input.limit;
        cases.truncate(input.limit);
        let next_before_commit_seq = truncated
            .then(|| cases.last().map(|record| record.commit_seq))
            .flatten();
        Ok(FeedbackExportResult {
            schema_version: 1,
            format: "memoree_retrieval_feedback_v1".into(),
            cases,
            truncated,
            next_before_commit_seq,
        })
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
        index_artifact_revision_chunks(
            &transaction,
            &revision_id,
            &artifact_id,
            &input.title,
            &search_text,
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
        index_artifact_revision_chunks(
            &transaction,
            &revision_id,
            &input.artifact_id,
            title,
            &search_text,
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
        self.search_filtered(context, input, None, SearchQualificationPolicy::Broad)
    }

    pub fn search_qualified(
        &self,
        context: &AmbientContext,
        input: &SearchInput,
    ) -> Result<SearchResult> {
        self.search_filtered(context, input, None, SearchQualificationPolicy::Qualified)
    }

    pub fn search_entity(
        &self,
        context: &AmbientContext,
        input: &SearchInput,
        entity_type: EntityType,
    ) -> Result<SearchResult> {
        self.search_filtered(
            context,
            input,
            Some(entity_type),
            SearchQualificationPolicy::Broad,
        )
    }

    pub fn search_entity_qualified(
        &self,
        context: &AmbientContext,
        input: &SearchInput,
        entity_type: EntityType,
    ) -> Result<SearchResult> {
        self.search_filtered(
            context,
            input,
            Some(entity_type),
            SearchQualificationPolicy::Qualified,
        )
    }

    fn search_filtered(
        &self,
        context: &AmbientContext,
        input: &SearchInput,
        entity_type: Option<EntityType>,
        qualification_policy: SearchQualificationPolicy,
    ) -> Result<SearchResult> {
        validate_context(context)?;
        if let Some(reason) = &input.reason
            && reason.len() > MAX_REASON_BYTES
        {
            return Err(MemoryError::InvalidRequest(format!(
                "search reason must not exceed {MAX_REASON_BYTES} bytes"
            )));
        }
        let query_analysis = analyze_query(&input.query)?;
        let query = query_analysis.fts_expression();
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

        let candidate_limit = i64::try_from((input.limit + 1).saturating_mul(8))
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
                    bm25(artifact_fts, 0.0, 0.0, 5.0, 1.0), ar.commit_seq,
                    artifact_fts.rowid
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
                    bm25(claim_fts, 0.0, 0.0, 1.0), cr.commit_seq,
                    claim_fts.rowid
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

        annotate_lexical_matches(&connection, &mut hits, &query_analysis)?;
        if !hits.iter().any(|candidate| {
            candidate.hit.ranking.qualified && candidate.hit.ranking.lexical_coverage == 1.0
        }) {
            append_trigram_candidates(
                &connection,
                context,
                input,
                entity_type,
                &query_analysis,
                candidate_limit,
                evaluated_at,
                &mut hits,
            )?;
            annotate_lexical_matches(&connection, &mut hits, &query_analysis)?;
        }
        let projection = if matches!(entity_type, Some(EntityType::Claim)) {
            ProjectionRetrievalStatus {
                state: "not_applicable".into(),
                policy_version: PROJECTION_POLICY_VERSION.into(),
                candidate_count: 0,
                reason: Some("derived projections are available only for artifacts".into()),
            }
        } else {
            // Candidate-only channels must never make authoritative lexical
            // retrieval unavailable. Stage mutations so a corrupt disposable
            // projection row cannot leak partial ordering signals either.
            let mut staged_hits = hits.clone();
            match append_projection_candidates(
                &connection,
                &self.cas,
                context,
                input,
                &query_analysis,
                candidate_limit,
                &mut staged_hits,
            ) {
                Ok(()) => {
                    let candidate_count = staged_hits
                        .iter()
                        .filter(|candidate| candidate.projection_score.is_some())
                        .count();
                    hits = staged_hits;
                    ProjectionRetrievalStatus {
                        state: if candidate_count == 0 {
                            "no_candidates".into()
                        } else {
                            "ready".into()
                        },
                        policy_version: PROJECTION_POLICY_VERSION.into(),
                        candidate_count,
                        reason: None,
                    }
                }
                Err(error) => ProjectionRetrievalStatus {
                    state: "error".into(),
                    policy_version: PROJECTION_POLICY_VERSION.into(),
                    candidate_count: 0,
                    reason: Some(format!(
                        "derived projection retrieval failed open; authoritative lexical retrieval remains available: {error}"
                    )),
                },
            }
        };
        let eligible_semantic =
            eligible_semantic_revisions(&connection, context, input, entity_type, evaluated_at)?;
        let semantic_limit = usize::try_from(candidate_limit)
            .map_err(|_| MemoryError::InvalidRequest("search limit is too large".into()))?
            .max(SEMANTIC_MIN_CANDIDATE_POOL);
        let (semantic_hits, mut semantic) = match self.semantic.search(
            &input.query,
            &eligible_semantic,
            semantic_limit,
            current_seq,
        ) {
            Ok(result) => result,
            Err(error) => (
                Vec::new(),
                SemanticRetrievalStatus {
                    state: "error".into(),
                    policy_version: SEMANTIC_POLICY_VERSION.into(),
                    model_id: None,
                    model_revision: None,
                    indexed_commit_seq: 0,
                    current_commit_seq: current_seq,
                    eligible_revision_count: eligible_semantic.len(),
                    indexed_revision_count: 0,
                    coverage: 0.0,
                    reason: Some(format!(
                        "semantic retrieval failed closed; lexical retrieval remains available: {error}"
                    )),
                },
            ),
        };
        if let Err(error) =
            append_semantic_candidates(&connection, &semantic_hits, evaluated_at, &mut hits)
        {
            semantic.state = "error".into();
            semantic.reason = Some(format!(
                "semantic candidates failed closed; lexical retrieval remains available: {error}"
            ));
        }
        finalize_trigram_qualification_and_fusion(&mut hits);
        let unqualified_candidate_count = hits
            .iter()
            .filter(|candidate| !candidate.hit.ranking.qualified)
            .count();
        let best_unqualified_coverage = hits
            .iter()
            .filter(|candidate| !candidate.hit.ranking.qualified)
            .map(|candidate| candidate.hit.ranking.lexical_coverage)
            .max_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
        select_artifact_citation_spans(&connection, &mut hits, &query_analysis)?;
        let mut hits = rerank_with_recency(hits, input.recency.enabled, evaluated_at);
        let reranker = match order_hits_with_reranker(
            &self.reranker,
            entity_type,
            &input.query,
            &mut hits,
        ) {
            Ok(status) => status,
            Err(error) => {
                let mut status = RerankerRetrievalStatus::disabled_on_surface(
                    hits.iter().filter(|hit| !hit.ranking.exact_tier).count(),
                    match entity_type {
                        Some(EntityType::Artifact) => "artifact",
                        Some(EntityType::Claim) => "claim",
                        None => "mixed",
                    },
                    format!(
                        "reranker ordering failed open; deterministic fused order remains available: {error}"
                    ),
                );
                status.state = "error".into();
                status
            }
        };
        // Keep the full reranker-sized recovery pool in the frozen search
        // snapshot. Recall still exposes only its caller-selected small
        // candidate limits, while context recovery can build a compact,
        // evidence-labelled subset without issuing a second search against a
        // potentially newer authority state.
        let candidate_partition_limit = RERANKER_ORDERING_CANDIDATE_LIMIT;
        let mut candidate_entity_ids = BTreeSet::new();
        let candidate_hit_count = hits
            .iter()
            .filter(|hit| !hit.ranking.qualified)
            .filter(|hit| candidate_entity_ids.insert(hit.entity_id.clone()))
            .count();
        candidate_entity_ids.clear();
        let candidate_hits = hits
            .iter()
            .filter(|hit| !hit.ranking.qualified)
            .filter(|hit| candidate_entity_ids.insert(hit.entity_id.clone()))
            .take(candidate_partition_limit)
            .cloned()
            .collect::<Vec<_>>();
        let candidate_hits_truncated = candidate_hit_count > candidate_hits.len();
        if qualification_policy == SearchQualificationPolicy::Qualified {
            hits.retain(|hit| hit.ranking.qualified);
        }
        retain_conflicted_hits_within_limit(&mut hits, input.limit);
        let truncated = hits.len() > input.limit;
        hits.truncate(input.limit);
        let semantic_component = (semantic.model_id.is_some()
            && semantic.eligible_revision_count > 0
            && semantic.state != "error")
            .then_some(SEMANTIC_POLICY_VERSION);
        let mut retrieval_mode = format!(
            "sqlite_fts5_bm25+{LEXICAL_POLICY_VERSION}+{TRIGRAM_POLICY_VERSION}+{PROJECTION_POLICY_VERSION}+{FUSION_POLICY_VERSION}"
        );
        if let Some(component) = semantic_component {
            retrieval_mode.push('+');
            retrieval_mode.push_str(component);
        }
        if reranker.ordering_applied {
            retrieval_mode.push('+');
            retrieval_mode.push_str(RERANKER_POLICY_VERSION);
        }
        if input.recency.enabled {
            retrieval_mode.push('+');
            retrieval_mode.push_str(RECENCY_POLICY_VERSION);
        }
        Ok(SearchResult {
            query: input.query.clone(),
            query_analysis: query_analysis.public(),
            horizon: input.horizon,
            retrieval_mode,
            projection,
            semantic,
            reranker,
            qualification_applied: qualification_policy
                == SearchQualificationPolicy::Qualified,
            unqualified_candidate_count,
            best_unqualified_coverage,
            broaden_hint: if hits.is_empty() && matches!(input.horizon, Horizon::Ambient) {
                Some(
                    "No ambient matches. Retry explicitly with horizon=workspace (and a reason) if broader precedent is needed."
                        .into(),
                )
            } else {
                None
            },
            hits,
            candidate_hits,
            candidate_hits_truncated,
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
             DELETE FROM artifact_trigram_fts;
             INSERT INTO artifact_trigram_fts(revision_id, artifact_id, title, body)
               SELECT id, artifact_id, title, search_text FROM artifact_revisions;
             DELETE FROM artifact_chunk_fts;
             DELETE FROM claim_fts;
             INSERT INTO claim_fts(revision_id, claim_id, statement)
               SELECT id, claim_id, statement FROM claim_revisions;
             DELETE FROM claim_trigram_fts;
             INSERT INTO claim_trigram_fts(revision_id, claim_id, statement)
               SELECT id, claim_id, statement FROM claim_revisions;",
        )?;
        rebuild_artifact_chunk_index(&transaction)?;
        transaction.commit()?;
        Ok(())
    }

    /// Explicitly download, verify, and install the pinned semantic model,
    /// then rebuild the disposable dense projection. Queries never call this
    /// path and therefore never download model files.
    pub fn semantic_enable(&self) -> Result<SemanticInstallReport> {
        let manifest = self.semantic.install_model()?;
        let (documents, indexed_commit_seq) = self.semantic_documents()?;
        let (_, rebuild) = self.semantic.rebuild(&documents, indexed_commit_seq)?;
        Ok(SemanticInstallReport {
            enabled: true,
            model_id: manifest.model_id,
            model_revision: manifest.model_revision,
            model_directory: self
                .db_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("semantic/model")
                .display()
                .to_string(),
            projection_database: self
                .db_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("semantic/semantic.sqlite3")
                .display()
                .to_string(),
            document_count: documents.len(),
            vector_count: rebuild.vector_count,
            reused_vector_count: rebuild.reused_vector_count,
            embedded_vector_count: rebuild.embedded_vector_count,
            deleted_vector_count: rebuild.deleted_vector_count,
            indexed_commit_seq,
            model_load_latency_ms: rebuild.model_load_latency_ms,
            embedding_generation_ms: rebuild.embedding_generation_ms,
        })
    }

    /// Install a previously verified local model directory, then rebuild the
    /// projection without any network-backed model resolution.
    pub fn semantic_enable_from_directory(&self, source: &Path) -> Result<SemanticInstallReport> {
        let manifest = self.semantic.install_model_from_directory(source)?;
        let (documents, indexed_commit_seq) = self.semantic_documents()?;
        let (_, rebuild) = self.semantic.rebuild(&documents, indexed_commit_seq)?;
        Ok(SemanticInstallReport {
            enabled: true,
            model_id: manifest.model_id,
            model_revision: manifest.model_revision,
            model_directory: self
                .db_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("semantic/model")
                .display()
                .to_string(),
            projection_database: self
                .db_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("semantic/semantic.sqlite3")
                .display()
                .to_string(),
            document_count: documents.len(),
            vector_count: rebuild.vector_count,
            reused_vector_count: rebuild.reused_vector_count,
            embedded_vector_count: rebuild.embedded_vector_count,
            deleted_vector_count: rebuild.deleted_vector_count,
            indexed_commit_seq,
            model_load_latency_ms: rebuild.model_load_latency_ms,
            embedding_generation_ms: rebuild.embedding_generation_ms,
        })
    }

    /// Rebuild an already installed semantic projection without network I/O.
    pub fn semantic_rebuild(&self) -> Result<SemanticInstallReport> {
        let (documents, indexed_commit_seq) = self.semantic_documents()?;
        let (manifest, rebuild) = self.semantic.rebuild(&documents, indexed_commit_seq)?;
        Ok(SemanticInstallReport {
            enabled: true,
            model_id: manifest.model_id,
            model_revision: manifest.model_revision,
            model_directory: self
                .db_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("semantic/model")
                .display()
                .to_string(),
            projection_database: self
                .db_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("semantic/semantic.sqlite3")
                .display()
                .to_string(),
            document_count: documents.len(),
            vector_count: rebuild.vector_count,
            reused_vector_count: rebuild.reused_vector_count,
            embedded_vector_count: rebuild.embedded_vector_count,
            deleted_vector_count: rebuild.deleted_vector_count,
            indexed_commit_seq,
            model_load_latency_ms: rebuild.model_load_latency_ms,
            embedding_generation_ms: rebuild.embedding_generation_ms,
        })
    }

    /// Report optional dense-projection readiness across all currently
    /// recallable revisions without loading the inference runtime.
    pub fn semantic_status(&self) -> Result<SemanticRetrievalStatus> {
        let connection = self.connection.lock();
        let current_commit_seq: i64 = connection.query_row(
            "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'commit_seq'",
            [],
            |row| row.get(0),
        )?;
        let evaluated_at = Utc::now();
        let mut eligible = BTreeMap::new();
        {
            let mut statement = connection.prepare(
                "SELECT ar.id, a.id, ar.blob_hash
                   FROM artifact_revisions ar
                   JOIN artifacts a ON a.id = ar.artifact_id
                  WHERE a.status = 'active' AND a.current_revision_id = ar.id",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?;
            for row in rows {
                let (revision_id, entity_id, revision_hash) = row?;
                eligible.insert(
                    revision_id,
                    EligibleSemanticRevision {
                        entity_type: EntityType::Artifact,
                        entity_id,
                        revision_hash,
                    },
                );
            }
        }
        {
            let mut statement = connection.prepare(
                "SELECT cr.id, c.id, cr.statement
                   FROM claim_revisions cr
                   JOIN claims c ON c.id = cr.claim_id
                  WHERE c.status IN ('active', 'conflicted')
                    AND c.current_revision_id = cr.id
                    AND (c.valid_from IS NULL OR c.valid_from <= ?1)
                    AND (c.valid_until IS NULL OR c.valid_until > ?1)",
            )?;
            let rows = statement.query_map([evaluated_at], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?;
            for row in rows {
                let (revision_id, entity_id, statement) = row?;
                eligible.insert(
                    revision_id,
                    EligibleSemanticRevision {
                        entity_type: EntityType::Claim,
                        entity_id,
                        revision_hash: semantic_claim_revision_hash(&statement),
                    },
                );
            }
        }
        drop(connection);
        self.semantic.status(current_commit_seq, &eligible)
    }

    /// Resolve a bounded exact byte window inside one already-cited artifact
    /// revision. This is candidate routing only: it never qualifies an answer,
    /// never searches another source, and fails closed when the local semantic
    /// projection is unavailable or stale.
    pub(crate) fn resolve_probe_evidence_spans(
        &self,
        context: &AmbientContext,
        horizon: Horizon,
        claim_statement: &str,
        user_query: &str,
        artifact_id: &str,
        revision_id: &str,
    ) -> Result<ProbeEvidenceResolutionSet> {
        let connection = self.connection.lock();
        ensure_entity_in_read_scope(
            &connection,
            context,
            EntityType::Artifact,
            artifact_id,
            horizon,
        )?;
        let raw = load_artifact_raw(&connection, artifact_id, Some(revision_id))?;
        let Some(raw) = raw else {
            return Ok(ProbeEvidenceResolutionSet {
                windows: Vec::new(),
                complete: false,
            });
        };
        let revision_hash = raw.blob.hash.clone();
        let blob = raw.blob;
        let current_commit_seq: i64 = connection.query_row(
            "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'commit_seq'",
            [],
            |row| row.get(0),
        )?;
        drop(connection);

        // Verify every emitted locator against authoritative immutable bytes,
        // not merely against the derived projection row.
        let authoritative_bytes = self.cas.get(&blob)?;
        if blake3::hash(&authoritative_bytes).to_hex().as_str() != revision_hash {
            return Err(MemoryError::Integrity(format!(
                "artifact revision {revision_id} changed while resolving evidence"
            )));
        }
        let Ok(authoritative_text) = std::str::from_utf8(&authoritative_bytes) else {
            return Ok(ProbeEvidenceResolutionSet {
                windows: Vec::new(),
                complete: false,
            });
        };

        let mut eligible = BTreeMap::new();
        eligible.insert(
            revision_id.to_owned(),
            EligibleSemanticRevision {
                entity_type: EntityType::Artifact,
                entity_id: artifact_id.to_owned(),
                revision_hash: revision_hash.clone(),
            },
        );
        let clauses = ranked_claim_evidence_clauses(claim_statement, user_query);
        let intended_count = clauses.len();
        let mut windows = Vec::<ProbeEvidenceResolution>::new();
        let mut all_selected_clauses_resolved = true;
        for clause in clauses.into_iter().take(3) {
            // Fixed decomposition is primary. The original user wording only
            // disambiguates the exact source window; neither input is executed
            // or sent to a remote model.
            let locator_query = format!(
                "Exact source evidence for this claim clause:\n{}\nOriginal user wording:\n{}",
                bounded_utf8_preview(&clause, 2 * 1024),
                bounded_utf8_preview(user_query, 2 * 1024),
            );
            let (hits, status) = self.semantic.search_ranged_windows(
                &locator_query,
                &eligible,
                6,
                current_commit_seq,
            )?;
            if status.state != "ready" {
                return Ok(ProbeEvidenceResolutionSet {
                    windows: Vec::new(),
                    complete: false,
                });
            }
            let resolved = hits.into_iter().find_map(|hit| {
                let (Some(start_byte), Some(end_byte)) = (hit.start_byte, hit.end_byte) else {
                    return None;
                };
                let (Ok(start), Ok(end)) = (usize::try_from(start_byte), usize::try_from(end_byte))
                else {
                    return None;
                };
                if start >= end
                    || end > authoritative_text.len()
                    || !authoritative_text.is_char_boundary(start)
                    || !authoritative_text.is_char_boundary(end)
                    || end - start > SEMANTIC_WINDOW_MAX_BYTES
                    || windows
                        .iter()
                        .any(|window| start_byte < window.end_byte && end_byte > window.start_byte)
                {
                    return None;
                }
                Some(ProbeEvidenceResolution {
                    start_byte,
                    end_byte,
                    source_revision_hash: revision_hash.clone(),
                    locator_policy_version: format!(
                        "claim_clause_windows_v1/{SEMANTIC_POLICY_VERSION}"
                    ),
                })
            });
            if let Some(resolved) = resolved {
                windows.push(resolved);
            } else {
                all_selected_clauses_resolved = false;
            }
        }
        windows.sort_by(|left, right| {
            left.start_byte
                .cmp(&right.start_byte)
                .then_with(|| left.end_byte.cmp(&right.end_byte))
        });
        Ok(ProbeEvidenceResolutionSet {
            complete: intended_count <= 3
                && all_selected_clauses_resolved
                && windows.len() == intended_count,
            windows,
        })
    }

    /// Select a strict, query-relevant subrange of one already exact artifact
    /// candidate. The caller preserves the parent citation for a bounded
    /// expansion; failure always leaves that exact parent unchanged.
    pub(crate) fn resolve_probe_artifact_window(
        &self,
        context: &AmbientContext,
        horizon: Horizon,
        user_query: &str,
        artifact_id: &str,
        revision_id: &str,
        parent_range: (u64, u64),
    ) -> Result<Option<ProbeEvidenceResolution>> {
        let (parent_start, parent_end) = parent_range;
        if parent_start >= parent_end {
            return Ok(None);
        }
        let connection = self.connection.lock();
        ensure_entity_in_read_scope(
            &connection,
            context,
            EntityType::Artifact,
            artifact_id,
            horizon,
        )?;
        let revision_hash = connection
            .query_row(
                "SELECT blob_hash
                   FROM artifact_revisions
                  WHERE artifact_id = ?1 AND id = ?2",
                params![artifact_id, revision_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(revision_hash) = revision_hash else {
            return Ok(None);
        };
        let current_commit_seq: i64 = connection.query_row(
            "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'commit_seq'",
            [],
            |row| row.get(0),
        )?;
        drop(connection);

        let mut eligible = BTreeMap::new();
        eligible.insert(
            revision_id.to_owned(),
            EligibleSemanticRevision {
                entity_type: EntityType::Artifact,
                entity_id: artifact_id.to_owned(),
                revision_hash: revision_hash.clone(),
            },
        );
        let (hits, status) = self.semantic.search_ranged_within(
            bounded_utf8_preview(user_query, 6 * 1024),
            &eligible,
            1,
            current_commit_seq,
            parent_start,
            parent_end,
        )?;
        if status.state != "ready" {
            return Ok(None);
        }
        let Some(hit) = hits.into_iter().next() else {
            return Ok(None);
        };
        let (Some(start_byte), Some(end_byte)) = (hit.start_byte, hit.end_byte) else {
            return Ok(None);
        };
        let span_bytes = end_byte.saturating_sub(start_byte);
        let strict_subrange = start_byte > parent_start || end_byte < parent_end;
        if !strict_subrange
            || start_byte < parent_start
            || end_byte > parent_end
            || start_byte >= end_byte
            || span_bytes > u64::try_from(SEMANTIC_WINDOW_MAX_BYTES).unwrap_or(u64::MAX)
        {
            return Ok(None);
        }
        Ok(Some(ProbeEvidenceResolution {
            start_byte,
            end_byte,
            source_revision_hash: revision_hash,
            locator_policy_version: format!("artifact_subrange_v1/{SEMANTIC_POLICY_VERSION}"),
        }))
    }

    pub fn semantic_model_installed(&self) -> bool {
        self.semantic.is_installed()
    }

    /// Explicitly download and digest-pin the provisional ordering-only
    /// reranker. This never enables answer qualification.
    pub fn reranker_enable(&self) -> Result<RerankerInstallReport> {
        self.reranker.install_model()
    }

    /// Install already downloaded reranker bytes without network access.
    pub fn reranker_enable_from_directory(&self, source: &Path) -> Result<RerankerInstallReport> {
        self.reranker.install_model_from_directory(source)
    }

    pub fn reranker_status(&self) -> Result<RerankerRetrievalStatus> {
        self.reranker.status()
    }

    pub fn reranker_model_installed(&self) -> bool {
        self.reranker.is_installed()
    }

    fn semantic_documents(&self) -> Result<(Vec<SemanticDocument>, i64)> {
        let connection = self.connection.lock();
        let indexed_commit_seq: i64 = connection.query_row(
            "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'commit_seq'",
            [],
            |row| row.get(0),
        )?;
        let mut documents = Vec::new();
        let artifact_revisions = {
            let mut statement = connection.prepare(
                "SELECT ar.id, ar.artifact_id, ar.title, ar.blob_hash, ar.commit_seq,
                        a.kind, a.component
                   FROM artifact_revisions ar
                   JOIN artifacts a ON a.id = ar.artifact_id
                  ORDER BY ar.commit_seq",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                ))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        for (revision_id, artifact_id, title, revision_hash, commit_seq, kind, component) in
            artifact_revisions
        {
            let mut semantic_ordinal = 0usize;
            for span in semantic_window_spans(&title) {
                documents.push(SemanticDocument {
                    entity_type: EntityType::Artifact,
                    entity_id: artifact_id.clone(),
                    revision_id: revision_id.clone(),
                    ordinal: semantic_ordinal,
                    start_byte: None,
                    end_byte: None,
                    revision_hash: revision_hash.clone(),
                    text: title[span.start_byte..span.end_byte].to_owned(),
                    commit_seq,
                });
                semantic_ordinal += 1;
            }
            let chunks = {
                let mut statement = connection.prepare(
                    "SELECT CAST(ordinal AS INTEGER), CAST(start_byte AS INTEGER),
                            CAST(end_byte AS INTEGER), body
                       FROM artifact_chunk_fts
                      WHERE revision_id = ?1 AND row_kind = 'body'
                      ORDER BY CAST(ordinal AS INTEGER)",
                )?;
                let rows = statement.query_map([&revision_id], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };
            for (_authority_ordinal, authority_start, authority_end, body) in chunks {
                let authority_start =
                    usize::try_from(authority_start).map_err(|_| MemoryError::ContentTooLarge)?;
                let authority_end =
                    usize::try_from(authority_end).map_err(|_| MemoryError::ContentTooLarge)?;
                if authority_end.saturating_sub(authority_start) != body.len() {
                    return Err(MemoryError::Integrity(format!(
                        "artifact authority chunk byte span does not match its body for revision {revision_id}"
                    )));
                }
                for span in semantic_window_spans(&body) {
                    documents.push(SemanticDocument {
                        entity_type: EntityType::Artifact,
                        entity_id: artifact_id.clone(),
                        revision_id: revision_id.clone(),
                        ordinal: semantic_ordinal,
                        start_byte: Some(authority_start + span.start_byte),
                        end_byte: Some(authority_start + span.end_byte),
                        revision_hash: revision_hash.clone(),
                        text: contextualized_semantic_passage(
                            &title,
                            &kind,
                            component.as_deref(),
                            &body[span.start_byte..span.end_byte],
                        ),
                        commit_seq,
                    });
                    semantic_ordinal += 1;
                }
            }
        }
        let claim_revisions = {
            let mut statement = connection.prepare(
                "SELECT cr.id, cr.claim_id, cr.statement, cr.commit_seq,
                        c.claim_type, c.component
                   FROM claim_revisions cr
                   JOIN claims c ON c.id = cr.claim_id
                  ORDER BY cr.commit_seq",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                ))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        for (revision_id, claim_id, statement, commit_seq, claim_type, component) in claim_revisions
        {
            let revision_hash = semantic_claim_revision_hash(&statement);
            for (ordinal, span) in semantic_window_spans(&statement).into_iter().enumerate() {
                documents.push(SemanticDocument {
                    entity_type: EntityType::Claim,
                    entity_id: claim_id.clone(),
                    revision_id: revision_id.clone(),
                    ordinal,
                    start_byte: None,
                    end_byte: None,
                    revision_hash: revision_hash.clone(),
                    text: contextualized_semantic_claim(
                        &claim_type,
                        component.as_deref(),
                        &statement[span.start_byte..span.end_byte],
                    ),
                    commit_seq,
                });
            }
        }
        Ok((documents, indexed_commit_seq))
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
        let artifact_trigram_count: i64 =
            connection.query_row("SELECT COUNT(*) FROM artifact_trigram_fts", [], |row| {
                row.get(0)
            })?;
        let claim_trigram_count: i64 =
            connection.query_row("SELECT COUNT(*) FROM claim_trigram_fts", [], |row| {
                row.get(0)
            })?;
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
        if artifact_trigram_count != checked_artifact_revisions as i64 {
            issues.push(format!(
                "artifact trigram row count {artifact_trigram_count} differs from revision count {checked_artifact_revisions}"
            ));
        }
        if claim_trigram_count != checked_claim_revisions as i64 {
            issues.push(format!(
                "claim trigram row count {claim_trigram_count} differs from revision count {checked_claim_revisions}"
            ));
        }
        let mismatched_artifact_trigrams: i64 = connection.query_row(
            "SELECT COUNT(*) FROM artifact_trigram_fts f
              LEFT JOIN artifact_revisions ar ON ar.id = f.revision_id
             WHERE ar.id IS NULL OR f.artifact_id <> ar.artifact_id
                OR f.title <> ar.title OR f.body <> ar.search_text",
            [],
            |row| row.get(0),
        )?;
        if mismatched_artifact_trigrams > 0 {
            issues.push(format!(
                "{mismatched_artifact_trigrams} artifact trigram rows differ from their authoritative revision text"
            ));
        }
        let mismatched_claim_trigrams: i64 = connection.query_row(
            "SELECT COUNT(*) FROM claim_trigram_fts f
              LEFT JOIN claim_revisions cr ON cr.id = f.revision_id
             WHERE cr.id IS NULL OR f.claim_id <> cr.claim_id
                OR f.statement <> cr.statement",
            [],
            |row| row.get(0),
        )?;
        if mismatched_claim_trigrams > 0 {
            issues.push(format!(
                "{mismatched_claim_trigrams} claim trigram rows differ from their authoritative revision text"
            ));
        }
        let malformed_projection_fts: i64 = connection.query_row(
            "SELECT COUNT(*) FROM retrieval_projections rp
             WHERE (rp.status = 'active' AND (
                       (SELECT COUNT(*) FROM projection_fts f
                         WHERE f.projection_id = rp.id) <> 1
                       OR NOT EXISTS (
                         SELECT 1 FROM projection_fts f
                          WHERE f.projection_id = rp.id
                            AND f.revision_id = rp.revision_id
                            AND f.artifact_id = rp.artifact_id AND f.text = rp.text)))
                OR (rp.status = 'dropped' AND EXISTS (
                      SELECT 1 FROM projection_fts f WHERE f.projection_id = rp.id))",
            [],
            |row| row.get(0),
        )?;
        let orphaned_projection_fts: i64 = connection.query_row(
            "SELECT COUNT(*) FROM projection_fts f
             LEFT JOIN retrieval_projections rp ON rp.id = f.projection_id
             WHERE rp.id IS NULL",
            [],
            |row| row.get(0),
        )?;
        if malformed_projection_fts > 0 || orphaned_projection_fts > 0 {
            issues.push(format!(
                "projection FTS differs from cited projection authority ({malformed_projection_fts} malformed, {orphaned_projection_fts} orphaned)"
            ));
        }
        let projection_sources = {
            let mut statement = connection.prepare(
                "SELECT rp.id, rp.evidence_json, ar.blob_size
                   FROM retrieval_projections rp
                   LEFT JOIN artifact_revisions ar
                     ON ar.id = rp.revision_id AND ar.artifact_id = rp.artifact_id
                  ORDER BY rp.commit_seq",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<i64>>(2)?,
                ))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        for (projection_id, evidence_json, blob_size) in projection_sources {
            let Some(blob_size) = blob_size.and_then(|size| u64::try_from(size).ok()) else {
                issues.push(format!(
                    "projection {projection_id} references a missing artifact revision"
                ));
                continue;
            };
            match serde_json::from_str::<Vec<ProjectionSpan>>(&evidence_json) {
                Ok(spans)
                    if !spans.is_empty()
                        && spans.len() <= MAX_PROJECTION_SPANS
                        && validate_projection_spans(&spans, blob_size).is_ok() => {}
                Ok(_) => issues.push(format!(
                    "projection {projection_id} has invalid exact evidence spans"
                )),
                Err(error) => issues.push(format!(
                    "projection {projection_id} has invalid evidence JSON: {error}"
                )),
            }
        }
        let broken_source_items: i64 = connection.query_row(
            "SELECT COUNT(*) FROM source_items si
             LEFT JOIN sources s ON s.id = si.source_id
             LEFT JOIN artifact_revisions ar
               ON ar.id = si.artifact_revision_id AND ar.artifact_id = si.artifact_id
             LEFT JOIN artifacts a ON a.id = si.artifact_id
             WHERE s.id IS NULL OR ar.id IS NULL OR a.id IS NULL
                OR (si.state = 'live' AND a.status <> 'active')
                OR (si.state = 'withdrawn' AND a.status <> 'forgotten')",
            [],
            |row| row.get(0),
        )?;
        if broken_source_items > 0 {
            issues.push(format!(
                "{broken_source_items} source items have inconsistent artifact authority or lifecycle"
            ));
        }
        let malformed_chunk_rows: i64 = connection.query_row(
            "SELECT COUNT(*) FROM artifact_chunk_fts f
              LEFT JOIN artifact_revisions ar ON ar.id = f.revision_id
             WHERE ar.id IS NULL OR ar.artifact_id <> f.artifact_id
                OR CAST(f.chunker_version AS INTEGER) <> ?1
                OR (f.row_kind = 'title' AND (
                      f.ordinal IS NOT NULL OR f.start_byte IS NOT NULL
                      OR f.end_byte IS NOT NULL OR f.title <> ar.title OR f.body <> ''))
                OR (f.row_kind = 'body' AND (
                      f.ordinal IS NULL OR f.start_byte IS NULL OR f.end_byte IS NULL
                      OR f.title <> '' OR CAST(f.start_byte AS INTEGER) < 0
                      OR CAST(f.end_byte AS INTEGER) <= CAST(f.start_byte AS INTEGER)))
                OR f.row_kind NOT IN ('title', 'body')",
            [ARTIFACT_CHUNKER_VERSION],
            |row| row.get(0),
        )?;
        if malformed_chunk_rows > 0 {
            issues.push(format!(
                "{malformed_chunk_rows} artifact chunk rows have invalid ownership, kind, version, or span metadata"
            ));
        }
        let wrong_title_row_count: i64 = connection.query_row(
            "SELECT COUNT(*) FROM artifact_revisions ar
              WHERE (SELECT COUNT(*) FROM artifact_chunk_fts f
                      WHERE f.revision_id = ar.id AND f.row_kind = 'title') <> 1",
            [],
            |row| row.get(0),
        )?;
        if wrong_title_row_count > 0 {
            issues.push(format!(
                "{wrong_title_row_count} artifact revisions do not have exactly one spanless title row"
            ));
        }
        let revision_chunk_sources = {
            let mut statement = connection.prepare(
                "SELECT id, search_text, blob_hash, blob_size
                   FROM artifact_revisions ORDER BY commit_seq",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        for (revision_id, search_text, blob_hash, blob_size) in revision_chunk_sources {
            let body_rows = {
                let mut statement = connection.prepare(
                    "SELECT CAST(ordinal AS INTEGER), CAST(start_byte AS INTEGER),
                            CAST(end_byte AS INTEGER), body
                       FROM artifact_chunk_fts
                      WHERE revision_id = ?1 AND row_kind = 'body'
                      ORDER BY CAST(ordinal AS INTEGER)",
                )?;
                let rows = statement.query_map([&revision_id], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })?;
                rows.collect::<rusqlite::Result<Vec<_>>>()?
            };
            let byte_identical = i64::try_from(search_text.len()).ok() == Some(blob_size)
                && blake3::hash(search_text.as_bytes()).to_hex().to_string() == blob_hash;
            let expected = if byte_identical {
                artifact_chunk_spans(&search_text)
            } else {
                Vec::new()
            };
            let valid = body_rows.len() == expected.len()
                && body_rows.iter().zip(&expected).enumerate().all(
                    |(ordinal, ((stored_ordinal, start, end, body), span))| {
                        usize::try_from(*stored_ordinal).ok() == Some(ordinal)
                            && usize::try_from(*start).ok() == Some(span.start_byte)
                            && usize::try_from(*end).ok() == Some(span.end_byte)
                            && body == &search_text[span.start_byte..span.end_byte]
                    },
                );
            if !valid {
                issues.push(format!(
                    "artifact revision {revision_id} chunk rows are not a dense, exact byte partition"
                ));
            }
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

fn open_private_migration_lock(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(file)
}

fn create_pre_migration_backup(
    connection: &Connection,
    db_path: &Path,
    blob_dir: &Path,
    from_schema: i64,
) -> Result<SchemaMigrationReport> {
    let data_dir = db_path
        .parent()
        .ok_or_else(|| MemoryError::Config("database path has no parent".into()))?;
    let backup_root = data_dir.join(MIGRATION_BACKUP_DIRECTORY);
    ensure_private_directory(&backup_root)?;
    preflight_migration_backup_space(db_path, blob_dir, &backup_root)?;

    let completed_at = Utc::now();
    let destination = backup_root.join(format!(
        "schema-{from_schema}-to-{SCHEMA_VERSION}-{}-{}",
        completed_at.format("%Y%m%dT%H%M%SZ"),
        Ulid::r#gen()
    ));
    ensure_backup_destination_absent(&destination)?;
    let staging = tempfile::Builder::new()
        .prefix(".memoree-migration-stage-")
        .tempdir_in(&backup_root)?;
    ensure_private_directory(staging.path())?;
    let staged_database_path = staging.path().join(MEMOREE_DATABASE_FILE);
    let staged_blobs_path = staging.path().join("blobs");

    let mut destination_connection = Connection::open(&staged_database_path)?;
    {
        let backup = rusqlite::backup::Backup::new(connection, &mut destination_connection)?;
        backup.run_to_completion(64, Duration::from_millis(5), None)?;
    }
    let integrity: String =
        destination_connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        return Err(MemoryError::Integrity(format!(
            "pre-migration SQLite snapshot failed integrity_check: {integrity}"
        )));
    }
    drop(destination_connection);
    set_sqlite_file_permissions(&staged_database_path)?;

    let source_cas = Cas::new(blob_dir)?;
    verify_referenced_external_blobs(connection, &source_cas)?;
    let copied_external_blobs = source_cas.copy_external_to(&staged_blobs_path)?;
    let copied_report = Cas::new(&staged_blobs_path)?.verify_all_external()?;
    if !copied_report.is_ok() {
        return Err(MemoryError::Integrity(format!(
            "pre-migration CAS snapshot failed verification: {}",
            copied_report.issues.join("; ")
        )));
    }

    let manifest_path = staging.path().join("migration.json");
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&json!({
            "schema": 1,
            "from_schema": from_schema,
            "to_schema": SCHEMA_VERSION,
            "database": MEMOREE_DATABASE_FILE,
            "blobs": "blobs",
            "copied_external_blobs": copied_external_blobs,
            "created_at": completed_at,
            "restore": "Stop Memoree, replace the data directory database and blobs with this snapshot, then reinstall the matching older binary."
        }))?,
    )?;
    set_private_regular_file(&manifest_path)?;
    sync_backup_tree(staging.path())?;
    atomic_publish_backup(staging.path(), &destination)?;
    sync_directory_best_effort(&backup_root);

    Ok(SchemaMigrationReport {
        from_schema,
        to_schema: SCHEMA_VERSION,
        backup_destination: destination.display().to_string(),
        copied_external_blobs,
        completed_at,
    })
}

fn preflight_migration_backup_space(
    db_path: &Path,
    blob_dir: &Path,
    destination: &Path,
) -> Result<()> {
    let sqlite_bytes = sqlite_live_bytes(db_path)?;
    let required = sqlite_bytes
        .saturating_mul(4)
        .saturating_add(directory_regular_file_bytes(blob_dir)?)
        .saturating_add(64 * 1024 * 1024);
    let available = fs2::available_space(destination)?;
    if available < required {
        return Err(MemoryError::Config(format!(
            "insufficient free space for schema migration backup: need at least {required} bytes, have {available} bytes in {}",
            destination.display()
        )));
    }
    Ok(())
}

fn sqlite_live_bytes(db_path: &Path) -> Result<u64> {
    let mut total = fs::metadata(db_path)?.len();
    for suffix in ["-wal", "-shm"] {
        let mut value = db_path.as_os_str().to_owned();
        value.push(suffix);
        let path = PathBuf::from(value);
        if path.is_file() {
            total = total.saturating_add(fs::metadata(path)?.len());
        }
    }
    Ok(total)
}

fn directory_regular_file_bytes(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let mut total = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            total = total.saturating_add(directory_regular_file_bytes(&entry.path())?);
        } else if file_type.is_file() {
            total = total.saturating_add(entry.metadata()?.len());
        }
    }
    Ok(total)
}

fn verify_referenced_external_blobs(connection: &Connection, cas: &Cas) -> Result<()> {
    let mut statement = connection.prepare(
        "SELECT blob_hash, blob_size
           FROM artifact_revisions
          WHERE inline_blob IS NULL",
    )?;
    let rows = statement.query_map([], |row| {
        Ok(BlobRef {
            hash: row.get(0)?,
            size_bytes: row.get::<_, i64>(1)? as u64,
            inline_bytes: None,
        })
    })?;
    for row in rows {
        cas.verify(&row?)?;
    }
    Ok(())
}

fn set_private_regular_file(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
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
    if version == 4 {
        return migrate_schema_v4_to_v5(connection);
    }
    if version == 3 {
        return migrate_schema_v3_to_v4(connection);
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
    migrate_schema_v3_to_v4(connection)
}

fn migrate_schema_v3_to_v4(connection: &mut Connection) -> Result<()> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(
        "DELETE FROM artifact_trigram_fts;
         INSERT INTO artifact_trigram_fts(revision_id, artifact_id, title, body)
           SELECT id, artifact_id, title, search_text FROM artifact_revisions;
         DELETE FROM claim_trigram_fts;
         INSERT INTO claim_trigram_fts(revision_id, claim_id, statement)
           SELECT id, claim_id, statement FROM claim_revisions;
         DELETE FROM artifact_chunk_fts;",
    )?;
    rebuild_artifact_chunk_index(&transaction)?;
    transaction.execute(
        "UPDATE meta SET value = '4' WHERE key = 'schema_version'",
        [],
    )?;
    verify_schema_v4_transaction(&transaction)?;
    transaction.commit()?;
    migrate_schema_v4_to_v5(connection)
}

fn verify_schema_v4_transaction(transaction: &Transaction<'_>) -> Result<()> {
    let integrity: String =
        transaction.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        return Err(MemoryError::Integrity(format!(
            "schema migration failed SQLite integrity_check before commit: {integrity}"
        )));
    }
    let foreign_key_violation: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check LIMIT 1)",
        [],
        |row| row.get(0),
    )?;
    if foreign_key_violation {
        return Err(MemoryError::Integrity(
            "schema migration introduced a foreign-key violation".into(),
        ));
    }
    let version: i64 = transaction.query_row(
        "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'schema_version'",
        [],
        |row| row.get(0),
    )?;
    if version != 4 {
        return Err(MemoryError::Integrity(format!(
            "schema-v4 migration staged version {version}, expected 4"
        )));
    }
    let missing_artifact_trigrams: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM artifact_revisions ar
          WHERE NOT EXISTS (
            SELECT 1 FROM artifact_trigram_fts tf WHERE tf.revision_id = ar.id
          )",
        [],
        |row| row.get(0),
    )?;
    let missing_claim_trigrams: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM claim_revisions cr
          WHERE NOT EXISTS (
            SELECT 1 FROM claim_trigram_fts tf WHERE tf.revision_id = cr.id
          )",
        [],
        |row| row.get(0),
    )?;
    let missing_artifact_chunks: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM artifact_revisions ar
          WHERE NOT EXISTS (
            SELECT 1 FROM artifact_chunk_fts cf WHERE cf.revision_id = ar.id
          )",
        [],
        |row| row.get(0),
    )?;
    if missing_artifact_trigrams != 0 || missing_claim_trigrams != 0 || missing_artifact_chunks != 0
    {
        return Err(MemoryError::Integrity(format!(
            "schema migration projection verification failed before commit: missing artifact trigrams={missing_artifact_trigrams}, claim trigrams={missing_claim_trigrams}, artifact chunks={missing_artifact_chunks}"
        )));
    }
    Ok(())
}

fn migrate_schema_v4_to_v5(connection: &mut Connection) -> Result<()> {
    // The idempotent SCHEMA pass creates every v5 table and index before this
    // transaction. v5 adds no authoritative rows during migration, so the
    // only state transition is the version marker after integrity checks.
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute(
        "UPDATE meta SET value = '5' WHERE key = 'schema_version'",
        [],
    )?;
    let integrity: String =
        transaction.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        return Err(MemoryError::Integrity(format!(
            "schema-v5 migration failed SQLite integrity_check before commit: {integrity}"
        )));
    }
    let foreign_key_violation: bool = transaction.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_foreign_key_check LIMIT 1)",
        [],
        |row| row.get(0),
    )?;
    if foreign_key_violation {
        return Err(MemoryError::Integrity(
            "schema-v5 migration introduced a foreign-key violation".into(),
        ));
    }
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

fn load_source(connection: &Connection, source_id: &str) -> Result<Option<SourceRecord>> {
    let raw = connection
        .query_row(
            "SELECT id, name, kind, locator, metadata_json, health, cursor,
                    health_message, last_observed_at, workspace_id, project_id,
                    task_id, component, created_at, updated_at, commit_seq
               FROM sources WHERE id = ?1",
            [source_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<DateTime<Utc>>>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                    row.get::<_, Option<String>>(11)?,
                    row.get::<_, Option<String>>(12)?,
                    row.get::<_, DateTime<Utc>>(13)?,
                    row.get::<_, DateTime<Utc>>(14)?,
                    row.get::<_, i64>(15)?,
                ))
            },
        )
        .optional()?;
    raw.map(
        |(
            source_id,
            name,
            kind,
            locator,
            metadata_json,
            health,
            cursor,
            health_message,
            last_observed_at,
            workspace_id,
            project_id,
            task_id,
            component,
            created_at,
            updated_at,
            commit_seq,
        )| {
            Ok(SourceRecord {
                source_id,
                name,
                kind,
                locator,
                metadata: serde_json::from_str(&metadata_json)?,
                health: parse_enum(&health)?,
                cursor,
                health_message,
                last_observed_at,
                context: AmbientContext {
                    workspace_id,
                    project_id,
                    task_id,
                    component,
                    pins: Vec::new(),
                },
                created_at,
                updated_at,
                commit_seq,
            })
        },
    )
    .transpose()
}

#[derive(Debug, Clone)]
struct RawSourceItem {
    source_id: String,
    external_id: String,
    external_revision: String,
    payload_digest: String,
    artifact_id: String,
    artifact_revision_id: String,
    state: String,
    observed_at: DateTime<Utc>,
    withdrawn_at: Option<DateTime<Utc>>,
    withdrawal_reason: Option<String>,
    updated_at: DateTime<Utc>,
    commit_seq: i64,
}

impl RawSourceItem {
    fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            source_id: row.get(0)?,
            external_id: row.get(1)?,
            external_revision: row.get(2)?,
            payload_digest: row.get(3)?,
            artifact_id: row.get(4)?,
            artifact_revision_id: row.get(5)?,
            state: row.get(6)?,
            observed_at: row.get(7)?,
            withdrawn_at: row.get(8)?,
            withdrawal_reason: row.get(9)?,
            updated_at: row.get(10)?,
            commit_seq: row.get(11)?,
        })
    }

    fn into_record(self) -> SourceItemRecord {
        SourceItemRecord {
            source_id: self.source_id,
            external_id: self.external_id,
            external_revision: self.external_revision,
            artifact_id: self.artifact_id,
            artifact_revision_id: self.artifact_revision_id,
            state: self.state,
            observed_at: self.observed_at,
            withdrawn_at: self.withdrawn_at,
            withdrawal_reason: self.withdrawal_reason,
            updated_at: self.updated_at,
            commit_seq: self.commit_seq,
        }
    }
}

fn load_source_item(
    connection: &Connection,
    source_id: &str,
    external_id: &str,
) -> Result<Option<RawSourceItem>> {
    Ok(connection
        .query_row(
            "SELECT source_id, external_id, external_revision, payload_digest,
                    artifact_id, artifact_revision_id, state, observed_at,
                    withdrawn_at, withdrawal_reason, updated_at, commit_seq
               FROM source_items WHERE source_id = ?1 AND external_id = ?2",
            params![source_id, external_id],
            RawSourceItem::from_row,
        )
        .optional()?)
}

#[derive(Debug, Clone)]
struct RawProjection {
    projection_id: String,
    artifact_id: String,
    revision_id: String,
    projection_key: String,
    kind: String,
    text: String,
    evidence_json: String,
    generator: String,
    generator_version: String,
    generator_digest: String,
    payload_digest: String,
    metadata_json: String,
    status: String,
    dropped_reason: Option<String>,
    actor: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    commit_seq: i64,
}

impl RawProjection {
    fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            projection_id: row.get(0)?,
            artifact_id: row.get(1)?,
            revision_id: row.get(2)?,
            projection_key: row.get(3)?,
            kind: row.get(4)?,
            text: row.get(5)?,
            evidence_json: row.get(6)?,
            generator: row.get(7)?,
            generator_version: row.get(8)?,
            generator_digest: row.get(9)?,
            payload_digest: row.get(10)?,
            metadata_json: row.get(11)?,
            status: row.get(12)?,
            dropped_reason: row.get(13)?,
            actor: row.get(14)?,
            created_at: row.get(15)?,
            updated_at: row.get(16)?,
            commit_seq: row.get(17)?,
        })
    }

    fn into_record(self) -> Result<ProjectionRecord> {
        Ok(ProjectionRecord {
            projection_id: self.projection_id,
            artifact_id: self.artifact_id,
            revision_id: self.revision_id,
            projection_key: self.projection_key,
            kind: self.kind,
            text: self.text,
            evidence_spans: serde_json::from_str(&self.evidence_json)?,
            generator: self.generator,
            generator_version: self.generator_version,
            generator_digest: self.generator_digest,
            metadata: serde_json::from_str(&self.metadata_json)?,
            status: self.status,
            dropped_reason: self.dropped_reason,
            actor: self.actor,
            created_at: self.created_at,
            updated_at: self.updated_at,
            commit_seq: self.commit_seq,
        })
    }
}

fn projection_select() -> &'static str {
    "SELECT id, artifact_id, revision_id, projection_key, kind, text,
            evidence_json, generator, generator_version, generator_digest,
            payload_digest, metadata_json, status, dropped_reason, actor,
            created_at, updated_at, commit_seq
       FROM retrieval_projections"
}

fn load_projection(connection: &Connection, projection_id: &str) -> Result<Option<RawProjection>> {
    Ok(connection
        .query_row(
            &format!("{} WHERE id = ?1", projection_select()),
            [projection_id],
            RawProjection::from_row,
        )
        .optional()?)
}

fn load_projection_by_key(
    connection: &Connection,
    revision_id: &str,
    projection_key: &str,
) -> Result<Option<RawProjection>> {
    Ok(connection
        .query_row(
            &format!(
                "{} WHERE revision_id = ?1 AND projection_key = ?2",
                projection_select()
            ),
            params![revision_id, projection_key],
            RawProjection::from_row,
        )
        .optional()?)
}

#[derive(Debug)]
struct RawFeedback {
    feedback_id: String,
    outcome: String,
    query_fingerprint: String,
    retained_query: Option<String>,
    citations_json: String,
    note: Option<String>,
    actor: Option<String>,
    context: AmbientContext,
    created_at: DateTime<Utc>,
    commit_seq: i64,
}

impl RawFeedback {
    fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            feedback_id: row.get(0)?,
            outcome: row.get(1)?,
            query_fingerprint: row.get(2)?,
            retained_query: row.get(3)?,
            citations_json: row.get(4)?,
            note: row.get(5)?,
            actor: row.get(6)?,
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

    fn into_record(self) -> Result<FeedbackRecord> {
        Ok(FeedbackRecord {
            feedback_id: self.feedback_id,
            outcome: parse_enum(&self.outcome)?,
            query_fingerprint: self.query_fingerprint,
            retained_query: self.retained_query,
            citations: serde_json::from_str(&self.citations_json)?,
            note: self.note,
            actor: self.actor,
            context: self.context,
            created_at: self.created_at,
            commit_seq: self.commit_seq,
        })
    }
}

fn feedback_select() -> &'static str {
    "SELECT id, outcome, query_fingerprint, retained_query, citations_json,
            note, actor, workspace_id, project_id, task_id, component,
            created_at, commit_seq
       FROM retrieval_feedback"
}

fn load_feedback(connection: &Connection, feedback_id: &str) -> Result<Option<FeedbackRecord>> {
    connection
        .query_row(
            &format!("{} WHERE id = ?1", feedback_select()),
            [feedback_id],
            RawFeedback::from_row,
        )
        .optional()?
        .map(RawFeedback::into_record)
        .transpose()
}

fn feedback_row(row: &Row<'_>) -> rusqlite::Result<FeedbackRecord> {
    RawFeedback::from_row(row)?.into_record().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
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
    commit_seq: i64,
    index_rowid: i64,
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
        commit_seq: row.get(14)?,
        index_rowid: row.get(15)?,
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
            commit_seq: self.commit_seq,
            index_rowid: self.index_rowid,
            lexical_candidate: true,
            trigram_score: None,
            projection_score: None,
            semantic_score: None,
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
    commit_seq: i64,
    index_rowid: i64,
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
        commit_seq: row.get(18)?,
        index_rowid: row.get(19)?,
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
            commit_seq: self.commit_seq,
            index_rowid: self.index_rowid,
            lexical_candidate: true,
            trigram_score: None,
            projection_score: None,
            semantic_score: None,
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
    /// Stable final tie-breaker for equal retrieval scores. Entity IDs are
    /// random ULIDs and must never decide ranked retrieval order.
    commit_seq: i64,
    index_rowid: i64,
    lexical_candidate: bool,
    trigram_score: Option<f64>,
    projection_score: Option<f64>,
    semantic_score: Option<f64>,
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
        lexical_policy_version: LEXICAL_POLICY_VERSION.into(),
        trigram_policy_version: TRIGRAM_POLICY_VERSION.into(),
        fusion_policy_version: FUSION_POLICY_VERSION.into(),
        query_unit_count: 0,
        matched_unit_count: 0,
        required_matches: 0,
        lexical_coverage: 0.0,
        phrase_group_count: 0,
        matched_phrase_group_count: 0,
        lexical_qualified: false,
        trigram_qualified: false,
        semantic_qualified: false,
        qualified: false,
        matched_terms: Vec::new(),
        matched_phrase_groups: Vec::new(),
        trigram_matched_terms: Vec::new(),
        trigram_similarity: None,
        semantic_similarity: None,
        exact_tier: false,
        fusion_score: 0.0,
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
        candidate.hit.ranking.policy_version = RECENCY_POLICY_VERSION.into();
        candidate.hit.ranking.recency_enabled = enabled;
        candidate.hit.ranking.recency_eligible = eligible;
        candidate.hit.ranking.recency_bonus = recency_bonus;
        candidate.hit.ranking.lexical_position = index + 1;
        candidate.hit.ranking.final_position = index + 1;
        candidate.hit.ranking.max_promotion = RECENCY_MAX_PROMOTION;
        candidate.hit.ranking.effective_at = candidate.effective_at;
        candidate.hit.ranking.effective_at_basis = candidate.effective_at_basis;
        candidate.hit.ranking.evaluated_at = evaluated_at;
        candidate.hit.ranking.decay_class = candidate.profile.class;
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
                && same_lexical_qualification_rank(&candidate, &reranked[insertion_index - 1])
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

fn order_hits_with_reranker(
    reranker: &RerankerManager,
    entity_type: Option<EntityType>,
    query: &str,
    hits: &mut [SearchHit],
) -> Result<RerankerRetrievalStatus> {
    for hit in hits.iter_mut() {
        hit.provenance.insert(
            "retrieval_tier".into(),
            Value::String(
                if hit.ranking.exact_tier {
                    "exact_match"
                } else {
                    "candidate"
                }
                .into(),
            ),
        );
        if hit.ranking.exact_tier {
            hit.provenance
                .insert("model_independent_ranking".into(), Value::Bool(true));
        }
    }
    let candidate_count = hits.iter().filter(|hit| !hit.ranking.exact_tier).count();
    if entity_type != Some(EntityType::Claim) {
        let surface = if entity_type == Some(EntityType::Artifact) {
            "artifact"
        } else {
            "mixed"
        };
        return reranker.surface_disabled(surface, candidate_count);
    }
    let positions = reranker_slate_positions(hits, RERANKER_ORDERING_CANDIDATE_LIMIT);
    let passages = positions
        .iter()
        .map(|index| hits[*index].excerpt.as_str())
        .collect::<Vec<_>>();
    let (order, mut status) = reranker.order(query, &passages, candidate_count)?;
    if order.is_empty() {
        return Ok(status);
    }
    if order.len() != positions.len() || order.iter().any(|index| *index >= positions.len()) || {
        let mut unique = order.clone();
        unique.sort_unstable();
        unique.dedup();
        unique.len() != positions.len()
    } {
        return Err(MemoryError::Integrity(
            "reranker returned an invalid candidate permutation".into(),
        ));
    }
    apply_reranker_order(hits, &positions, &order);
    status.ordering_applied = status.scored_candidate_count > 1;
    Ok(status)
}

fn reranker_slate_positions(hits: &[SearchHit], limit: usize) -> Vec<usize> {
    let mut positions = Vec::with_capacity(limit);
    for qualified in [true, false] {
        let tier = hits
            .iter()
            .enumerate()
            .filter(|(_, hit)| !hit.ranking.exact_tier && hit.ranking.qualified == qualified)
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        let remaining = limit.saturating_sub(positions.len());
        if remaining == 0 {
            break;
        }
        if tier.len() <= remaining {
            positions.extend(tier);
            continue;
        }

        let fused_quota = remaining.div_ceil(2);
        let semantic_quota = remaining / 2;
        let mut selected = BTreeSet::new();
        for index in tier.iter().take(fused_quota) {
            if selected.insert(*index) {
                positions.push(*index);
            }
        }

        let mut semantic = tier.clone();
        semantic.sort_by(|left, right| {
            hits[*right]
                .ranking
                .semantic_similarity
                .partial_cmp(&hits[*left].ranking.semantic_similarity)
                .unwrap_or(Ordering::Equal)
                .then_with(|| left.cmp(right))
        });
        for index in semantic.into_iter().take(semantic_quota) {
            if selected.insert(index) {
                positions.push(index);
            }
        }
        for index in tier {
            if positions.len() >= limit {
                break;
            }
            if selected.insert(index) {
                positions.push(index);
            }
        }
    }
    positions
}

fn apply_reranker_order(hits: &mut [SearchHit], positions: &[usize], order: &[usize]) {
    let candidates = positions
        .iter()
        .map(|position| hits[*position].clone())
        .collect::<Vec<_>>();
    for qualified in [true, false] {
        let mut targets = positions
            .iter()
            .copied()
            .filter(|position| hits[*position].ranking.qualified == qualified)
            .collect::<Vec<_>>();
        targets.sort_unstable();
        let ordered = order
            .iter()
            .map(|index| candidates[*index].clone())
            .filter(|hit| hit.ranking.qualified == qualified)
            .collect::<Vec<_>>();
        for (target, hit) in targets.into_iter().zip(ordered) {
            hits[target] = hit;
        }
    }
    for (index, hit) in hits.iter_mut().enumerate() {
        hit.ranking.final_position = index + 1;
    }
}

fn retain_conflicted_hits_within_limit(hits: &mut [SearchHit], limit: usize) {
    if hits.len() <= limit || limit == 0 {
        return;
    }
    let mut replacement_positions = (0..limit)
        .filter(|index| !hits[*index].ranking.exact_tier && hits[*index].status != "conflicted")
        .collect::<Vec<_>>();
    let promoted_positions = (limit..hits.len())
        .filter(|index| hits[*index].status == "conflicted")
        .collect::<Vec<_>>();
    for promoted in promoted_positions {
        let Some(replacement) = replacement_positions.pop() else {
            break;
        };
        hits.swap(replacement, promoted);
        let hit = &mut hits[replacement];
        hit.provenance.insert(
            "conflict_retention".into(),
            json!({
                "policy_version": "conflict_retention_v1",
                "reason": "unresolved conflict evidence retained within the response limit",
            }),
        );
        if !hit
            .matched_by
            .iter()
            .any(|channel| channel == "conflict_retention_v1")
        {
            hit.matched_by.push("conflict_retention_v1".into());
        }
    }
    for (index, hit) in hits.iter_mut().enumerate() {
        hit.ranking.final_position = index + 1;
    }
}

fn same_lexical_qualification_rank(left: &SearchCandidate, right: &SearchCandidate) -> bool {
    left.hit.ranking.exact_tier == right.hit.ranking.exact_tier
        && left.hit.ranking.lexical_qualified == right.hit.ranking.lexical_qualified
        && left.hit.ranking.trigram_qualified == right.hit.ranking.trigram_qualified
        && left.hit.ranking.semantic_qualified == right.hit.ranking.semantic_qualified
        && left.hit.ranking.matched_phrase_group_count
            == right.hit.ranking.matched_phrase_group_count
        && left.hit.ranking.matched_unit_count == right.hit.ranking.matched_unit_count
        && left.hit.ranking.query_unit_count == right.hit.ranking.query_unit_count
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

fn bounded_utf8_preview(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
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

fn semantic_claim_revision_hash(statement: &str) -> String {
    blake3::hash(statement.as_bytes()).to_hex().to_string()
}

fn eligible_semantic_revisions(
    connection: &Connection,
    context: &AmbientContext,
    input: &SearchInput,
    entity_type: Option<EntityType>,
    evaluated_at: DateTime<Utc>,
) -> Result<BTreeMap<String, EligibleSemanticRevision>> {
    let (pin_artifacts, exact_revision_pins) = normalized_artifact_pins(&context.pins);
    let pins = serde_json::to_string(&pin_artifacts)?;
    let pinned_revisions = serde_json::to_string(&exact_revision_pins)?;
    let horizon = enum_string(&input.horizon)?;
    let historical = i64::from(input.include_historical);
    let mut eligible = BTreeMap::new();

    if !matches!(entity_type, Some(EntityType::Claim)) {
        let sql = format!(
            "SELECT ar.id, a.id, ar.blob_hash
               FROM artifact_revisions ar
               JOIN artifacts a ON a.id = ar.artifact_id
              WHERE (?1 = 1 OR (a.status = 'active' AND (
                         a.current_revision_id = ar.id
                         OR (a.id || '@' || ar.id) IN
                            (SELECT value FROM json_each(?8))
                    )))
                AND ({} OR (a.id || '@' || ar.id) IN
                     (SELECT value FROM json_each(?8)))",
            horizon_filter_sql("a", true)
        );
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(
            params![
                historical,
                "",
                horizon,
                context.workspace_id,
                context.project_id,
                context.task_id,
                pins,
                pinned_revisions,
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )?;
        for row in rows {
            let (revision_id, entity_id, revision_hash) = row?;
            eligible.insert(
                revision_id,
                EligibleSemanticRevision {
                    entity_type: EntityType::Artifact,
                    entity_id,
                    revision_hash,
                },
            );
        }
    }

    if !matches!(entity_type, Some(EntityType::Artifact)) {
        let sql = format!(
            "SELECT cr.id, c.id, cr.statement
               FROM claim_revisions cr
               JOIN claims c ON c.id = cr.claim_id
              WHERE (?1 = 1 OR (
                         c.status IN ('active', 'conflicted')
                         AND c.current_revision_id = cr.id
                         AND (c.valid_from IS NULL OR c.valid_from <= ?7)
                         AND (c.valid_until IS NULL OR c.valid_until > ?7)
                    ))
                AND {}",
            horizon_filter_sql("c", false)
        );
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(
            params![
                historical,
                "",
                horizon,
                context.workspace_id,
                context.project_id,
                context.task_id,
                evaluated_at,
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )?;
        for row in rows {
            let (revision_id, entity_id, statement) = row?;
            eligible.insert(
                revision_id,
                EligibleSemanticRevision {
                    entity_type: EntityType::Claim,
                    entity_id,
                    revision_hash: semantic_claim_revision_hash(&statement),
                },
            );
        }
    }

    Ok(eligible)
}

fn append_projection_candidates(
    connection: &Connection,
    cas: &Cas,
    context: &AmbientContext,
    input: &SearchInput,
    analysis: &AnalyzedQuery,
    candidate_limit: i64,
    candidates: &mut Vec<SearchCandidate>,
) -> Result<()> {
    let pins = serde_json::to_string(&normalized_artifact_pins(&context.pins).0)?;
    let horizon = enum_string(&input.horizon)?;
    let sql = "SELECT a.id, ar.id, ar.title, ar.title,
                      a.status, a.workspace_id, a.project_id, a.task_id, a.component,
                      a.kind, ar.provenance_json, ar.created_at,
                      a.current_revision_id = ar.id, 0.0, ar.commit_seq,
                      COALESCE((SELECT rowid FROM artifact_fts
                                WHERE revision_id = ar.id LIMIT 1), 0),
                      rp.id, rp.kind, rp.text, rp.evidence_json,
                      rp.generator, rp.generator_version, rp.generator_digest,
                      bm25(projection_fts, 0.0, 0.0, 0.0, 1.0),
                      projection_fts.rowid
                 FROM projection_fts
                 JOIN retrieval_projections rp ON rp.id = projection_fts.projection_id
                 JOIN artifact_revisions ar ON ar.id = rp.revision_id
                 JOIN artifacts a ON a.id = rp.artifact_id
                WHERE projection_fts MATCH ?1
                  AND rp.status = 'active' AND a.status = 'active'
                  AND a.current_revision_id = rp.revision_id
                  AND (
                    ?2 = 'personal'
                    OR (?2 = 'workspace' AND a.workspace_id = ?3)
                    OR (?2 = 'ambient' AND a.workspace_id = ?3 AND a.project_id = ?4
                        AND (?5 IS NULL OR a.task_id IS NULL OR a.task_id = ?5))
                    OR a.id IN (SELECT value FROM json_each(?6))
                  )
                ORDER BY bm25(projection_fts, 0.0, 0.0, 0.0, 1.0),
                         rp.commit_seq DESC
                LIMIT ?7";
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map(
        params![
            analysis.fts_expression(),
            horizon,
            context.workspace_id,
            context.project_id,
            context.task_id,
            pins,
            candidate_limit,
        ],
        |row| {
            Ok((
                search_artifact_row(row)?,
                row.get::<_, String>(16)?,
                row.get::<_, String>(17)?,
                row.get::<_, String>(18)?,
                row.get::<_, String>(19)?,
                row.get::<_, String>(20)?,
                row.get::<_, String>(21)?,
                row.get::<_, String>(22)?,
                row.get::<_, f64>(23)?,
                row.get::<_, i64>(24)?,
            ))
        },
    )?;
    for row in rows {
        let (
            artifact_row,
            projection_id,
            projection_kind,
            projection_text,
            evidence_json,
            generator,
            generator_version,
            generator_digest,
            rank,
            projection_rowid,
        ) = row?;
        let spans: Vec<ProjectionSpan> = serde_json::from_str(&evidence_json)?;
        let first = spans.first().ok_or_else(|| {
            MemoryError::Integrity(format!("projection {projection_id} has no evidence spans"))
        })?;
        let raw = load_artifact_raw(
            connection,
            &artifact_row.id,
            Some(&artifact_row.revision_id),
        )?
        .ok_or_else(|| {
            MemoryError::Integrity(format!(
                "projection {projection_id} references missing artifact revision"
            ))
        })?;
        let bytes = cas.get(&raw.blob)?;
        let start = usize::try_from(first.start_byte).map_err(|_| MemoryError::ContentTooLarge)?;
        let end = usize::try_from(first.end_byte).map_err(|_| MemoryError::ContentTooLarge)?;
        if start >= end || end > bytes.len() {
            return Err(MemoryError::Integrity(format!(
                "projection {projection_id} evidence is outside its artifact revision"
            )));
        }
        let exact_excerpt = String::from_utf8(bytes[start..end].to_vec()).map_err(|_| {
            MemoryError::Integrity(format!(
                "projection {projection_id} evidence is not an exact UTF-8 span"
            ))
        })?;
        let score = normalized_rank(rank);
        let mut matched_units = Vec::new();
        for unit in &analysis.units {
            if matching_fts_rowids(
                connection,
                "projection_fts",
                &unit.expression,
                &[projection_rowid],
            )?
            .contains(&projection_rowid)
            {
                matched_units.push(unit.display.clone());
            }
        }
        let mut candidate = artifact_row.into_candidate()?;
        candidate.lexical_candidate = false;
        candidate.projection_score = Some(score);
        candidate.hit.score = 0.0;
        candidate.hit.ranking.lexical_score = 0.0;
        candidate.hit.ranking.query_unit_count = analysis.units.len();
        candidate.hit.ranking.required_matches = analysis.required_matches;
        candidate.hit.ranking.matched_unit_count = matched_units.len();
        candidate.hit.ranking.lexical_coverage = if analysis.units.is_empty() {
            0.0
        } else {
            matched_units.len() as f64 / analysis.units.len() as f64
        };
        candidate.hit.ranking.matched_terms = matched_units;
        candidate.hit.excerpt = exact_excerpt;
        candidate.hit.citation = format!(
            "memoree://artifact/{}@{}#{}-{}",
            candidate.hit.entity_id, candidate.hit.revision_id, first.start_byte, first.end_byte
        );
        candidate.hit.matched_by.clear();
        candidate
            .hit
            .matched_by
            .push(PROJECTION_POLICY_VERSION.into());
        candidate.hit.provenance.insert(
            "derived_projection".into(),
            json!({
                "projection_id": projection_id,
                "kind": projection_kind,
                "generator": generator,
                "generator_version": generator_version,
                "generator_digest": generator_digest,
                "candidate_only": true,
                "projection_score": score,
                "matched_text_preview": bounded_utf8_preview(&projection_text, 512),
                "evidence_spans": spans,
            }),
        );
        candidate.hit.provenance.insert(
            "retrieval_span".into(),
            json!({
                "start_byte": first.start_byte,
                "end_byte": first.end_byte,
                "coordinate_space": "immutable_artifact_bytes",
                "selected_by": PROJECTION_POLICY_VERSION,
            }),
        );
        if let Some(existing) = candidates.iter_mut().find(|existing| {
            existing.hit.entity_type == EntityType::Artifact
                && existing.hit.entity_id == candidate.hit.entity_id
                && existing.hit.revision_id == candidate.hit.revision_id
        }) {
            let replace = existing
                .projection_score
                .is_none_or(|existing_score| score > existing_score);
            existing.projection_score = Some(
                existing
                    .projection_score
                    .map_or(score, |existing_score| existing_score.max(score)),
            );
            if !existing
                .hit
                .matched_by
                .iter()
                .any(|channel| channel == PROJECTION_POLICY_VERSION)
            {
                existing
                    .hit
                    .matched_by
                    .push(PROJECTION_POLICY_VERSION.into());
            }
            if replace && !existing.hit.ranking.qualified {
                existing.hit.excerpt = candidate.hit.excerpt;
                existing.hit.citation = candidate.hit.citation;
                existing.hit.provenance.insert(
                    "derived_projection".into(),
                    candidate.hit.provenance["derived_projection"].clone(),
                );
                existing.hit.provenance.insert(
                    "retrieval_span".into(),
                    candidate.hit.provenance["retrieval_span"].clone(),
                );
            }
        } else {
            candidates.push(candidate);
        }
    }
    Ok(())
}

fn append_semantic_candidates(
    connection: &Connection,
    hits: &[SemanticHit],
    evaluated_at: DateTime<Utc>,
    candidates: &mut Vec<SearchCandidate>,
) -> Result<()> {
    for hit in hits {
        if let Some(existing) = candidates.iter_mut().find(|candidate| {
            candidate.hit.entity_type == hit.entity_type
                && candidate.hit.entity_id == hit.entity_id
                && candidate.hit.revision_id == hit.revision_id
        }) {
            apply_semantic_hit(connection, existing, hit)?;
            continue;
        }
        let candidate = match hit.entity_type {
            EntityType::Artifact => connection
                .query_row(
                    "SELECT a.id, ar.id, ar.title, ar.title,
                            a.status, a.workspace_id, a.project_id, a.task_id, a.component,
                            a.kind, ar.provenance_json, ar.created_at,
                            a.current_revision_id = ar.id,
                            0.0, ar.commit_seq,
                            COALESCE((SELECT rowid FROM artifact_fts
                                      WHERE revision_id = ar.id LIMIT 1), 0)
                       FROM artifact_revisions ar
                       JOIN artifacts a ON a.id = ar.artifact_id
                      WHERE ar.id = ?1",
                    [&hit.revision_id],
                    search_artifact_row,
                )
                .optional()?
                .map(ArtifactSearchRow::into_candidate)
                .transpose()?,
            EntityType::Claim => connection
                .query_row(
                    "SELECT c.id, cr.id, c.claim_type, cr.statement, c.status,
                            c.workspace_id, c.project_id, c.task_id, c.component,
                            cr.evidence_json, cr.confidence, c.valid_from, c.valid_until,
                            c.current_revision_id = cr.id, ?2,
                            CASE
                              WHEN c.valid_from IS NOT NULL AND c.valid_from > ?2 THEN 'future'
                              WHEN c.valid_until IS NOT NULL AND c.valid_until <= ?2 THEN 'expired'
                              ELSE 'current'
                            END,
                            cr.created_at, 0.0, cr.commit_seq,
                            COALESCE((SELECT rowid FROM claim_fts
                                      WHERE revision_id = cr.id LIMIT 1), 0)
                       FROM claim_revisions cr
                       JOIN claims c ON c.id = cr.claim_id
                      WHERE cr.id = ?1",
                    params![hit.revision_id, evaluated_at],
                    search_claim_row,
                )
                .optional()?
                .map(ClaimSearchRow::into_candidate)
                .transpose()?,
        };
        let Some(mut candidate) = candidate else {
            continue;
        };
        candidate.lexical_candidate = false;
        candidate.hit.score = 0.0;
        candidate.hit.ranking.lexical_score = 0.0;
        candidate.hit.matched_by.clear();
        apply_semantic_hit(connection, &mut candidate, hit)?;
        candidates.push(candidate);
    }
    Ok(())
}

fn apply_semantic_hit(
    connection: &Connection,
    candidate: &mut SearchCandidate,
    hit: &SemanticHit,
) -> Result<()> {
    candidate.semantic_score = Some(hit.similarity);
    candidate.hit.ranking.semantic_similarity = Some(hit.similarity);
    // Dense cosine creates candidates only. It has no calibrated answerability
    // meaning and cannot qualify or suppress evidence.
    candidate.hit.ranking.semantic_qualified = false;
    let channel = "semantic_candidate_v1";
    if !candidate
        .hit
        .matched_by
        .iter()
        .any(|matched| matched == channel)
    {
        candidate.hit.matched_by.push(channel.into());
    }
    if !matches!(hit.entity_type, EntityType::Artifact) {
        return Ok(());
    }
    match (hit.start_byte, hit.end_byte) {
        (Some(start_byte), Some(end_byte)) => {
            let authority_chunk = connection
                .query_row(
                    "SELECT CAST(start_byte AS INTEGER), body
                       FROM artifact_chunk_fts
                      WHERE revision_id = ?1 AND row_kind = 'body'
                        AND CAST(start_byte AS INTEGER) <= ?2
                        AND CAST(end_byte AS INTEGER) >= ?3
                      ORDER BY CAST(end_byte AS INTEGER) - CAST(start_byte AS INTEGER),
                               CAST(ordinal AS INTEGER)
                      LIMIT 1",
                    params![
                        hit.revision_id,
                        i64::try_from(start_byte).map_err(|_| MemoryError::ContentTooLarge)?,
                        i64::try_from(end_byte).map_err(|_| MemoryError::ContentTooLarge)?,
                    ],
                    |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()?;
            let Some((authority_start, authority_body)) = authority_chunk else {
                return Err(MemoryError::Integrity(format!(
                    "semantic artifact span {}-{} is absent from authoritative chunks for revision {}",
                    start_byte, end_byte, hit.revision_id
                )));
            };
            let authority_start =
                u64::try_from(authority_start).map_err(|_| MemoryError::ContentTooLarge)?;
            let relative_start = usize::try_from(start_byte.saturating_sub(authority_start))
                .map_err(|_| MemoryError::ContentTooLarge)?;
            let relative_end = usize::try_from(end_byte.saturating_sub(authority_start))
                .map_err(|_| MemoryError::ContentTooLarge)?;
            if start_byte < authority_start
                || relative_start >= relative_end
                || relative_end > authority_body.len()
                || !authority_body.is_char_boundary(relative_start)
                || !authority_body.is_char_boundary(relative_end)
            {
                return Err(MemoryError::Integrity(format!(
                    "semantic artifact span {}-{} is not an exact UTF-8 slice of revision {}",
                    start_byte, end_byte, hit.revision_id
                )));
            }
            let excerpt = authority_body[relative_start..relative_end].to_owned();
            candidate.hit.excerpt = excerpt;
            candidate.hit.citation = format!(
                "memoree://artifact/{}@{}#{}-{}",
                candidate.hit.entity_id, candidate.hit.revision_id, start_byte, end_byte
            );
            candidate.hit.provenance.insert(
                "retrieval_span".into(),
                json!({
                    "start_byte": start_byte,
                    "end_byte": end_byte,
                    "chunker_version": ARTIFACT_CHUNKER_VERSION,
                    "coordinate_space": "immutable_artifact_bytes",
                    "selected_by": SEMANTIC_POLICY_VERSION,
                    "semantic_window_max_bytes": SEMANTIC_WINDOW_MAX_BYTES,
                    "semantic_window_overlap_bytes": SEMANTIC_WINDOW_OVERLAP_BYTES,
                }),
            );
            if !candidate
                .hit
                .matched_by
                .iter()
                .any(|matched| matched == "artifact_citation_span_v1")
            {
                candidate
                    .hit
                    .matched_by
                    .push("artifact_citation_span_v1".into());
            }
        }
        (None, None) => {
            candidate
                .hit
                .provenance
                .insert("retrieval_match".into(), Value::String("title".into()));
        }
        _ => {
            return Err(MemoryError::Integrity(format!(
                "semantic artifact span is incomplete for revision {}",
                hit.revision_id
            )));
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn append_trigram_candidates(
    connection: &Connection,
    context: &AmbientContext,
    input: &SearchInput,
    entity_type: Option<EntityType>,
    analysis: &AnalyzedQuery,
    candidate_limit: i64,
    evaluated_at: DateTime<Utc>,
    candidates: &mut Vec<SearchCandidate>,
) -> Result<()> {
    if entity_type.is_none() {
        // Each FTS vocabulary has independent document frequencies. Building
        // one anchor expression from their summed frequencies can suppress an
        // artifact that is recoverable in artifact-only recall (or vice
        // versa), breaking cross-surface citation parity.
        append_trigram_candidates(
            connection,
            context,
            input,
            Some(EntityType::Artifact),
            analysis,
            candidate_limit,
            evaluated_at,
            candidates,
        )?;
        append_trigram_candidates(
            connection,
            context,
            input,
            Some(EntityType::Claim),
            analysis,
            candidate_limit,
            evaluated_at,
            candidates,
        )?;
        return Ok(());
    }
    let Some(query) = trigram_fts_expression(connection, analysis, entity_type)? else {
        return Ok(());
    };
    let (pin_artifacts, exact_revision_pins) = normalized_artifact_pins(&context.pins);
    let pins = serde_json::to_string(&pin_artifacts)?;
    let pinned_revisions = serde_json::to_string(&exact_revision_pins)?;
    let horizon = enum_string(&input.horizon)?;
    let historical = i64::from(input.include_historical);

    if !matches!(entity_type, Some(EntityType::Claim)) {
        let sql = format!(
            "SELECT a.id, ar.id, ar.title,
                    snippet(artifact_trigram_fts, -1, '', '', ' … ', 64),
                    a.status, a.workspace_id, a.project_id, a.task_id, a.component,
                    a.kind, ar.provenance_json, ar.created_at,
                    a.current_revision_id = ar.id,
                    bm25(artifact_trigram_fts, 0.0, 0.0, 5.0, 1.0), ar.commit_seq,
                    (SELECT rowid FROM artifact_fts WHERE revision_id = ar.id LIMIT 1)
               FROM artifact_trigram_fts
               JOIN artifact_revisions ar ON ar.id = artifact_trigram_fts.revision_id
               JOIN artifacts a ON a.id = ar.artifact_id
              WHERE artifact_trigram_fts MATCH ?1
                AND (?2 = 1 OR (a.status = 'active' AND (
                     a.current_revision_id = ar.id
                     OR (a.id || '@' || ar.id) IN (SELECT value FROM json_each(?8))
                )))
                AND ({} OR (a.id || '@' || ar.id) IN (SELECT value FROM json_each(?8)))
              ORDER BY bm25(artifact_trigram_fts, 0.0, 0.0, 5.0, 1.0), ar.commit_seq DESC
              LIMIT ?9",
            horizon_filter_sql("a", true)
        );
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(
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
        for row in rows {
            let row = row?;
            let match_text = format!("{} {}", row.title, row.excerpt);
            let trigram_score = normalized_rank(row.rank);
            let mut candidate = row.into_candidate()?;
            prepare_trigram_candidate(&mut candidate, analysis, &match_text, trigram_score);
            merge_trigram_candidate(candidates, candidate);
        }
    }

    if !matches!(entity_type, Some(EntityType::Artifact)) {
        let sql = format!(
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
                    bm25(claim_trigram_fts, 0.0, 0.0, 1.0), cr.commit_seq,
                    (SELECT rowid FROM claim_fts WHERE revision_id = cr.id LIMIT 1)
               FROM claim_trigram_fts
               JOIN claim_revisions cr ON cr.id = claim_trigram_fts.revision_id
               JOIN claims c ON c.id = cr.claim_id
              WHERE claim_trigram_fts MATCH ?1
                AND (?2 = 1 OR (
                     c.status IN ('active', 'conflicted')
                     AND c.current_revision_id = cr.id
                     AND (c.valid_from IS NULL OR c.valid_from <= ?10)
                     AND (c.valid_until IS NULL OR c.valid_until > ?10)
                ))
                AND {}
              ORDER BY bm25(claim_trigram_fts, 0.0, 0.0, 1.0), cr.commit_seq DESC
              LIMIT ?9",
            horizon_filter_sql("c", false)
        );
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(
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
        for row in rows {
            let row = row?;
            let match_text = row.statement.clone();
            let trigram_score = normalized_rank(row.rank);
            let mut candidate = row.into_candidate()?;
            prepare_trigram_candidate(&mut candidate, analysis, &match_text, trigram_score);
            merge_trigram_candidate(candidates, candidate);
        }
    }
    Ok(())
}

fn prepare_trigram_candidate(
    candidate: &mut SearchCandidate,
    analysis: &AnalyzedQuery,
    match_text: &str,
    trigram_score: f64,
) {
    let (similarity, matched_terms, qualified) = fuzzy_token_qualification(analysis, match_text);
    candidate.lexical_candidate = false;
    candidate.trigram_score = Some(trigram_score);
    candidate.hit.score = 0.0;
    candidate.hit.ranking.lexical_score = 0.0;
    candidate.hit.ranking.trigram_similarity = Some(similarity);
    candidate.hit.ranking.trigram_matched_terms = matched_terms;
    candidate.hit.ranking.trigram_qualified = qualified;
    candidate.hit.matched_by = vec![if qualified {
        TRIGRAM_POLICY_VERSION.into()
    } else {
        "trigram_candidate_v1".into()
    }];
}

fn merge_trigram_candidate(candidates: &mut Vec<SearchCandidate>, fuzzy: SearchCandidate) {
    if let Some(existing) = candidates.iter_mut().find(|candidate| {
        candidate.hit.entity_type == fuzzy.hit.entity_type
            && candidate.hit.entity_id == fuzzy.hit.entity_id
            && candidate.hit.revision_id == fuzzy.hit.revision_id
    }) {
        existing.trigram_score = fuzzy.trigram_score;
        existing.hit.ranking.trigram_similarity = fuzzy.hit.ranking.trigram_similarity;
        existing.hit.ranking.trigram_matched_terms = fuzzy.hit.ranking.trigram_matched_terms;
        existing.hit.ranking.trigram_qualified = fuzzy.hit.ranking.trigram_qualified;
        if !existing
            .hit
            .matched_by
            .iter()
            .any(|channel| channel == TRIGRAM_POLICY_VERSION)
            && fuzzy.hit.ranking.trigram_qualified
        {
            existing.hit.matched_by.push(TRIGRAM_POLICY_VERSION.into());
        }
    } else {
        candidates.push(fuzzy);
    }
}

fn trigram_fts_expression(
    connection: &Connection,
    analysis: &AnalyzedQuery,
    entity_type: Option<EntityType>,
) -> Result<Option<String>> {
    let words = fuzzy_query_words(analysis);
    let mut trigrams_by_word = Vec::new();
    let mut all_trigrams = BTreeSet::new();
    for word in words {
        let characters = word.chars().collect::<Vec<_>>();
        let trigrams = characters
            .windows(3)
            .map(|window| window.iter().collect::<String>())
            .collect::<BTreeSet<_>>();
        all_trigrams.extend(trigrams.iter().cloned());
        trigrams_by_word.push(trigrams);
    }
    if all_trigrams.is_empty() {
        return Ok(None);
    }
    let trigram_json = serde_json::to_string(&all_trigrams)?;
    let mut frequencies = BTreeMap::<String, i64>::new();
    for table in match entity_type {
        Some(EntityType::Artifact) => vec!["artifact_trigram_vocab"],
        Some(EntityType::Claim) => vec!["claim_trigram_vocab"],
        None => vec!["artifact_trigram_vocab", "claim_trigram_vocab"],
    } {
        let sql = format!(
            "SELECT term, doc FROM {table}
              WHERE term IN (SELECT value FROM json_each(?1))"
        );
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map([&trigram_json], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        for row in rows {
            let (trigram, documents) = row?;
            *frequencies.entry(trigram).or_default() += documents;
        }
    }
    let anchors = trigrams_by_word
        .into_iter()
        .flat_map(|trigrams| {
            let mut ranked = trigrams
                .into_iter()
                .filter_map(|trigram| {
                    frequencies
                        .get(&trigram)
                        .copied()
                        .filter(|documents| *documents > 0)
                        .map(|documents| (documents, trigram))
                })
                .collect::<Vec<_>>();
            ranked.sort();
            ranked
                .into_iter()
                .take(2)
                .map(|(_, trigram)| trigram)
                .collect::<Vec<_>>()
        })
        .collect::<BTreeSet<_>>();
    Ok((!anchors.is_empty()).then(|| {
        anchors
            .into_iter()
            .map(|trigram| format!("\"{trigram}\""))
            .collect::<Vec<_>>()
            .join(" OR ")
    }))
}

fn fuzzy_query_words(analysis: &AnalyzedQuery) -> Vec<String> {
    analysis
        .units
        .iter()
        .flat_map(|unit| {
            unit.display
                .split(|character: char| !character.is_alphanumeric() && character != '_')
        })
        .filter(|word| word.chars().count() >= 3)
        .map(str::to_owned)
        .collect()
}

fn fuzzy_token_qualification(
    analysis: &AnalyzedQuery,
    candidate_text: &str,
) -> (f64, Vec<String>, bool) {
    let query_words = fuzzy_query_words(analysis);
    if query_words.is_empty() {
        return (0.0, Vec::new(), false);
    }
    let candidate_words = candidate_text
        .split(|character: char| !character.is_alphanumeric() && character != '_')
        .filter(|word| word.chars().count() >= 3)
        .map(|word| word.to_lowercase())
        .collect::<BTreeSet<_>>();
    let threshold = if query_words.len() == 1 { 0.80 } else { 0.72 };
    let mut scores = Vec::with_capacity(query_words.len());
    let mut matched = Vec::new();
    for query_word in &query_words {
        let best = candidate_words
            .iter()
            .map(|candidate| normalized_edit_similarity(query_word, candidate))
            .fold(0.0_f64, f64::max);
        scores.push(best);
        if best >= threshold {
            matched.push(query_word.clone());
        }
    }
    let required = if query_words.len() == 1 {
        1
    } else {
        (query_words.len() * 3).div_ceil(5)
    };
    let similarity = scores.iter().copied().sum::<f64>() / scores.len() as f64;
    let qualified = matched.len() >= required;
    (similarity, matched, qualified)
}

fn normalized_edit_similarity(left: &str, right: &str) -> f64 {
    let left = left.chars().collect::<Vec<_>>();
    let right = right.chars().collect::<Vec<_>>();
    let longest = left.len().max(right.len());
    if longest == 0 {
        return 1.0;
    }
    let mut previous = (0..=right.len()).collect::<Vec<_>>();
    let mut current = vec![0; right.len() + 1];
    for (left_index, left_character) in left.iter().enumerate() {
        current[0] = left_index + 1;
        for (right_index, right_character) in right.iter().enumerate() {
            current[right_index + 1] = (previous[right_index + 1] + 1)
                .min(current[right_index] + 1)
                .min(previous[right_index] + usize::from(left_character != right_character));
        }
        std::mem::swap(&mut previous, &mut current);
    }
    1.0 - previous[right.len()] as f64 / longest as f64
}

fn finalize_trigram_qualification_and_fusion(candidates: &mut [SearchCandidate]) {
    let mut lexical_order = (0..candidates.len())
        .filter(|index| candidates[*index].lexical_candidate)
        .collect::<Vec<_>>();
    lexical_order.sort_by(|left, right| {
        candidates[*right]
            .hit
            .ranking
            .lexical_score
            .partial_cmp(&candidates[*left].hit.ranking.lexical_score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                candidates[*right]
                    .commit_seq
                    .cmp(&candidates[*left].commit_seq)
            })
    });
    let mut trigram_order = (0..candidates.len())
        .filter(|index| candidates[*index].trigram_score.is_some())
        .collect::<Vec<_>>();
    trigram_order.sort_by(|left, right| {
        candidates[*right]
            .trigram_score
            .unwrap_or_default()
            .partial_cmp(&candidates[*left].trigram_score.unwrap_or_default())
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                candidates[*right]
                    .commit_seq
                    .cmp(&candidates[*left].commit_seq)
            })
    });
    let mut semantic_order = (0..candidates.len())
        .filter(|index| candidates[*index].semantic_score.is_some())
        .collect::<Vec<_>>();
    semantic_order.sort_by(|left, right| {
        candidates[*right]
            .semantic_score
            .unwrap_or_default()
            .partial_cmp(&candidates[*left].semantic_score.unwrap_or_default())
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                candidates[*right]
                    .commit_seq
                    .cmp(&candidates[*left].commit_seq)
            })
            .then_with(|| {
                candidates[*left]
                    .hit
                    .revision_id
                    .cmp(&candidates[*right].hit.revision_id)
            })
    });
    let mut projection_order = (0..candidates.len())
        .filter(|index| candidates[*index].projection_score.is_some())
        .collect::<Vec<_>>();
    projection_order.sort_by(|left, right| {
        candidates[*right]
            .projection_score
            .unwrap_or_default()
            .partial_cmp(&candidates[*left].projection_score.unwrap_or_default())
            .unwrap_or(Ordering::Equal)
            .then_with(|| {
                candidates[*right]
                    .commit_seq
                    .cmp(&candidates[*left].commit_seq)
            })
            .then_with(|| {
                candidates[*left]
                    .hit
                    .revision_id
                    .cmp(&candidates[*right].hit.revision_id)
            })
    });
    let mut lexical_ranks = vec![None; candidates.len()];
    let mut trigram_ranks = vec![None; candidates.len()];
    let mut semantic_ranks = vec![None; candidates.len()];
    let mut projection_ranks = vec![None; candidates.len()];
    for (rank, index) in lexical_order.into_iter().enumerate() {
        lexical_ranks[index] = Some(rank + 1);
    }
    for (rank, index) in trigram_order.into_iter().enumerate() {
        trigram_ranks[index] = Some(rank + 1);
    }
    for (rank, index) in semantic_order.into_iter().enumerate() {
        semantic_ranks[index] = Some(rank + 1);
    }
    for (rank, index) in projection_order.into_iter().enumerate() {
        projection_ranks[index] = Some(rank + 1);
    }
    for (index, candidate) in candidates.iter_mut().enumerate() {
        let ranking = &mut candidate.hit.ranking;
        ranking.lexical_qualified = ranking.qualified;
        ranking.qualified =
            ranking.lexical_qualified || ranking.trigram_qualified || ranking.semantic_qualified;
        ranking.exact_tier = ranking.lexical_qualified && ranking.lexical_coverage == 1.0;
        ranking.fusion_policy_version = FUSION_POLICY_VERSION.into();
        ranking.trigram_policy_version = TRIGRAM_POLICY_VERSION.into();
        let lexical_rrf = lexical_ranks[index].map_or(0.0, |rank| 1.0 / (RRF_K + rank as f64));
        let trigram_rrf = trigram_ranks[index].map_or(0.0, |rank| 0.7 / (RRF_K + rank as f64));
        let semantic_rrf = semantic_ranks[index].map_or(0.0, |rank| 0.85 / (RRF_K + rank as f64));
        let projection_rrf =
            projection_ranks[index].map_or(0.0, |rank| 0.75 / (RRF_K + rank as f64));
        // Quantization prevents platform-level floating-point noise from
        // becoming an observable ordering decision. Stable authority fields
        // remain the final tie-breakers.
        let fused = if ranking.exact_tier {
            // Exact/full lexical results are model-independent: optional typo
            // or dense channels may annotate them but can never reorder them.
            lexical_rrf
        } else {
            lexical_rrf + trigram_rrf + semantic_rrf + projection_rrf
        };
        ranking.fusion_score = (fused * 1e12).round() / 1e12;
        candidate.hit.score = ranking.fusion_score;
        if ranking.qualified
            && !candidate
                .hit
                .matched_by
                .iter()
                .any(|channel| channel == FUSION_POLICY_VERSION)
        {
            candidate.hit.matched_by.push(FUSION_POLICY_VERSION.into());
        }
    }
    candidates.sort_by(|left, right| {
        right
            .hit
            .ranking
            .exact_tier
            .cmp(&left.hit.ranking.exact_tier)
            .then_with(|| {
                right
                    .hit
                    .ranking
                    .fusion_score
                    .partial_cmp(&left.hit.ranking.fusion_score)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| {
                right
                    .hit
                    .ranking
                    .matched_phrase_group_count
                    .cmp(&left.hit.ranking.matched_phrase_group_count)
            })
            .then_with(|| {
                right
                    .hit
                    .ranking
                    .lexical_coverage
                    .partial_cmp(&left.hit.ranking.lexical_coverage)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| right.commit_seq.cmp(&left.commit_seq))
            .then_with(|| left.hit.entity_id.cmp(&right.hit.entity_id))
    });
}

#[derive(Clone)]
struct QueryUnit {
    display: String,
    expression: String,
    phrase: bool,
    component_count: usize,
}

#[derive(Clone)]
struct AnalyzedQuery {
    units: Vec<QueryUnit>,
    dropped_stopwords: Vec<String>,
    normalized_query: String,
    required_matches: usize,
}

impl AnalyzedQuery {
    fn fts_expression(&self) -> String {
        self.units
            .iter()
            .map(|unit| unit.expression.as_str())
            .collect::<Vec<_>>()
            .join(" OR ")
    }

    fn phrase_count(&self) -> usize {
        self.units.iter().filter(|unit| unit.phrase).count()
    }

    fn public(&self) -> QueryAnalysis {
        QueryAnalysis {
            policy_version: LEXICAL_POLICY_VERSION.into(),
            normalized_query: self.normalized_query.clone(),
            content_units: self.units.iter().map(|unit| unit.display.clone()).collect(),
            phrase_groups: self
                .units
                .iter()
                .filter(|unit| unit.phrase)
                .map(|unit| unit.display.clone())
                .collect(),
            dropped_stopwords: self.dropped_stopwords.clone(),
            required_matches: self.required_matches,
            retrieval_profile: retrieval_profile(&self.normalized_query),
        }
    }
}

fn retrieval_profile(query: &str) -> RetrievalProfile {
    let normalized = query.to_lowercase();
    let identifier_like_units = normalized
        .split_whitespace()
        .filter(|unit| {
            unit.contains('_')
                || unit.chars().any(|character| character.is_ascii_digit())
                    && unit.chars().any(|character| character.is_alphabetic())
                || unit.contains("::")
                || unit.contains('/')
        })
        .count();
    let contains_any = |phrases: &[&str]| phrases.iter().any(|phrase| normalized.contains(phrase));
    let intent_hint = if contains_any(&[
        "previous",
        "previously",
        "prior",
        "earlier",
        "history",
        "historical",
        "decided",
        "decision",
        "learned",
        "audit",
        "remember",
    ]) {
        RetrievalIntentHint::HistoricalMemory
    } else if contains_any(&[
        "current code",
        "current implementation",
        "currently",
        "source code",
        "where is",
        "defined in",
        "this version",
        "after the migration",
        "after migration",
        "right now",
        "latest state",
    ]) {
        RetrievalIntentHint::CurrentSource
    } else if identifier_like_units > 0 {
        RetrievalIntentHint::IdentifierLookup
    } else {
        RetrievalIntentHint::Ambiguous
    };
    let authority_hint = match intent_hint {
        RetrievalIntentHint::HistoricalMemory => "memory_for_history_repository_for_current_source",
        RetrievalIntentHint::CurrentSource => "repository_authoritative",
        RetrievalIntentHint::IdentifierLookup => "repository_first_for_live_identifiers",
        RetrievalIntentHint::Ambiguous => "compare_memory_with_current_authoritative_source",
    };
    RetrievalProfile {
        policy_version: "conservative_query_profile_v1".into(),
        intent_hint,
        script_profile: query_script_profile(query),
        identifier_like_units,
        semantic_role: "candidate_and_ordering_only".into(),
        authority_hint: authority_hint.into(),
    }
}

fn query_script_profile(query: &str) -> QueryScriptProfile {
    let mut scripts = BTreeSet::new();
    for character in query.chars().filter(|character| character.is_alphabetic()) {
        let code = character as u32;
        let script = if code <= 0x024f || (0x1e00..=0x1eff).contains(&code) {
            QueryScriptProfile::Latin
        } else if (0x0400..=0x052f).contains(&code) {
            QueryScriptProfile::Cyrillic
        } else if (0x0600..=0x06ff).contains(&code)
            || (0x0750..=0x077f).contains(&code)
            || (0x08a0..=0x08ff).contains(&code)
        {
            QueryScriptProfile::Arabic
        } else if (0x3040..=0x30ff).contains(&code)
            || (0x3400..=0x9fff).contains(&code)
            || (0xac00..=0xd7af).contains(&code)
        {
            QueryScriptProfile::Cjk
        } else {
            QueryScriptProfile::Other
        };
        scripts.insert(script as u8);
    }
    if scripts.is_empty() {
        return QueryScriptProfile::Unknown;
    }
    if scripts.len() > 1 {
        return QueryScriptProfile::Mixed;
    }
    match *scripts
        .iter()
        .next()
        .unwrap_or(&(QueryScriptProfile::Other as u8))
    {
        value if value == QueryScriptProfile::Latin as u8 => QueryScriptProfile::Latin,
        value if value == QueryScriptProfile::Cyrillic as u8 => QueryScriptProfile::Cyrillic,
        value if value == QueryScriptProfile::Arabic as u8 => QueryScriptProfile::Arabic,
        value if value == QueryScriptProfile::Cjk as u8 => QueryScriptProfile::Cjk,
        _ => QueryScriptProfile::Other,
    }
}

fn analyze_query(query: &str) -> Result<AnalyzedQuery> {
    if query.len() > MAX_QUERY_BYTES {
        return Err(MemoryError::InvalidRequest(format!(
            "search query must not exceed {MAX_QUERY_BYTES} bytes"
        )));
    }
    let mut parsed = parse_query_units(query);
    if parsed.is_empty() {
        return Err(MemoryError::InvalidRequest(
            "search query must contain at least one word or identifier".into(),
        ));
    }
    let component_count: usize = parsed.iter().map(|unit| unit.component_count).sum();
    if component_count > 48 {
        return Err(MemoryError::InvalidRequest(
            "search query must not contain more than 48 words or identifiers".into(),
        ));
    }

    let has_content = parsed
        .iter()
        .any(|unit| unit.phrase || !is_query_stopword(&unit.display));
    let mut dropped_stopwords = Vec::new();
    if has_content {
        parsed.retain(|unit| {
            let drop = !unit.phrase && is_query_stopword(&unit.display);
            if drop {
                dropped_stopwords.push(unit.display.clone());
            }
            !drop
        });
    }
    let mut seen = BTreeSet::new();
    parsed.retain(|unit| seen.insert((unit.phrase, unit.display.clone())));
    dropped_stopwords.sort();
    dropped_stopwords.dedup();
    let required_matches = required_lexical_matches(parsed.len());
    let normalized_query = parsed
        .iter()
        .map(|unit| {
            if unit.phrase {
                format!("\"{}\"", unit.display)
            } else {
                unit.display.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    Ok(AnalyzedQuery {
        units: parsed,
        dropped_stopwords,
        normalized_query,
        required_matches,
    })
}

fn annotate_lexical_matches(
    connection: &Connection,
    candidates: &mut [SearchCandidate],
    analysis: &AnalyzedQuery,
) -> Result<()> {
    let artifact_rowids = candidates
        .iter()
        .filter(|candidate| matches!(candidate.hit.entity_type, EntityType::Artifact))
        .map(|candidate| candidate.index_rowid)
        .collect::<Vec<_>>();
    let claim_rowids = candidates
        .iter()
        .filter(|candidate| matches!(candidate.hit.entity_type, EntityType::Claim))
        .map(|candidate| candidate.index_rowid)
        .collect::<Vec<_>>();
    let mut artifact_matches = Vec::with_capacity(analysis.units.len());
    let mut claim_matches = Vec::with_capacity(analysis.units.len());
    for unit in &analysis.units {
        artifact_matches.push(matching_fts_rowids(
            connection,
            "artifact_fts",
            &unit.expression,
            &artifact_rowids,
        )?);
        claim_matches.push(matching_fts_rowids(
            connection,
            "claim_fts",
            &unit.expression,
            &claim_rowids,
        )?);
    }

    let phrase_group_count = analysis.phrase_count();
    for candidate in candidates {
        let matches = match candidate.hit.entity_type {
            EntityType::Artifact => &artifact_matches,
            EntityType::Claim => &claim_matches,
        };
        let mut matched_terms = Vec::new();
        let mut matched_phrase_groups = Vec::new();
        for (unit, rowids) in analysis.units.iter().zip(matches) {
            if rowids.contains(&candidate.index_rowid) {
                if unit.phrase {
                    matched_phrase_groups.push(unit.display.clone());
                } else {
                    matched_terms.push(unit.display.clone());
                }
            }
        }
        let matched_unit_count = matched_terms.len() + matched_phrase_groups.len();
        let phrase_requirements_met = matched_phrase_groups.len() == phrase_group_count;
        let qualified = phrase_requirements_met
            && matched_unit_count >= analysis.required_matches
            && matched_unit_count > 0;
        let lexical_coverage = if analysis.units.is_empty() {
            0.0
        } else {
            matched_unit_count as f64 / analysis.units.len() as f64
        };
        candidate.hit.ranking.lexical_policy_version = LEXICAL_POLICY_VERSION.into();
        candidate.hit.ranking.query_unit_count = analysis.units.len();
        candidate.hit.ranking.matched_unit_count = matched_unit_count;
        candidate.hit.ranking.required_matches = analysis.required_matches;
        candidate.hit.ranking.lexical_coverage = lexical_coverage;
        candidate.hit.ranking.phrase_group_count = phrase_group_count;
        candidate.hit.ranking.matched_phrase_group_count = matched_phrase_groups.len();
        candidate.hit.ranking.qualified = qualified;
        candidate.hit.ranking.matched_terms = matched_terms;
        candidate.hit.ranking.matched_phrase_groups = matched_phrase_groups;
        candidate.hit.matched_by.push(if qualified {
            LEXICAL_POLICY_VERSION.into()
        } else {
            "lexical_candidate_v1".into()
        });
    }
    Ok(())
}

fn matching_fts_rowids(
    connection: &Connection,
    table: &str,
    expression: &str,
    candidate_rowids: &[i64],
) -> Result<BTreeSet<i64>> {
    if candidate_rowids.is_empty() {
        return Ok(BTreeSet::new());
    }
    let candidate_json = serde_json::to_string(candidate_rowids)?;
    let sql = format!(
        "SELECT rowid FROM {table}
         WHERE {table} MATCH ?1
           AND rowid IN (SELECT CAST(value AS INTEGER) FROM json_each(?2))"
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(params![expression, candidate_json], |row| row.get(0))?;
    rows.collect::<rusqlite::Result<BTreeSet<i64>>>()
        .map_err(Into::into)
}

#[derive(Debug)]
struct ArtifactChunkMatch {
    rowid: i64,
    revision_id: String,
    row_kind: String,
    ordinal: Option<i64>,
    start_byte: Option<i64>,
    end_byte: Option<i64>,
    chunker_version: i64,
    exact_excerpt: String,
    lexical_score: f64,
    matched_unit_count: usize,
    matched_phrase_count: usize,
}

fn select_artifact_citation_spans(
    connection: &Connection,
    candidates: &mut [SearchCandidate],
    analysis: &AnalyzedQuery,
) -> Result<()> {
    let revision_ids = candidates
        .iter()
        .filter(|candidate| matches!(candidate.hit.entity_type, EntityType::Artifact))
        .map(|candidate| candidate.hit.revision_id.clone())
        .collect::<Vec<_>>();
    if revision_ids.is_empty() {
        return Ok(());
    }
    let revision_json = serde_json::to_string(&revision_ids)?;
    let query = analysis.fts_expression();
    let mut matches = {
        let mut statement = connection.prepare(
            "SELECT rowid, revision_id, row_kind, ordinal, start_byte, end_byte,
                    chunker_version,
                    CASE WHEN row_kind = 'title' THEN title ELSE body END,
                    bm25(artifact_chunk_fts, 0.0, 0.0, 0.0, 0.0, 0.0,
                         0.0, 0.0, 5.0, 1.0)
               FROM artifact_chunk_fts
              WHERE artifact_chunk_fts MATCH ?1
                AND revision_id IN (SELECT value FROM json_each(?2))",
        )?;
        let rows = statement.query_map(params![query, revision_json], |row| {
            Ok(ArtifactChunkMatch {
                rowid: row.get(0)?,
                revision_id: row.get(1)?,
                row_kind: row.get(2)?,
                ordinal: row.get(3)?,
                start_byte: row.get(4)?,
                end_byte: row.get(5)?,
                chunker_version: row.get(6)?,
                exact_excerpt: row.get(7)?,
                lexical_score: normalized_rank(row.get(8)?),
                matched_unit_count: 0,
                matched_phrase_count: 0,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    let rowids = matches.iter().map(|item| item.rowid).collect::<Vec<_>>();
    let unit_matches = analysis
        .units
        .iter()
        .map(|unit| {
            matching_fts_rowids(connection, "artifact_chunk_fts", &unit.expression, &rowids)
        })
        .collect::<Result<Vec<_>>>()?;
    for item in &mut matches {
        for (unit, matching_rows) in analysis.units.iter().zip(&unit_matches) {
            if matching_rows.contains(&item.rowid) {
                item.matched_unit_count += 1;
                item.matched_phrase_count += usize::from(unit.phrase);
            }
        }
    }
    matches.sort_by(|left, right| {
        left.revision_id
            .cmp(&right.revision_id)
            .then_with(|| right.matched_phrase_count.cmp(&left.matched_phrase_count))
            .then_with(|| right.matched_unit_count.cmp(&left.matched_unit_count))
            .then_with(|| {
                right
                    .lexical_score
                    .partial_cmp(&left.lexical_score)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| left.ordinal.unwrap_or(-1).cmp(&right.ordinal.unwrap_or(-1)))
            .then_with(|| left.rowid.cmp(&right.rowid))
    });
    let mut best_by_revision = BTreeMap::new();
    for item in matches {
        best_by_revision
            .entry(item.revision_id.clone())
            .or_insert(item);
    }
    for candidate in candidates {
        if !matches!(candidate.hit.entity_type, EntityType::Artifact) {
            continue;
        }
        if candidate.hit.ranking.semantic_qualified
            && !candidate.hit.ranking.lexical_qualified
            && !candidate.hit.ranking.trigram_qualified
        {
            // `apply_semantic_hit` already selected and validated the exact
            // immutable chunk span (or the spanless title). Do not replace it
            // with a weak lexical OR-term match.
            continue;
        }
        if candidate.hit.provenance.contains_key("derived_projection")
            && !candidate.hit.ranking.lexical_qualified
            && !candidate.hit.ranking.trigram_qualified
        {
            // The projection path already resolved the derived match back to
            // an exact span in the immutable source artifact. Preserve it;
            // weak raw OR-term matches must not replace its cited evidence.
            continue;
        }
        let best = if let Some(best) = best_by_revision.remove(&candidate.hit.revision_id) {
            Some(best)
        } else if candidate.hit.ranking.trigram_qualified {
            best_fuzzy_artifact_chunk(connection, candidate, analysis)?
        } else {
            None
        };
        let Some(best) = best else {
            continue;
        };
        candidate.hit.excerpt = best.exact_excerpt;
        if best.row_kind == "body" {
            let (Some(start_byte), Some(end_byte)) = (best.start_byte, best.end_byte) else {
                return Err(MemoryError::Integrity(format!(
                    "artifact chunk body for revision {} has no byte span",
                    candidate.hit.revision_id
                )));
            };
            candidate.hit.citation = format!(
                "memoree://artifact/{}@{}#{}-{}",
                candidate.hit.entity_id, candidate.hit.revision_id, start_byte, end_byte
            );
            candidate.hit.provenance.insert(
                "retrieval_span".into(),
                json!({
                    "start_byte": start_byte,
                    "end_byte": end_byte,
                    "chunker_version": best.chunker_version,
                    "coordinate_space": "immutable_artifact_bytes"
                }),
            );
            if !candidate
                .hit
                .matched_by
                .iter()
                .any(|channel| channel == "artifact_citation_span_v1")
            {
                candidate
                    .hit
                    .matched_by
                    .push("artifact_citation_span_v1".into());
            }
        } else {
            candidate.hit.citation = format!(
                "memoree://artifact/{}@{}",
                candidate.hit.entity_id, candidate.hit.revision_id
            );
            candidate.hit.provenance.remove("retrieval_span");
            candidate
                .hit
                .matched_by
                .retain(|channel| channel != "artifact_citation_span_v1");
            candidate
                .hit
                .provenance
                .insert("retrieval_match".into(), Value::String("title".into()));
        }
    }
    Ok(())
}

fn best_fuzzy_artifact_chunk(
    connection: &Connection,
    candidate: &SearchCandidate,
    analysis: &AnalyzedQuery,
) -> Result<Option<ArtifactChunkMatch>> {
    let mut rows = {
        let mut statement = connection.prepare(
            "SELECT rowid, revision_id, row_kind, ordinal, start_byte, end_byte,
                    chunker_version,
                    CASE WHEN row_kind = 'title' THEN title ELSE body END
               FROM artifact_chunk_fts
              WHERE revision_id = ?1",
        )?;
        let mapped = statement.query_map([&candidate.hit.revision_id], |row| {
            let excerpt = row.get::<_, String>(7)?;
            let (similarity, matched_terms, qualified) =
                fuzzy_token_qualification(analysis, &excerpt);
            Ok((
                ArtifactChunkMatch {
                    rowid: row.get(0)?,
                    revision_id: row.get(1)?,
                    row_kind: row.get(2)?,
                    ordinal: row.get(3)?,
                    start_byte: row.get(4)?,
                    end_byte: row.get(5)?,
                    chunker_version: row.get(6)?,
                    exact_excerpt: excerpt,
                    lexical_score: similarity,
                    matched_unit_count: matched_terms.len(),
                    matched_phrase_count: 0,
                },
                qualified,
            ))
        })?;
        mapped.collect::<rusqlite::Result<Vec<_>>>()?
    };
    rows.retain(|(_, qualified)| *qualified);
    rows.sort_by(|(left, _), (right, _)| {
        right
            .matched_unit_count
            .cmp(&left.matched_unit_count)
            .then_with(|| {
                right
                    .lexical_score
                    .partial_cmp(&left.lexical_score)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| {
                let left_len = left
                    .end_byte
                    .zip(left.start_byte)
                    .map_or(usize::MAX, |(end, start)| (end - start) as usize);
                let right_len = right
                    .end_byte
                    .zip(right.start_byte)
                    .map_or(usize::MAX, |(end, start)| (end - start) as usize);
                left_len.cmp(&right_len)
            })
            .then_with(|| left.ordinal.unwrap_or(-1).cmp(&right.ordinal.unwrap_or(-1)))
    });
    Ok(rows.into_iter().next().map(|(chunk, _)| chunk))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ArtifactChunkSpan {
    start_byte: usize,
    end_byte: usize,
}

fn index_artifact_revision_chunks(
    connection: &Connection,
    revision_id: &str,
    artifact_id: &str,
    title: &str,
    exact_body: &str,
) -> Result<()> {
    connection.execute(
        "INSERT INTO artifact_chunk_fts(
             revision_id, artifact_id, row_kind, ordinal, start_byte,
             end_byte, chunker_version, title, body
         ) VALUES (?1, ?2, 'title', NULL, NULL, NULL, ?3, ?4, '')",
        params![revision_id, artifact_id, ARTIFACT_CHUNKER_VERSION, title],
    )?;
    for (ordinal, span) in artifact_chunk_spans(exact_body).into_iter().enumerate() {
        connection.execute(
            "INSERT INTO artifact_chunk_fts(
                 revision_id, artifact_id, row_kind, ordinal, start_byte,
                 end_byte, chunker_version, title, body
             ) VALUES (?1, ?2, 'body', ?3, ?4, ?5, ?6, '', ?7)",
            params![
                revision_id,
                artifact_id,
                i64::try_from(ordinal).map_err(|_| MemoryError::ContentTooLarge)?,
                i64::try_from(span.start_byte).map_err(|_| MemoryError::ContentTooLarge)?,
                i64::try_from(span.end_byte).map_err(|_| MemoryError::ContentTooLarge)?,
                ARTIFACT_CHUNKER_VERSION,
                &exact_body[span.start_byte..span.end_byte],
            ],
        )?;
    }
    Ok(())
}

fn rebuild_artifact_chunk_index(connection: &Connection) -> Result<()> {
    let revisions = {
        let mut statement = connection.prepare(
            "SELECT id, artifact_id, title, search_text, blob_hash, blob_size
               FROM artifact_revisions ORDER BY commit_seq",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    for (revision_id, artifact_id, title, search_text, blob_hash, blob_size) in revisions {
        // Older schemas allowed lossy UTF-8 conversion. Only a byte-identical
        // search projection may become a cited body coordinate space.
        let byte_identical = i64::try_from(search_text.len()).ok() == Some(blob_size)
            && blake3::hash(search_text.as_bytes()).to_hex().as_str() == blob_hash;
        index_artifact_revision_chunks(
            connection,
            &revision_id,
            &artifact_id,
            &title,
            if byte_identical { &search_text } else { "" },
        )?;
    }
    Ok(())
}

fn artifact_chunk_spans(text: &str) -> Vec<ArtifactChunkSpan> {
    if text.is_empty() {
        return Vec::new();
    }
    let blocks = artifact_structural_blocks(text);
    let mut chunks = Vec::new();
    let mut current_start = blocks[0].start_byte;
    let mut current_end = current_start;
    for block in blocks {
        let block_len = block.end_byte - block.start_byte;
        if block_len > ARTIFACT_CHUNK_MAX_BYTES {
            if current_end > current_start {
                chunks.push(ArtifactChunkSpan {
                    start_byte: current_start,
                    end_byte: current_end,
                });
            }
            chunks.extend(split_oversized_artifact_block(
                text,
                block.start_byte,
                block.end_byte,
            ));
            current_start = block.end_byte;
            current_end = block.end_byte;
            continue;
        }
        if current_end == current_start {
            current_start = block.start_byte;
            current_end = block.end_byte;
            continue;
        }
        let combined = block.end_byte - current_start;
        if combined <= ARTIFACT_CHUNK_TARGET_BYTES
            || (current_end - current_start < ARTIFACT_CHUNK_MIN_TAIL_BYTES
                && combined <= ARTIFACT_CHUNK_MAX_BYTES)
        {
            current_end = block.end_byte;
        } else {
            chunks.push(ArtifactChunkSpan {
                start_byte: current_start,
                end_byte: current_end,
            });
            current_start = block.start_byte;
            current_end = block.end_byte;
        }
    }
    if current_end > current_start {
        chunks.push(ArtifactChunkSpan {
            start_byte: current_start,
            end_byte: current_end,
        });
    }
    if chunks.len() >= 2 {
        let tail = chunks[chunks.len() - 1];
        let previous = chunks[chunks.len() - 2];
        if tail.end_byte - tail.start_byte < ARTIFACT_CHUNK_MIN_TAIL_BYTES
            && tail.end_byte - previous.start_byte <= ARTIFACT_CHUNK_MAX_BYTES
        {
            chunks.pop();
            chunks.last_mut().expect("previous chunk exists").end_byte = tail.end_byte;
        }
    }
    debug_assert_artifact_chunk_partition(text, &chunks);
    chunks
}

/// Produces deterministic, overlapping exact-byte slices small enough to avoid
/// silently truncating evidence at the embedding model's token limit.
fn contextualized_semantic_passage(
    title: &str,
    kind: &str,
    component: Option<&str>,
    passage: &str,
) -> String {
    let title = bounded_utf8_preview(title, 256);
    let kind = bounded_utf8_preview(kind, 96);
    let component = component.map(|value| bounded_utf8_preview(value, 128));
    match component {
        Some(component) => format!(
            "Artifact title: {title}\nArtifact kind: {kind}\nComponent: {component}\nPassage: {passage}"
        ),
        None => format!("Artifact title: {title}\nArtifact kind: {kind}\nPassage: {passage}"),
    }
}

fn contextualized_semantic_claim(
    claim_type: &str,
    component: Option<&str>,
    statement: &str,
) -> String {
    let claim_type = bounded_utf8_preview(claim_type, 96);
    let component = component.map(|value| bounded_utf8_preview(value, 128));
    match component {
        Some(component) => format!(
            "Grounded claim type: {claim_type}\nComponent: {component}\nStatement: {statement}"
        ),
        None => format!("Grounded claim type: {claim_type}\nStatement: {statement}"),
    }
}

fn semantic_window_spans(text: &str) -> Vec<ArtifactChunkSpan> {
    if text.is_empty() {
        return Vec::new();
    }
    let mut windows = Vec::new();
    let mut start = 0usize;
    while start < text.len() {
        let ceiling =
            floor_char_boundary(text, (start + SEMANTIC_WINDOW_MAX_BYTES).min(text.len()));
        let end = if ceiling == text.len() {
            ceiling
        } else {
            preferred_semantic_window_end(text, start, ceiling)
        };
        debug_assert!(end > start);
        windows.push(ArtifactChunkSpan {
            start_byte: start,
            end_byte: end,
        });
        if end == text.len() {
            break;
        }
        let desired = end.saturating_sub(SEMANTIC_WINDOW_OVERLAP_BYTES);
        let mut next_start = nearest_word_boundary(text, desired, start + 1, end);
        if next_start <= start {
            next_start = next_char_boundary(text, start);
        }
        start = next_start;
    }
    debug_assert_semantic_windows(text, &windows);
    windows
}

fn preferred_semantic_window_end(text: &str, start: usize, ceiling: usize) -> usize {
    let preferred_floor = start + SEMANTIC_WINDOW_PREFERRED_MIN_BYTES;
    let mut sentence = None;
    let mut whitespace = None;
    for (relative, character) in text[start..ceiling].char_indices() {
        let boundary = start + relative + character.len_utf8();
        if boundary < preferred_floor {
            continue;
        }
        if character.is_whitespace() {
            whitespace = Some(boundary);
        }
        if matches!(character, '.' | '!' | '?' | '\n') {
            sentence = Some(boundary);
        }
    }
    sentence.or(whitespace).unwrap_or(ceiling)
}

fn nearest_word_boundary(text: &str, desired: usize, minimum: usize, maximum: usize) -> usize {
    let radius = SEMANTIC_WINDOW_OVERLAP_BYTES / 2;
    // Snap forward only. Together with the preferred minimum window size this
    // bounds progress even for adversarial whitespace and prevents O(n)
    // near-duplicate windows.
    let lower = floor_char_boundary(text, desired.max(minimum));
    let upper = floor_char_boundary(text, (desired + radius).min(maximum));
    let mut best = None;
    for (relative, character) in text[lower..upper].char_indices() {
        if !character.is_whitespace() {
            continue;
        }
        let boundary = lower + relative + character.len_utf8();
        if boundary < minimum || boundary >= maximum {
            continue;
        }
        let distance = boundary.abs_diff(desired);
        if best.is_none_or(|(best_distance, best_boundary)| {
            distance < best_distance || (distance == best_distance && boundary < best_boundary)
        }) {
            best = Some((distance, boundary));
        }
    }
    best.map(|(_, boundary)| boundary)
        .unwrap_or_else(|| floor_char_boundary(text, desired.max(minimum)))
}

fn floor_char_boundary(text: &str, mut byte: usize) -> usize {
    byte = byte.min(text.len());
    while byte > 0 && !text.is_char_boundary(byte) {
        byte -= 1;
    }
    byte
}

fn next_char_boundary(text: &str, byte: usize) -> usize {
    byte + text[byte..].chars().next().map_or(0, char::len_utf8)
}

fn debug_assert_semantic_windows(text: &str, windows: &[ArtifactChunkSpan]) {
    debug_assert!(!windows.is_empty());
    debug_assert_eq!(windows[0].start_byte, 0);
    debug_assert_eq!(
        windows.last().map(|window| window.end_byte),
        Some(text.len())
    );
    for (index, window) in windows.iter().enumerate() {
        debug_assert!(window.start_byte < window.end_byte);
        debug_assert!(window.end_byte - window.start_byte <= SEMANTIC_WINDOW_MAX_BYTES);
        debug_assert!(text.is_char_boundary(window.start_byte));
        debug_assert!(text.is_char_boundary(window.end_byte));
        if index > 0 {
            debug_assert!(windows[index - 1].start_byte < window.start_byte);
            debug_assert!(window.start_byte < windows[index - 1].end_byte);
        }
    }
}

fn artifact_structural_blocks(text: &str) -> Vec<ArtifactChunkSpan> {
    let mut blocks = Vec::new();
    let mut block_start = 0;
    let mut offset = 0;
    let mut in_fence = false;
    for line in text.split_inclusive('\n') {
        let line_start = offset;
        offset += line.len();
        let trimmed = line.trim_start();
        let fence = trimmed.starts_with("```") || trimmed.starts_with("~~~");
        let heading = !in_fence && trimmed.starts_with('#');
        if heading && line_start > block_start {
            blocks.push(ArtifactChunkSpan {
                start_byte: block_start,
                end_byte: line_start,
            });
            block_start = line_start;
        }
        if fence {
            if !in_fence && line_start > block_start {
                blocks.push(ArtifactChunkSpan {
                    start_byte: block_start,
                    end_byte: line_start,
                });
                block_start = line_start;
            }
            in_fence = !in_fence;
            if !in_fence {
                blocks.push(ArtifactChunkSpan {
                    start_byte: block_start,
                    end_byte: offset,
                });
                block_start = offset;
            }
        } else if !in_fence && line.trim().is_empty() {
            blocks.push(ArtifactChunkSpan {
                start_byte: block_start,
                end_byte: offset,
            });
            block_start = offset;
        }
    }
    if offset < text.len() {
        offset = text.len();
    }
    if block_start < offset {
        blocks.push(ArtifactChunkSpan {
            start_byte: block_start,
            end_byte: offset,
        });
    }
    blocks.retain(|block| block.end_byte > block.start_byte);
    if blocks.is_empty() {
        blocks.push(ArtifactChunkSpan {
            start_byte: 0,
            end_byte: text.len(),
        });
    }
    blocks
}

fn split_oversized_artifact_block(
    text: &str,
    start_byte: usize,
    end_byte: usize,
) -> Vec<ArtifactChunkSpan> {
    let mut chunks = Vec::new();
    let mut start = start_byte;
    while end_byte - start > ARTIFACT_CHUNK_MAX_BYTES {
        let ceiling = start + ARTIFACT_CHUNK_MAX_BYTES;
        let target = (start + ARTIFACT_CHUNK_TARGET_BYTES).min(ceiling);
        let split = preferred_artifact_split(text, start, target, ceiling);
        chunks.push(ArtifactChunkSpan {
            start_byte: start,
            end_byte: split,
        });
        start = split;
    }
    if start < end_byte {
        chunks.push(ArtifactChunkSpan {
            start_byte: start,
            end_byte,
        });
    }
    chunks
}

fn preferred_artifact_split(text: &str, start: usize, target: usize, ceiling: usize) -> usize {
    let mut ceiling = ceiling;
    while ceiling > start && !text.is_char_boundary(ceiling) {
        ceiling -= 1;
    }
    let range = &text[start..ceiling];
    let target_relative = target.saturating_sub(start).min(range.len());
    let choose = |predicate: &dyn Fn(&str, usize) -> bool| {
        range
            .char_indices()
            .filter_map(|(index, _)| {
                let boundary = index;
                (boundary >= target_relative && predicate(range, boundary)).then_some(boundary)
            })
            .last()
            .map(|relative| start + relative)
    };
    choose(&|source, boundary| source[..boundary].ends_with("\n\n"))
        .or_else(|| choose(&|source, boundary| source[..boundary].ends_with('\n')))
        .or_else(|| {
            choose(&|source, boundary| {
                let before = source[..boundary].chars().next_back();
                let after = source[boundary..].chars().next();
                matches!(before, Some('.' | '!' | '?')) && after.is_some_and(char::is_whitespace)
            })
        })
        .or_else(|| {
            choose(&|source, boundary| {
                source[boundary..]
                    .chars()
                    .next()
                    .is_some_and(char::is_whitespace)
            })
        })
        .unwrap_or(ceiling)
}

fn debug_assert_artifact_chunk_partition(text: &str, chunks: &[ArtifactChunkSpan]) {
    debug_assert!(!chunks.is_empty());
    debug_assert_eq!(chunks[0].start_byte, 0);
    debug_assert_eq!(chunks.last().map(|chunk| chunk.end_byte), Some(text.len()));
    for (index, chunk) in chunks.iter().enumerate() {
        debug_assert!(chunk.start_byte < chunk.end_byte);
        debug_assert!(chunk.end_byte - chunk.start_byte <= ARTIFACT_CHUNK_MAX_BYTES);
        debug_assert!(text.is_char_boundary(chunk.start_byte));
        debug_assert!(text.is_char_boundary(chunk.end_byte));
        if index > 0 {
            debug_assert_eq!(chunks[index - 1].end_byte, chunk.start_byte);
        }
    }
}

fn parse_query_units(query: &str) -> Vec<QueryUnit> {
    let mut units = Vec::new();
    let mut buffer = String::new();
    let mut quoted = false;
    for character in query.chars() {
        if character == '"' {
            if quoted {
                push_phrase_unit(&buffer, &mut units);
            } else {
                push_unquoted_units(&buffer, &mut units);
            }
            buffer.clear();
            quoted = !quoted;
        } else {
            buffer.push(character);
        }
    }
    if quoted {
        // An unmatched quote is treated as ordinary punctuation rather than
        // changing the meaning of the remainder of the query.
        push_unquoted_units(&buffer, &mut units);
    } else {
        push_unquoted_units(&buffer, &mut units);
    }
    units
}

fn push_unquoted_units(source: &str, units: &mut Vec<QueryUnit>) {
    for piece in source.split_whitespace() {
        let trimmed = piece.trim_matches(|character: char| {
            !character.is_alphanumeric() && character != '_' && character != '-'
        });
        let hyphen_parts = trimmed.split('-').collect::<Vec<_>>();
        if hyphen_parts.len() > 1
            && hyphen_parts.iter().all(|part| {
                !part.is_empty()
                    && part
                        .chars()
                        .all(|character| character.is_alphanumeric() || character == '_')
            })
        {
            push_unit_from_tokens(
                hyphen_parts.into_iter().map(str::to_owned).collect(),
                true,
                units,
            );
        } else {
            for token in lexical_tokens(trimmed) {
                push_unit_from_tokens(vec![token], false, units);
            }
        }
    }
}

fn push_phrase_unit(source: &str, units: &mut Vec<QueryUnit>) {
    let tokens = lexical_tokens(source);
    if !tokens.is_empty() {
        push_unit_from_tokens(tokens, true, units);
    }
}

fn push_unit_from_tokens(tokens: Vec<String>, phrase: bool, units: &mut Vec<QueryUnit>) {
    if tokens.is_empty() {
        return;
    }
    let tokens = tokens
        .into_iter()
        .map(|token| token.to_lowercase())
        .collect::<Vec<_>>();
    let display = tokens.join(" ");
    let expression = format!("\"{}\"", display.replace('"', "\"\""));
    units.push(QueryUnit {
        display,
        expression,
        phrase,
        component_count: tokens.len(),
    });
}

fn lexical_tokens(source: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for character in source.chars() {
        if character.is_alphanumeric() || character == '_' {
            current.push(character);
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

/// Fixed, replayable clause decomposition for legacy claim evidence. It is
/// deliberately lexical: no generative model can invent, merge, or rewrite a
/// clause before authority bytes are selected.
fn ranked_claim_evidence_clauses(statement: &str, user_query: &str) -> Vec<String> {
    let mut pieces = statement
        .split(['.', ';', '\n', '\r'])
        .map(str::trim)
        .filter(|piece| !piece.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    for marker in [", and ", ", but ", " while ", " because ", " so ", " and "] {
        let mut next = Vec::new();
        for piece in pieces {
            let split = piece.split(marker).map(str::trim).collect::<Vec<_>>();
            let balanced =
                split.len() > 1 && split.iter().all(|part| lexical_tokens(part).len() >= 2);
            if balanced {
                next.extend(split.into_iter().map(str::to_owned));
            } else {
                next.push(piece);
            }
        }
        pieces = next;
    }

    let query_terms = lexical_tokens(user_query)
        .into_iter()
        .map(|token| token.to_lowercase())
        .filter(|token| !is_query_stopword(token))
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let mut ranked = pieces
        .into_iter()
        .take(32)
        .enumerate()
        .filter_map(|(ordinal, clause)| {
            let normalized = clause.to_lowercase();
            if !seen.insert(normalized) {
                return None;
            }
            let terms = lexical_tokens(&clause)
                .into_iter()
                .map(|token| token.to_lowercase())
                .filter(|token| !is_query_stopword(token))
                .collect::<BTreeSet<_>>();
            let shared = terms.intersection(&query_terms).count();
            let shared_negation = ["no", "not", "never", "without", "only"]
                .into_iter()
                .filter(|term| terms.contains(*term) && query_terms.contains(*term))
                .count();
            Some((shared, shared_negation, ordinal, clause))
        })
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| right.1.cmp(&left.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    let mut clauses = ranked
        .into_iter()
        .take(8)
        .map(|(_, _, _, clause)| clause)
        .collect::<Vec<_>>();
    if clauses.is_empty() && !statement.trim().is_empty() {
        clauses.push(bounded_utf8_preview(statement.trim(), 2 * 1024).to_owned());
    }
    clauses
}

fn required_lexical_matches(unit_count: usize) -> usize {
    match unit_count {
        0 => 0,
        1 => 1,
        2 => 2,
        count => 2.max((count * 2).div_ceil(5)),
    }
}

fn is_query_stopword(token: &str) -> bool {
    matches!(
        token,
        "a" | "about"
            | "an"
            | "and"
            | "are"
            | "as"
            | "at"
            | "be"
            | "been"
            | "but"
            | "by"
            | "can"
            | "could"
            | "did"
            | "do"
            | "does"
            | "for"
            | "from"
            | "had"
            | "has"
            | "have"
            | "how"
            | "i"
            | "if"
            | "in"
            | "into"
            | "is"
            | "it"
            | "its"
            | "may"
            | "of"
            | "on"
            | "or"
            | "should"
            | "that"
            | "the"
            | "their"
            | "this"
            | "to"
            | "was"
            | "were"
            | "what"
            | "when"
            | "where"
            | "which"
            | "who"
            | "why"
            | "will"
            | "with"
            | "would"
    )
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

fn source_payload_digest(
    kind: &str,
    title: &str,
    media_type: &str,
    blob_hash: &str,
    provenance: &BTreeMap<String, Value>,
) -> Result<String> {
    let encoded = serde_json::to_vec(&json!({
        "kind": kind,
        "title": title,
        "media_type": media_type,
        "blob_hash": blob_hash,
        "provenance": provenance,
    }))?;
    Ok(blake3::hash(&encoded).to_hex().to_string())
}

fn projection_payload_digest(input: &ProjectionPutInput) -> Result<String> {
    let encoded = serde_json::to_vec(&json!({
        "artifact_id": input.artifact_id,
        "revision_id": input.revision_id,
        "projection_key": input.projection_key,
        "kind": input.kind,
        "text": input.text,
        "evidence_spans": input.evidence_spans,
        "generator": input.generator,
        "generator_version": input.generator_version,
        "generator_digest": input.generator_digest,
        "metadata": input.metadata,
    }))?;
    Ok(blake3::hash(&encoded).to_hex().to_string())
}

fn validate_projection_spans(spans: &[ProjectionSpan], artifact_size: u64) -> Result<()> {
    let mut previous_end = 0;
    for (index, span) in spans.iter().enumerate() {
        if span.start_byte >= span.end_byte
            || span.end_byte > artifact_size
            || span.end_byte - span.start_byte > MAX_RECALL_EXCERPT_BYTES as u64
        {
            return Err(MemoryError::InvalidRequest(format!(
                "projection evidence span {index} must be non-empty, inside 0..{artifact_size}, and no larger than {MAX_RECALL_EXCERPT_BYTES} bytes"
            )));
        }
        if index > 0 && span.start_byte < previous_end {
            return Err(MemoryError::InvalidRequest(
                "projection evidence spans must be sorted and non-overlapping".into(),
            ));
        }
        previous_end = span.end_byte;
    }
    Ok(())
}

const FEEDBACK_FINGERPRINT_KEY_META: &str = "feedback_fingerprint_key_v1";

fn feedback_fingerprint_key(connection: &Connection) -> Result<[u8; 32]> {
    let encoded: Option<String> = connection
        .query_row(
            "SELECT value FROM meta WHERE key = ?1",
            [FEEDBACK_FINGERPRINT_KEY_META],
            |row| row.get(0),
        )
        .optional()?;
    let bytes = match encoded {
        Some(encoded) => base64::engine::general_purpose::STANDARD_NO_PAD
            .decode(encoded)
            .map_err(|error| {
                MemoryError::Integrity(format!("feedback fingerprint key is invalid: {error}"))
            })?,
        None => {
            let mut bytes = vec![0u8; 32];
            let mut random = File::open("/dev/urandom").map_err(|error| {
                MemoryError::Config(format!(
                    "secure randomness is required for feedback fingerprints: {error}"
                ))
            })?;
            random.read_exact(&mut bytes)?;
            let encoded = base64::engine::general_purpose::STANDARD_NO_PAD.encode(bytes.as_slice());
            connection.execute(
                "INSERT INTO meta(key, value) VALUES (?1, ?2)",
                params![FEEDBACK_FINGERPRINT_KEY_META, encoded],
            )?;
            bytes
        }
    };
    bytes.try_into().map_err(|_| {
        MemoryError::Integrity("feedback fingerprint key must contain exactly 32 bytes".into())
    })
}

fn keyed_query_fingerprint(key: &[u8; 32], normalized_query: &str) -> String {
    blake3::keyed_hash(key, normalized_query.as_bytes())
        .to_hex()
        .to_string()
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
        // Search spans are byte citations into the immutable source. Invalid
        // UTF-8 cannot be transformed lossily without changing that coordinate
        // space, so it remains title-searchable but has no body projection.
        std::str::from_utf8(bytes).unwrap_or_default().to_owned()
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
    use proptest::prelude::*;

    #[test]
    fn legacy_claim_clause_decomposition_is_deterministic_and_query_ranked() {
        let statement = "The raw ceiling passed, but acceptance still requires gzip. The preload imports renderer glue; it never initializes WASM, and adapters only observe acknowledgements.";
        let first = ranked_claim_evidence_clauses(
            statement,
            "Can the preload initialize the WASM compute module?",
        );
        let second = ranked_claim_evidence_clauses(
            statement,
            "Can the preload initialize the WASM compute module?",
        );
        assert_eq!(first, second);
        assert!(first.len() >= 4);
        assert!(first[0].contains("preload") || first[0].contains("WASM"));
        assert!(first.iter().any(|clause| clause.contains("requires gzip")));
        assert!(
            first
                .iter()
                .any(|clause| clause.contains("never initializes WASM"))
        );
        assert!(
            first
                .iter()
                .any(|clause| clause.contains("only observe acknowledgements"))
        );
    }

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
            commit_seq: 0,
            index_rowid: 0,
            lexical_candidate: true,
            trigram_score: None,
            projection_score: None,
            semantic_score: None,
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
    fn reranker_permutation_only_reorders_non_exact_hits_deterministically() {
        let evaluated_at = Utc::now();
        let profile = RecencyProfile {
            class: RecencyDecayClass::General,
            half_life_days: 180.0,
            max_bonus_ratio: 0.05,
        };
        let mut hits = ["exact-a", "exact-b", "candidate-a", "candidate-b"]
            .into_iter()
            .map(|id| ranking_candidate(id, 1.0, evaluated_at, profile).hit)
            .collect::<Vec<_>>();
        hits[0].ranking.exact_tier = true;
        hits[1].ranking.exact_tier = true;
        let exact_before = hits[..2]
            .iter()
            .map(|hit| {
                (
                    hit.entity_id.clone(),
                    hit.excerpt.clone(),
                    hit.citation.clone(),
                )
            })
            .collect::<Vec<_>>();
        let original = hits.clone();

        apply_reranker_order(&mut hits, &[2, 3], &[1, 0]);
        assert_eq!(
            hits[..2]
                .iter()
                .map(|hit| (
                    hit.entity_id.clone(),
                    hit.excerpt.clone(),
                    hit.citation.clone()
                ))
                .collect::<Vec<_>>(),
            exact_before
        );
        assert_eq!(hits[2].entity_id, "candidate-b");
        assert_eq!(hits[2].excerpt, "candidate-b");
        assert!(hits[2].citation.contains("candidate-b"));
        assert!(!hits[2].provenance.contains_key("reranker"));

        let mut restarted = original;
        apply_reranker_order(&mut restarted, &[2, 3], &[1, 0]);
        assert_eq!(
            hits.iter()
                .map(|hit| hit.entity_id.as_str())
                .collect::<Vec<_>>(),
            restarted
                .iter()
                .map(|hit| hit.entity_id.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn reranker_slate_is_the_deterministic_union_of_fused_and_dense_top_eight() {
        let evaluated_at = Utc::now();
        let profile = RecencyProfile {
            class: RecencyDecayClass::General,
            half_life_days: 180.0,
            max_bonus_ratio: 0.05,
        };
        let mut hits = (0..32)
            .map(|index| {
                let mut hit =
                    ranking_candidate(&format!("candidate-{index}"), 1.0, evaluated_at, profile)
                        .hit;
                hit.ranking.semantic_similarity = Some(index as f64 / 32.0);
                hit
            })
            .collect::<Vec<_>>();
        hits[0].ranking.exact_tier = true;
        let slate = reranker_slate_positions(&hits, RERANKER_ORDERING_CANDIDATE_LIMIT);
        let repeated = reranker_slate_positions(&hits, RERANKER_ORDERING_CANDIDATE_LIMIT);
        assert_eq!(slate, repeated);
        assert_eq!(slate.len(), RERANKER_ORDERING_CANDIDATE_LIMIT);
        assert!(!slate.contains(&0));
        for fused in 1..=8 {
            assert!(slate.contains(&fused), "missing fused position {fused}");
        }
        for dense in 24..32 {
            assert!(slate.contains(&dense), "missing dense position {dense}");
        }
    }

    #[test]
    fn reranker_slate_never_spends_a_qualified_slot_on_an_unqualified_hit() {
        let evaluated_at = Utc::now();
        let profile = RecencyProfile {
            class: RecencyDecayClass::General,
            half_life_days: 180.0,
            max_bonus_ratio: 0.05,
        };
        let hits = (0..24)
            .map(|index| {
                let mut hit =
                    ranking_candidate(&format!("tiered-{index}"), 1.0, evaluated_at, profile).hit;
                hit.ranking.qualified = index >= 4;
                hit.ranking.semantic_similarity = Some(index as f64 / 24.0);
                hit
            })
            .collect::<Vec<_>>();
        let slate = reranker_slate_positions(&hits, 8);
        assert_eq!(slate, vec![4, 5, 6, 7, 23, 22, 21, 20]);
        assert!(slate.iter().all(|index| hits[*index].ranking.qualified));
    }

    #[test]
    fn artifact_surface_never_invokes_cross_encoder_ordering() {
        let evaluated_at = Utc::now();
        let profile = RecencyProfile {
            class: RecencyDecayClass::General,
            half_life_days: 180.0,
            max_bonus_ratio: 0.05,
        };
        let mut hits = ["artifact-a", "artifact-b"]
            .into_iter()
            .map(|id| ranking_candidate(id, 1.0, evaluated_at, profile).hit)
            .collect::<Vec<_>>();
        let original_order = hits
            .iter()
            .map(|hit| {
                (
                    hit.entity_id.clone(),
                    hit.excerpt.clone(),
                    hit.citation.clone(),
                )
            })
            .collect::<Vec<_>>();
        let temporary = tempfile::tempdir().unwrap();
        let reranker = RerankerManager::new(temporary.path());

        let status = order_hits_with_reranker(
            &reranker,
            Some(EntityType::Artifact),
            "artifact ordering",
            &mut hits,
        )
        .unwrap();

        assert_eq!(status.state, "surface_disabled");
        assert_eq!(status.surface, "artifact");
        assert_eq!(status.scored_candidate_count, 0);
        assert!(!status.ordering_applied);
        assert_eq!(
            hits.iter()
                .map(|hit| (
                    hit.entity_id.clone(),
                    hit.excerpt.clone(),
                    hit.citation.clone()
                ))
                .collect::<Vec<_>>(),
            original_order
        );
        assert!(
            hits.iter()
                .all(|hit| !hit.provenance.contains_key("reranker"))
        );
    }

    #[test]
    fn unresolved_conflict_candidate_is_retained_without_displacing_exact_hits() {
        let evaluated_at = Utc::now();
        let profile = RecencyProfile {
            class: RecencyDecayClass::General,
            half_life_days: 180.0,
            max_bonus_ratio: 0.05,
        };
        let mut hits = [
            "exact",
            "candidate-a",
            "candidate-b",
            "conflict",
            "candidate-c",
        ]
        .into_iter()
        .map(|id| ranking_candidate(id, 1.0, evaluated_at, profile).hit)
        .collect::<Vec<_>>();
        hits[0].ranking.exact_tier = true;
        hits[3].status = "conflicted".into();

        retain_conflicted_hits_within_limit(&mut hits, 3);

        assert_eq!(hits[0].entity_id, "exact");
        assert!(hits[..3].iter().any(|hit| hit.entity_id == "conflict"));
        let conflict = hits.iter().find(|hit| hit.entity_id == "conflict").unwrap();
        assert_eq!(
            conflict.provenance["conflict_retention"]["policy_version"],
            Value::String("conflict_retention_v1".into())
        );
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
        assert!(searchable_text("text/plain", b"invalid \xff utf8").is_empty());
    }

    #[test]
    fn artifact_chunks_are_a_complete_stable_utf8_partition() {
        let text = format!(
            "# Heading\n\n{}\n\n```text\n{}\n```\n\n{}",
            "paragraph with emoji 🧭. ".repeat(300),
            "fenced λ content ".repeat(400),
            "尾部内容。".repeat(500)
        );
        let first = artifact_chunk_spans(&text);
        let second = artifact_chunk_spans(&text);
        assert_eq!(first, second);
        assert_eq!(first.first().unwrap().start_byte, 0);
        assert_eq!(first.last().unwrap().end_byte, text.len());
        for pair in first.windows(2) {
            assert_eq!(pair[0].end_byte, pair[1].start_byte);
        }
        for chunk in first {
            assert!(chunk.end_byte - chunk.start_byte <= ARTIFACT_CHUNK_MAX_BYTES);
            assert!(text.is_char_boundary(chunk.start_byte));
            assert!(text.is_char_boundary(chunk.end_byte));
        }
    }

    #[test]
    fn semantic_windows_are_stable_bounded_overlapping_utf8_slices() {
        let text = format!(
            "# Memory retrieval\n\n{}\n{}",
            "semantic claims with emoji 🧭 and exact evidence. ".repeat(80),
            "尾部证据必须保持精确。 ".repeat(80)
        );
        let first = semantic_window_spans(&text);
        let second = semantic_window_spans(&text);
        assert_eq!(first, second);
        assert_eq!(first.first().unwrap().start_byte, 0);
        assert_eq!(first.last().unwrap().end_byte, text.len());
        assert!(first.len() > 2);
        for (index, window) in first.iter().enumerate() {
            assert!(window.start_byte < window.end_byte);
            assert!(window.end_byte - window.start_byte <= SEMANTIC_WINDOW_MAX_BYTES);
            assert!(text.is_char_boundary(window.start_byte));
            assert!(text.is_char_boundary(window.end_byte));
            if index > 0 {
                assert!(first[index - 1].start_byte < window.start_byte);
                assert!(window.start_byte < first[index - 1].end_byte);
            }
        }
        for token in text.split_whitespace() {
            let token_start = text.find(token).unwrap();
            let token_end = token_start + token.len();
            assert!(first.iter().any(|window| {
                window.start_byte <= token_start && window.end_byte >= token_end
            }));
        }
    }

    #[test]
    fn semantic_windows_handle_short_and_empty_text() {
        assert!(semantic_window_spans("").is_empty());
        assert_eq!(
            semantic_window_spans("short 🧭 evidence"),
            vec![ArtifactChunkSpan {
                start_byte: 0,
                end_byte: "short 🧭 evidence".len(),
            }]
        );
    }

    #[test]
    fn semantic_documents_add_bounded_context_without_changing_authority_spans() {
        let passage = "The preload never initializes WASM or a Worker.";
        let artifact = contextualized_semantic_passage(
            "PRELOAD-JS-53 startup preload",
            "procedure",
            Some("renderer"),
            passage,
        );
        assert!(artifact.contains("Artifact title: PRELOAD-JS-53 startup preload"));
        assert!(artifact.contains("Artifact kind: procedure"));
        assert!(artifact.contains("Component: renderer"));
        assert!(artifact.ends_with(passage));

        let claim = contextualized_semantic_claim(
            "constraint",
            None,
            "Canvas removal must preserve CLI operation.",
        );
        assert!(claim.starts_with("Grounded claim type: constraint"));
        assert!(claim.ends_with("Canvas removal must preserve CLI operation."));
    }

    proptest! {
        #[test]
        fn semantic_windows_preserve_arbitrary_utf8_with_bounded_progress(
            characters in proptest::collection::vec(any::<char>(), 0..4096)
        ) {
            let text = characters.into_iter().collect::<String>();
            let windows = semantic_window_spans(&text);
            if text.is_empty() {
                prop_assert!(windows.is_empty());
                return Ok(());
            }
            prop_assert_eq!(windows.first().map(|window| window.start_byte), Some(0));
            prop_assert_eq!(windows.last().map(|window| window.end_byte), Some(text.len()));
            prop_assert!(windows.len() <= text.len().div_ceil(256) + 1);
            let mut covered_end = 0usize;
            let mut previous_start = None;
            for window in &windows {
                prop_assert!(window.start_byte < window.end_byte);
                prop_assert!(window.end_byte - window.start_byte <= SEMANTIC_WINDOW_MAX_BYTES);
                prop_assert!(text.is_char_boundary(window.start_byte));
                prop_assert!(text.is_char_boundary(window.end_byte));
                prop_assert!(window.start_byte <= covered_end);
                if let Some(start) = previous_start {
                    prop_assert!(window.start_byte > start);
                }
                let exact = &text[window.start_byte..window.end_byte];
                prop_assert_eq!(exact.as_bytes(), &text.as_bytes()[window.start_byte..window.end_byte]);
                covered_end = covered_end.max(window.end_byte);
                previous_start = Some(window.start_byte);
            }
            prop_assert_eq!(covered_end, text.len());
        }
    }

    #[test]
    fn long_artifact_search_returns_an_exact_body_span() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let body = format!(
            "{}\n\n# Deep section\n\nThe deepneedle is paired with zebraflux for verification.\n\n{}",
            "ordinary architectural background. ".repeat(2500),
            "unrelated appendix material. ".repeat(1200)
        );
        let inserted = store
            .artifact_put(
                &context("one"),
                &artifact("Very long architecture note", &body),
                Some("long-span"),
                "long-span",
            )
            .unwrap();
        let result = store
            .search_qualified(
                &context("one"),
                &SearchInput {
                    query: "deepneedle zebraflux".into(),
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 10,
                    include_historical: false,
                    min_commit_seq: None,
                    recency: Default::default(),
                },
            )
            .unwrap();
        assert_eq!(result.hits.len(), 1);
        let hit = &result.hits[0];
        assert_eq!(hit.entity_id, inserted.value.artifact_id);
        let (_, span) = hit.citation.rsplit_once('#').unwrap();
        let (start, end) = span.split_once('-').unwrap();
        let start: usize = start.parse().unwrap();
        let end: usize = end.parse().unwrap();
        assert_eq!(hit.excerpt, body[start..end]);
        assert!(hit.excerpt.contains("deepneedle"));
        assert!(end - start <= ARTIFACT_CHUNK_MAX_BYTES);
        assert_eq!(
            hit.provenance["retrieval_span"]["coordinate_space"],
            "immutable_artifact_bytes"
        );
    }

    #[test]
    fn title_match_is_spanless_even_for_a_many_chunk_artifact() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        store
            .artifact_put(
                &context("one"),
                &artifact("ORBITAL-TITLE-742", &"body material. ".repeat(4000)),
                Some("title-spanless"),
                "title-spanless",
            )
            .unwrap();
        let result = store
            .search_qualified(
                &context("one"),
                &SearchInput {
                    query: "ORBITAL-TITLE-742".into(),
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 10,
                    include_historical: false,
                    min_commit_seq: None,
                    recency: Default::default(),
                },
            )
            .unwrap();
        assert_eq!(result.hits.len(), 1);
        assert!(!result.hits[0].citation.contains('#'));
        assert_eq!(result.hits[0].excerpt, "ORBITAL-TITLE-742");
        assert_eq!(result.hits[0].provenance["retrieval_match"], "title");
    }

    #[test]
    fn document_qualification_survives_terms_in_distant_chunks() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let body = format!(
            "alphaunique begins the decision.\n\n{}\n\nomegaunique closes the decision.",
            "middle context without either marker. ".repeat(500)
        );
        store
            .artifact_put(
                &context("one"),
                &artifact("Distributed decision", &body),
                Some("distant-terms"),
                "distant-terms",
            )
            .unwrap();
        let result = store
            .search_qualified(
                &context("one"),
                &SearchInput {
                    query: "alphaunique omegaunique".into(),
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 10,
                    include_historical: false,
                    min_commit_seq: None,
                    recency: Default::default(),
                },
            )
            .unwrap();
        assert_eq!(result.hits.len(), 1);
        assert!(result.hits[0].ranking.qualified);
        assert_eq!(result.hits[0].ranking.matched_unit_count, 2);
        assert!(result.hits[0].citation.contains('#'));
    }

    #[test]
    fn lexical_threshold_is_explicit_for_small_and_long_queries() {
        let expected = [0, 1, 2, 2, 2, 2, 3, 3, 4, 4, 4];
        for (unit_count, required) in expected.into_iter().enumerate() {
            assert_eq!(required_lexical_matches(unit_count), required);
        }
    }

    #[test]
    fn query_analysis_preserves_phrases_identifiers_and_all_stopword_queries() {
        let analyzed = analyze_query("What about APP-BOUNDARY-101 and \"cold cache\"?").unwrap();
        assert_eq!(
            analyzed
                .units
                .iter()
                .map(|unit| (unit.display.as_str(), unit.phrase))
                .collect::<Vec<_>>(),
            vec![("app boundary 101", true), ("cold cache", true)]
        );
        assert_eq!(analyzed.required_matches, 2);
        assert_eq!(analyzed.dropped_stopwords, vec!["about", "and", "what"]);

        let underscore = analyze_query("SCOPED_ALPHA_01").unwrap();
        assert_eq!(underscore.units[0].display, "scoped_alpha_01");
        assert!(!underscore.units[0].phrase);

        let stopwords = analyze_query("what is the").unwrap();
        assert_eq!(stopwords.units.len(), 3);
        assert!(stopwords.dropped_stopwords.is_empty());

        let historical = analyze_query("What did we previously decide?")
            .unwrap()
            .public();
        assert!(matches!(
            historical.retrieval_profile.intent_hint,
            RetrievalIntentHint::HistoricalMemory
        ));
        assert!(matches!(
            historical.retrieval_profile.script_profile,
            QueryScriptProfile::Latin
        ));

        let arabic = analyze_query("ما القرار السابق للنشر؟").unwrap().public();
        assert!(matches!(
            arabic.retrieval_profile.script_profile,
            QueryScriptProfile::Arabic
        ));
        assert_eq!(
            arabic.retrieval_profile.semantic_role,
            "candidate_and_ordering_only"
        );

        let mixed = analyze_query("предыдущее решение release")
            .unwrap()
            .public();
        assert!(matches!(
            mixed.retrieval_profile.script_profile,
            QueryScriptProfile::Mixed
        ));

        let post_migration = analyze_query("Where do events go after the migration?")
            .unwrap()
            .public();
        assert!(matches!(
            post_migration.retrieval_profile.intent_hint,
            RetrievalIntentHint::CurrentSource
        ));
        assert_eq!(
            post_migration.retrieval_profile.authority_hint,
            "repository_authoritative"
        );

        let historical_after_migration =
            analyze_query("What was previously decided after the migration?")
                .unwrap()
                .public();
        assert!(matches!(
            historical_after_migration.retrieval_profile.intent_hint,
            RetrievalIntentHint::HistoricalMemory
        ));
    }

    #[test]
    fn qualified_search_abstains_but_reports_weak_lexical_candidates() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        store
            .artifact_put(
                &context("one"),
                &artifact("Runtime boundary", "The runtime snapshot is versioned."),
                Some("weak-candidate"),
                "weak-candidate",
            )
            .unwrap();
        let input = SearchInput {
            query: "What runtime stores user login records in the database?".into(),
            horizon: Horizon::Ambient,
            reason: None,
            limit: 10,
            include_historical: false,
            min_commit_seq: None,
            recency: Default::default(),
        };
        let broad = store.search(&context("one"), &input).unwrap();
        assert_eq!(broad.hits.len(), 1);
        assert!(!broad.hits[0].ranking.qualified);
        assert_eq!(broad.hits[0].ranking.matched_terms, vec!["runtime"]);

        let qualified = store.search_qualified(&context("one"), &input).unwrap();
        assert!(qualified.hits.is_empty());
        assert_eq!(qualified.unqualified_candidate_count, 1);
        assert_eq!(qualified.best_unqualified_coverage, Some(1.0 / 6.0));
    }

    #[test]
    fn hyphenated_identifier_requires_adjacent_ordered_fts_tokens() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let exact = store
            .artifact_put(
                &context("one"),
                &artifact("Exact", "Decision APP-BOUNDARY-101 is accepted."),
                Some("identifier-exact"),
                "identifier-exact",
            )
            .unwrap();
        store
            .artifact_put(
                &context("one"),
                &artifact(
                    "Scattered",
                    "The app starts here. A boundary appears later. Revision 101 is unrelated.",
                ),
                Some("identifier-scattered"),
                "identifier-scattered",
            )
            .unwrap();
        let result = store
            .search_qualified(
                &context("one"),
                &SearchInput {
                    query: "APP-BOUNDARY-101".into(),
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 10,
                    include_historical: false,
                    min_commit_seq: None,
                    recency: Default::default(),
                },
            )
            .unwrap();
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].entity_id, exact.value.artifact_id);
        assert_eq!(
            result.hits[0].ranking.matched_phrase_groups,
            vec!["app boundary 101"]
        );
    }

    #[test]
    fn trigram_typo_recovery_returns_a_qualified_exact_source_span() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let body = "The browser engine is the authoritative solver and UI forks are prohibited.";
        store
            .artifact_put(
                &context("one"),
                &artifact("Solver boundary", body),
                Some("trigram-typo"),
                "trigram-typo",
            )
            .unwrap();
        let input = SearchInput {
            query: "browsr authorative solvr".into(),
            horizon: Horizon::Ambient,
            reason: None,
            limit: 10,
            include_historical: false,
            min_commit_seq: None,
            recency: Default::default(),
        };
        let first = store.search_qualified(&context("one"), &input).unwrap();
        let second = store.search_qualified(&context("one"), &input).unwrap();
        assert_eq!(first.hits.len(), 1);
        assert!(first.hits[0].ranking.trigram_qualified);
        assert!(!first.hits[0].ranking.lexical_qualified);
        assert!(first.hits[0].citation.contains('#'));
        assert_eq!(first.hits[0].excerpt, body);
        assert_eq!(first.hits[0].entity_id, second.hits[0].entity_id);
        assert_eq!(first.hits[0].citation, second.hits[0].citation);
        assert_eq!(
            first.hits[0].ranking.trigram_matched_terms,
            second.hits[0].ranking.trigram_matched_terms
        );
        assert_eq!(
            first.hits[0].ranking.fusion_score,
            second.hits[0].ranking.fusion_score
        );
    }

    #[test]
    fn partial_lexical_match_does_not_starve_typo_candidates() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let relevant = store
            .artifact_put(
                &context("one"),
                &artifact(
                    "Authority policy",
                    "The browser follows the authoritative deployment policy.",
                ),
                Some("mixed-relevant"),
                "mixed-relevant",
            )
            .unwrap();
        store
            .artifact_put(
                &context("one"),
                &artifact(
                    "Browser settings",
                    "The browser setting controls dashboard colors.",
                ),
                Some("mixed-distractor"),
                "mixed-distractor",
            )
            .unwrap();
        let result = store
            .search_qualified(
                &context("one"),
                &SearchInput {
                    query: "browser setting authortative".into(),
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 10,
                    include_historical: false,
                    min_commit_seq: None,
                    recency: Default::default(),
                },
            )
            .unwrap();
        let recovered = result
            .hits
            .iter()
            .find(|hit| hit.entity_id == relevant.value.artifact_id)
            .expect("the typo-supported candidate must not be starved");
        assert!(recovered.ranking.trigram_qualified);
    }

    #[test]
    fn verification_detects_same_count_trigram_content_drift() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let inserted = store
            .artifact_put(
                &context("one"),
                &artifact("Projection source", "authoritative source body"),
                Some("trigram-drift"),
                "trigram-drift",
            )
            .unwrap();
        {
            let connection = store.connection.lock();
            connection
                .execute(
                    "DELETE FROM artifact_trigram_fts WHERE revision_id = ?1",
                    [&inserted.value.revision_id],
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO artifact_trigram_fts(revision_id, artifact_id, title, body)
                     VALUES (?1, ?2, 'Projection source', 'drifted body')",
                    params![inserted.value.revision_id, inserted.value.artifact_id],
                )
                .unwrap();
        }
        let report = store.verify().unwrap();
        assert!(!report.ok);
        assert!(
            report
                .issues
                .iter()
                .any(|issue| issue.contains("trigram rows differ"))
        );
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

    #[test]
    fn source_projection_candidates_are_cited_but_never_qualify() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let ambient = context("projection-contract");
        let source = store
            .source_register(
                &ambient,
                &SourceRegisterInput {
                    name: "Test knowledge base".into(),
                    kind: "test_adapter".into(),
                    locator: Some("test://knowledge".into()),
                    metadata: BTreeMap::new(),
                    actor: Some("test".into()),
                },
                Some("source-register"),
                "source-register",
            )
            .unwrap();
        let raw = "Falcon processors use wafer-scale execution for inference.";
        let ingest_input = SourceIngestInput {
            source_id: source.value.source_id.clone(),
            external_id: "document-1".into(),
            external_revision: "etag-1".into(),
            kind: "source_document".into(),
            title: "Falcon architecture".into(),
            media_type: "text/plain; charset=utf-8".into(),
            content: ArtifactContent::Text(raw.into()),
            provenance: BTreeMap::new(),
            observed_at: None,
            cursor: Some("cursor-1".into()),
            actor: Some("test".into()),
        };
        let ingested = store
            .source_ingest(
                &ambient,
                &ingest_input,
                Some("source-ingest"),
                "source-ingest",
            )
            .unwrap();
        let replay = store
            .source_ingest(
                &ambient,
                &ingest_input,
                Some("source-ingest-replay"),
                "source-ingest-replay",
            )
            .unwrap();
        assert!(!replay.created);
        assert_eq!(replay.commit_seq, ingested.commit_seq);

        store
            .projection_put(
                &ambient,
                &ProjectionPutInput {
                    artifact_id: ingested.value.artifact.artifact_id.clone(),
                    revision_id: ingested.value.artifact.revision_id.clone(),
                    projection_key: "summary-1".into(),
                    kind: "summary".into(),
                    text: "A supercalifragilistic retrieval accelerator".into(),
                    evidence_spans: vec![ProjectionSpan {
                        start_byte: 0,
                        end_byte: 6,
                    }],
                    generator: "test".into(),
                    generator_version: "1".into(),
                    generator_digest: "blake3:test".into(),
                    metadata: BTreeMap::new(),
                    actor: Some("test".into()),
                },
                Some("projection-put"),
                "projection-put",
            )
            .unwrap();

        let search = SearchInput {
            query: "supercalifragilistic".into(),
            horizon: Horizon::Ambient,
            reason: None,
            limit: 10,
            include_historical: false,
            min_commit_seq: None,
            recency: Default::default(),
        };
        let broad = store.search(&ambient, &search).unwrap();
        assert_eq!(broad.projection.state, "ready");
        assert_eq!(broad.projection.candidate_count, 1);
        assert_eq!(broad.hits.len(), 1);
        assert!(!broad.hits[0].ranking.qualified);
        assert_eq!(broad.hits[0].excerpt, "Falcon");
        assert!(broad.hits[0].citation.ends_with("#0-6"));
        assert_eq!(
            broad.hits[0].provenance["derived_projection"]["candidate_only"],
            true
        );
        assert!(
            store
                .search_qualified(&ambient, &search)
                .unwrap()
                .hits
                .is_empty()
        );

        let withdrawn = store
            .source_withdraw(
                &ambient,
                &SourceWithdrawInput {
                    source_id: source.value.source_id,
                    external_id: "document-1".into(),
                    reason: "removed upstream".into(),
                    observed_at: None,
                    actor: Some("test".into()),
                },
                Some("source-withdraw"),
                "source-withdraw",
            )
            .unwrap();
        assert!(!withdrawn.value.erasure_performed);
        assert!(store.search(&ambient, &search).unwrap().hits.is_empty());
    }

    #[test]
    fn corrupt_candidate_projection_fails_open_without_partial_ranking() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let ambient = context("projection-fail-open");
        let inserted = store
            .artifact_put(
                &ambient,
                &artifact("Authority", "authoritative source evidence"),
                Some("projection-fail-open-artifact"),
                "projection-fail-open-artifact",
            )
            .unwrap();
        let projection = store
            .projection_put(
                &ambient,
                &ProjectionPutInput {
                    artifact_id: inserted.value.artifact_id.clone(),
                    revision_id: inserted.value.revision_id.clone(),
                    projection_key: "summary".into(),
                    kind: "summary".into(),
                    text: "authoritative".into(),
                    evidence_spans: vec![ProjectionSpan {
                        start_byte: 0,
                        end_byte: 13,
                    }],
                    generator: "test".into(),
                    generator_version: "1".into(),
                    generator_digest: "blake3:test".into(),
                    metadata: BTreeMap::new(),
                    actor: Some("test".into()),
                },
                Some("projection-fail-open-put"),
                "projection-fail-open-put",
            )
            .unwrap();
        {
            let connection = store.connection.lock();
            connection
                .execute(
                    "UPDATE retrieval_projections SET evidence_json = '[]' WHERE id = ?1",
                    [&projection.value.projection_id],
                )
                .unwrap();
        }
        let result = store
            .search_qualified(
                &ambient,
                &SearchInput {
                    query: "authoritative".into(),
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 10,
                    include_historical: false,
                    min_commit_seq: None,
                    recency: Default::default(),
                },
            )
            .unwrap();
        assert_eq!(result.projection.state, "error");
        assert_eq!(result.projection.candidate_count, 0);
        assert!(
            result
                .projection
                .reason
                .as_deref()
                .is_some_and(|reason| reason.contains("failed open"))
        );
        assert_eq!(result.hits.len(), 1);
        assert_eq!(result.hits[0].entity_id, inserted.value.artifact_id);
        assert!(
            !result.hits[0]
                .matched_by
                .iter()
                .any(|channel| channel == PROJECTION_POLICY_VERSION)
        );
    }

    #[test]
    fn feedback_uses_a_keyed_fingerprint_unless_query_retention_is_explicit() {
        let temporary = tempfile::tempdir().unwrap();
        let store = Store::open(temporary.path()).unwrap();
        let ambient = context("feedback-privacy");
        let private = store
            .feedback_record(
                &ambient,
                &FeedbackRecordInput {
                    query: "sensitive customer retrieval".into(),
                    outcome: FeedbackOutcome::Miss,
                    retain_query: false,
                    citations: vec![],
                    note: None,
                    actor: Some("test".into()),
                },
                Some("feedback-private"),
                "feedback-private",
            )
            .unwrap();
        assert!(private.value.retained_query.is_none());
        assert_ne!(
            private.value.query_fingerprint,
            "sensitive customer retrieval"
        );
        assert_eq!(private.value.query_fingerprint.len(), 64);

        let retained = store
            .feedback_record(
                &ambient,
                &FeedbackRecordInput {
                    query: "explicit evaluation query".into(),
                    outcome: FeedbackOutcome::Useful,
                    retain_query: true,
                    citations: vec!["memoree://artifact/example@revision#0-4".into()],
                    note: Some("approved for offline evaluation".into()),
                    actor: Some("test".into()),
                },
                Some("feedback-retained"),
                "feedback-retained",
            )
            .unwrap();
        assert_eq!(
            retained.value.retained_query.as_deref(),
            Some("explicit evaluation query")
        );
        let listed = store
            .feedback_list(
                &ambient,
                &FeedbackListInput {
                    limit: 10,
                    before_commit_seq: None,
                },
            )
            .unwrap();
        assert_eq!(listed.feedback.len(), 2);
        let exported = store
            .feedback_export(
                &ambient,
                &FeedbackExportInput {
                    limit: 10,
                    before_commit_seq: None,
                },
            )
            .unwrap();
        assert_eq!(exported.format, "memoree_retrieval_feedback_v1");
        assert_eq!(exported.cases.len(), 1);
        assert_eq!(exported.cases[0].query, "explicit evaluation query");
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
    fn schema_v3_backfills_only_exact_artifact_byte_partitions() {
        let temporary = tempfile::tempdir().unwrap();
        {
            let store = Store::open(temporary.path()).unwrap();
            store
                .artifact_put(
                    &context("migration-v3"),
                    &artifact(
                        "Migration artifact",
                        &format!("{} migrationneedle", "context. ".repeat(1000)),
                    ),
                    Some("migration-v3-artifact"),
                    "migration-v3-artifact",
                )
                .unwrap();
            let connection = store.connection.lock();
            connection
                .execute("DELETE FROM artifact_chunk_fts", [])
                .unwrap();
            connection
                .execute(
                    "UPDATE meta SET value = '3' WHERE key = 'schema_version'",
                    [],
                )
                .unwrap();
        }
        let migrated = Store::open(temporary.path()).unwrap();
        let migration = migrated
            .schema_migration()
            .expect("schema 3 migration must publish a recovery snapshot");
        assert_eq!(migration.from_schema, 3);
        assert_eq!(migration.to_schema, SCHEMA_VERSION);
        let backup = PathBuf::from(&migration.backup_destination);
        assert!(backup.join(MEMOREE_DATABASE_FILE).is_file());
        assert!(backup.join("migration.json").is_file());
        let backup_connection = Connection::open(backup.join(MEMOREE_DATABASE_FILE)).unwrap();
        let backup_schema: i64 = backup_connection
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            backup_schema, 3,
            "recovery snapshot must remain pre-migration"
        );
        assert_eq!(migrated.verify().unwrap().schema_version, SCHEMA_VERSION);
        assert!(migrated.verify().unwrap().ok);
        let result = migrated
            .search_qualified(
                &context("migration-v3"),
                &SearchInput {
                    query: "migrationneedle".into(),
                    horizon: Horizon::Ambient,
                    reason: None,
                    limit: 10,
                    include_historical: false,
                    min_commit_seq: None,
                    recency: Default::default(),
                },
            )
            .unwrap();
        assert_eq!(result.hits.len(), 1);
        assert!(result.hits[0].citation.contains('#'));
    }

    #[test]
    fn schema_v4_migrates_source_projection_and_feedback_contract_with_backup() {
        let temporary = tempfile::tempdir().unwrap();
        {
            let store = Store::open(temporary.path()).unwrap();
            store
                .artifact_put(
                    &context("migration-v4"),
                    &artifact("v4 authority", "existing schema four authority survives"),
                    Some("migration-v4-artifact"),
                    "migration-v4-artifact",
                )
                .unwrap();
            let connection = store.connection.lock();
            connection
                .execute_batch(
                    "DROP TABLE projection_fts;
                 DROP TABLE retrieval_feedback;
                 DROP TABLE retrieval_projections;
                 DROP TABLE source_items;
                 DROP TABLE sources;
                 UPDATE meta SET value = '4' WHERE key = 'schema_version';",
                )
                .unwrap();
        }

        let migrated = Store::open(temporary.path()).unwrap();
        let migration = migrated
            .schema_migration()
            .expect("schema 4 migration must publish a recovery snapshot");
        assert_eq!(migration.from_schema, 4);
        assert_eq!(migration.to_schema, SCHEMA_VERSION);
        let backup = PathBuf::from(&migration.backup_destination);
        let backup_connection = Connection::open(backup.join(MEMOREE_DATABASE_FILE)).unwrap();
        let backup_schema: i64 = backup_connection
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(backup_schema, 4);
        assert_eq!(migrated.schema_version().unwrap(), SCHEMA_VERSION);
        assert!(migrated.verify().unwrap().ok);
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
        assert_eq!(schema_version, SCHEMA_VERSION);
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
        assert_eq!(schema_version, SCHEMA_VERSION);
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
