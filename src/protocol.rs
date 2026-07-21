use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;

pub const PROTOCOL_VERSION: u32 = 1;
/// Maximum encoded request or response frame accepted by the daemon.
pub const MAX_FRAME_BYTES: usize = 24 * 1024 * 1024;
/// Maximum raw artifact payload. Base64 plus bounded metadata remains below
/// `MAX_FRAME_BYTES`.
pub const MAX_ARTIFACT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_ENCODED_CONTENT_BYTES: usize = 12 * 1024 * 1024;
pub const MAX_REQUEST_ID_BYTES: usize = 256;
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 512;
pub const MAX_CONTEXT_ID_BYTES: usize = 256;
pub const MAX_CONTEXT_PINS: usize = 128;
pub const MAX_PIN_BYTES: usize = 512;
pub const MAX_QUERY_BYTES: usize = 8 * 1024;
pub const MAX_TITLE_BYTES: usize = 4 * 1024;
pub const MAX_CLAIM_STATEMENT_BYTES: usize = 128 * 1024;
pub const MAX_METADATA_BYTES: usize = 64 * 1024;
pub const MAX_EVIDENCE_ITEMS: usize = 128;
pub const MAX_SEARCH_ITEMS: usize = 100;
pub const MAX_RECALL_CANDIDATE_CLAIMS: usize = 16;
pub const MAX_RECALL_CANDIDATE_ARTIFACT_REFS: usize = 16;
pub const MAX_RECALL_CLAIMS: usize = 10;
pub const MAX_RECALL_ARTIFACT_REFS: usize = 20;
pub const MAX_RECALL_EXCERPT_BYTES: usize = 1024;
pub const MAX_RECALL_EVIDENCE_EXCERPTS_PER_CLAIM: usize = 4;
pub const MAX_PROBE_ITEMS: usize = 8;
pub const MAX_PROBE_TITLE_BYTES: usize = 48;
pub const MAX_PROBE_SOURCES_PER_LEAD: usize = 3;
pub const MAX_PROBE_EVIDENCE_BYTES_PER_LEAD: usize = 4 * 1024;
pub const MAX_CITATION_BYTES: usize = 1024;
pub const MAX_CITATION_FETCH_BYTES: usize = 8 * 1024;
pub const MAX_HISTORY_ITEMS: usize = 100;
pub const MAX_RELATION_LIST_ITEMS: usize = 100;
pub const MAX_CONFLICT_LIST_ITEMS: usize = 100;
pub const MAX_SOURCE_ITEMS: usize = 100;
pub const MAX_PROJECTION_ITEMS: usize = 100;
pub const MAX_FEEDBACK_ITEMS: usize = 100;
pub const MAX_EXTERNAL_ID_BYTES: usize = 4 * 1024;
pub const MAX_SOURCE_CURSOR_BYTES: usize = 64 * 1024;
pub const MAX_PROJECTION_TEXT_BYTES: usize = 64 * 1024;
pub const MAX_PROJECTION_SPANS: usize = 64;
pub const MAX_FEEDBACK_NOTE_BYTES: usize = 64 * 1024;

const _: () = assert!(MAX_ENCODED_CONTENT_BYTES + MAX_METADATA_BYTES < MAX_FRAME_BYTES);

fn default_version() -> u32 {
    PROTOCOL_VERSION
}

fn default_request_id() -> String {
    format!("req_{}", Ulid::r#gen())
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Request {
    #[serde(default = "default_version")]
    pub v: u32,
    pub request_id: String,
    pub op: Operation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<AmbientContext>,
    #[serde(default)]
    pub context_source: ContextSource,
    #[serde(default)]
    pub input: Value,
}

impl Request {
    pub fn new(op: Operation, input: impl Serialize) -> serde_json::Result<Self> {
        Ok(Self {
            v: PROTOCOL_VERSION,
            request_id: default_request_id(),
            op,
            idempotency_key: None,
            context: None,
            context_source: ContextSource::None,
            input: serde_json::to_value(input)?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum Operation {
    #[serde(rename = "context.resolve")]
    ContextResolve,
    #[serde(rename = "capabilities")]
    Capabilities,
    #[serde(rename = "instructions")]
    Instructions,
    #[serde(rename = "schema")]
    Schema,
    #[serde(rename = "artifact.put")]
    ArtifactPut,
    #[serde(rename = "artifact.get")]
    ArtifactGet,
    #[serde(rename = "citation.get")]
    CitationGet,
    #[serde(rename = "artifact.revise")]
    ArtifactRevise,
    #[serde(rename = "artifact.history")]
    ArtifactHistory,
    #[serde(rename = "artifact.forget")]
    ArtifactForget,
    #[serde(rename = "claim.assert")]
    ClaimAssert,
    #[serde(rename = "claim.get")]
    ClaimGet,
    #[serde(rename = "claim.history")]
    ClaimHistory,
    #[serde(rename = "claim.revise")]
    ClaimRevise,
    #[serde(rename = "claim.retract")]
    ClaimRetract,
    #[serde(rename = "relation.put")]
    RelationPut,
    #[serde(rename = "relation.list")]
    RelationList,
    #[serde(rename = "source.register")]
    SourceRegister,
    #[serde(rename = "source.get")]
    SourceGet,
    #[serde(rename = "source.ingest")]
    SourceIngest,
    #[serde(rename = "source.checkpoint")]
    SourceCheckpoint,
    #[serde(rename = "source.withdraw")]
    SourceWithdraw,
    #[serde(rename = "projection.put")]
    ProjectionPut,
    #[serde(rename = "projection.list")]
    ProjectionList,
    #[serde(rename = "projection.drop")]
    ProjectionDrop,
    #[serde(rename = "feedback.record")]
    FeedbackRecord,
    #[serde(rename = "feedback.get")]
    FeedbackGet,
    #[serde(rename = "feedback.list")]
    FeedbackList,
    #[serde(rename = "feedback.export")]
    FeedbackExport,
    #[serde(rename = "conflict.list")]
    ConflictList,
    #[serde(rename = "search")]
    Search,
    #[serde(rename = "memory.recall")]
    MemoryRecall,
    #[serde(rename = "memory.probe")]
    MemoryProbe,
    #[serde(rename = "context.build")]
    ContextBuild,
    #[serde(rename = "doctor")]
    Doctor,
    #[serde(rename = "verify")]
    Verify,
    #[serde(rename = "backup.create")]
    BackupCreate,
}

impl Operation {
    pub fn is_mutating(self) -> bool {
        matches!(
            self,
            Self::ArtifactPut
                | Self::ArtifactRevise
                | Self::ArtifactForget
                | Self::ClaimAssert
                | Self::ClaimRevise
                | Self::ClaimRetract
                | Self::RelationPut
                | Self::SourceRegister
                | Self::SourceIngest
                | Self::SourceCheckpoint
                | Self::SourceWithdraw
                | Self::ProjectionPut
                | Self::ProjectionDrop
                | Self::FeedbackRecord
        )
    }

    /// Operations that can change durable state outside the response stream.
    /// Logical memory mutations additionally require an idempotency key;
    /// backup.create is an atomic administrative filesystem side effect.
    pub fn has_side_effects(self) -> bool {
        self.is_mutating() || matches!(self, Self::BackupCreate)
    }

    pub fn needs_context(self) -> bool {
        matches!(
            self,
            Self::ContextResolve
                | Self::ArtifactPut
                | Self::ArtifactRevise
                | Self::ArtifactForget
                | Self::ClaimAssert
                | Self::ClaimRevise
                | Self::ClaimRetract
                | Self::RelationPut
                | Self::RelationList
                | Self::SourceRegister
                | Self::SourceGet
                | Self::SourceIngest
                | Self::SourceCheckpoint
                | Self::SourceWithdraw
                | Self::ProjectionPut
                | Self::ProjectionList
                | Self::ProjectionDrop
                | Self::FeedbackRecord
                | Self::FeedbackGet
                | Self::FeedbackList
                | Self::FeedbackExport
                | Self::ConflictList
                | Self::Search
                | Self::MemoryRecall
                | Self::MemoryProbe
                | Self::ContextBuild
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ContextSource {
    Explicit,
    Session,
    Marker,
    Personal,
    #[default]
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AmbientContext {
    pub workspace_id: String,
    pub project_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub component: Option<String>,
    #[serde(default)]
    pub pins: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Horizon {
    #[default]
    Ambient,
    Workspace,
    Personal,
}

impl Horizon {
    pub fn broadened(self) -> bool {
        !matches!(self, Self::Ambient)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ResolvedContext {
    #[serde(flatten)]
    pub ambient: AmbientContext,
    pub resolved_from: ContextSource,
    pub horizon: Horizon,
    pub broadened: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ContextResolveResult {
    pub resolved: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<ResolvedContext>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DoctorResult {
    pub status: String,
    pub running: bool,
    pub daemon_pid: u32,
    #[serde(default)]
    pub binary_version: String,
    #[serde(default)]
    pub schema_version: i64,
    #[serde(default)]
    pub lifecycle_owner: String,
    pub authoritative_store: String,
    pub retrieval_mode: String,
    pub last_commit_seq: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Response {
    pub v: u32,
    pub request_id: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<ResolvedContext>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorBody>,
    #[serde(default)]
    pub warnings: Vec<Warning>,
}

impl Response {
    pub fn success(request: &Request, result: impl Serialize) -> serde_json::Result<Self> {
        Ok(Self {
            v: PROTOCOL_VERSION,
            request_id: request.request_id.clone(),
            ok: true,
            context: None,
            commit_seq: None,
            result: Some(serde_json::to_value(result)?),
            error: None,
            warnings: vec![],
        })
    }

    pub fn failure(request_id: impl Into<String>, error: &crate::error::MemoryError) -> Self {
        Self {
            v: PROTOCOL_VERSION,
            request_id: request_id.into(),
            ok: false,
            context: None,
            commit_seq: None,
            result: None,
            error: Some(ErrorBody {
                code: error.code(),
                message: error.to_string(),
                retryable: error.retryable(),
                hint: error_hint(error),
                details: error_details(error),
            }),
            warnings: vec![],
        }
    }
}

fn error_hint(error: &crate::error::MemoryError) -> Option<String> {
    match error {
        crate::error::MemoryError::NoAmbientContext => Some(
            "Run `memoree init` in the project, or use `memoree session exec` from an initialized project."
                .to_owned(),
        ),
        crate::error::MemoryError::RevisionConflict { .. } => {
            Some("Fetch the current revision and retry with its revision id.".to_owned())
        }
        crate::error::MemoryError::IdempotencyConflict(_) => Some(
            "Use the same key only for an exact retry, or choose a new idempotency key.".to_owned(),
        ),
        crate::error::MemoryError::IndexNotReady { .. } => Some(
            "Retry the read with the same min_commit_seq after the acknowledged write is visible."
                .to_owned(),
        ),
        crate::error::MemoryError::ScopeViolation(_) => Some(
            "Resolve the ambient context that owns the entity before mutating it; pinned and exact-looked-up entities remain read-only outside their owner context."
                .to_owned(),
        ),
        crate::error::MemoryError::Reasoner { .. } => Some(
            "Run `memoree compiler status`, authenticate with `codex login` or `claude auth login`, and choose with `memoree compiler configure`. API-key fallback remains Codex-only and requires explicit permission for `memoree remember --allow-api-key`; use `--raw` only when preserving without claims is intended."
                .to_owned(),
        ),
        _ => None,
    }
}

fn error_details(error: &crate::error::MemoryError) -> Value {
    match error {
        crate::error::MemoryError::RevisionConflict {
            entity_type,
            entity_id,
            current_revision,
            requested_revision,
        } => serde_json::json!({
            "entity_type": entity_type,
            "entity_id": entity_id,
            "current_revision": current_revision,
            "requested_revision": requested_revision,
        }),
        crate::error::MemoryError::IdempotencyConflict(key) => {
            serde_json::json!({ "idempotency_key": key })
        }
        crate::error::MemoryError::IndexNotReady { requested, current } => serde_json::json!({
            "requested_commit_seq": requested,
            "current_commit_seq": current,
        }),
        crate::error::MemoryError::UnsupportedVersion(received) => serde_json::json!({
            "received_version": received,
            "supported_version": PROTOCOL_VERSION,
        }),
        crate::error::MemoryError::Citation { kind, details, .. } => {
            let mut details = details.clone();
            if !details.is_object() {
                details = serde_json::json!({});
            }
            if let Some(object) = details.as_object_mut() {
                object.insert("citation_error".into(), Value::String((*kind).into()));
            }
            details
        }
        _ => Value::Null,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ErrorBody {
    pub code: ErrorCode,
    pub message: String,
    pub retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
    #[serde(default)]
    pub details: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    NoAmbientContext,
    InvalidRequest,
    NotFound,
    CitationError,
    RevisionConflict,
    IdempotencyConflict,
    IndexNotReady,
    ScopeViolation,
    ConfigError,
    ContentTooLarge,
    IntegrityError,
    UnsupportedVersion,
    TransportError,
    ReasonerError,
    InternalError,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Warning {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(
    tag = "type",
    content = "data",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum ArtifactContent {
    Text(String),
    Base64(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArtifactPutInput {
    pub kind: String,
    pub title: String,
    #[serde(default = "default_text_media_type")]
    pub media_type: String,
    pub content: ArtifactContent,
    #[serde(default)]
    pub provenance: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

fn default_text_media_type() -> String {
    "text/plain; charset=utf-8".to_owned()
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArtifactReviseInput {
    pub artifact_id: String,
    pub if_revision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    pub content: ArtifactContent,
    #[serde(default)]
    pub provenance: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArtifactGetInput {
    pub artifact_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
    #[serde(default = "default_true")]
    pub include_content: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CitationGetInput {
    /// Immutable Memoree artifact citation with an exact UTF-8 byte range.
    pub citation: String,
    /// Caller-selected output ceiling. Oversized ranges are narrowed exactly.
    #[serde(default = "default_citation_fetch_bytes")]
    pub max_bytes: usize,
}

fn default_citation_fetch_bytes() -> usize {
    MAX_CITATION_FETCH_BYTES
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArtifactHistoryInput {
    pub artifact_id: String,
    #[serde(default = "default_history_limit")]
    pub limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_revision_number: Option<i64>,
}

fn default_history_limit() -> usize {
    50
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArtifactForgetInput {
    pub artifact_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SourceHealth {
    Unknown,
    Healthy,
    Degraded,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceRegisterInput {
    pub name: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub locator: Option<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceGetInput {
    pub source_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceIngestInput {
    pub source_id: String,
    pub external_id: String,
    pub external_revision: String,
    pub kind: String,
    pub title: String,
    #[serde(default = "default_text_media_type")]
    pub media_type: String,
    pub content: ArtifactContent,
    #[serde(default)]
    pub provenance: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceCheckpointInput {
    pub source_id: String,
    pub health: SourceHealth,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SourceWithdrawInput {
    pub source_id: String,
    pub external_id: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProjectionSpan {
    pub start_byte: u64,
    pub end_byte: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProjectionPutInput {
    pub artifact_id: String,
    pub revision_id: String,
    /// Stable adapter-owned identity within one immutable artifact revision.
    pub projection_key: String,
    pub kind: String,
    pub text: String,
    pub evidence_spans: Vec<ProjectionSpan>,
    pub generator: String,
    pub generator_version: String,
    pub generator_digest: String,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProjectionListInput {
    pub artifact_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
    #[serde(default = "default_projection_list_limit")]
    pub limit: usize,
}

fn default_projection_list_limit() -> usize {
    MAX_PROJECTION_ITEMS
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProjectionDropInput {
    pub projection_id: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackOutcome {
    Miss,
    Useful,
    Incorrect,
    Stale,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FeedbackRecordInput {
    pub query: String,
    pub outcome: FeedbackOutcome,
    #[serde(default)]
    pub retain_query: bool,
    #[serde(default)]
    pub citations: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FeedbackGetInput {
    pub feedback_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FeedbackListInput {
    #[serde(default = "default_feedback_list_limit")]
    pub limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_commit_seq: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FeedbackExportInput {
    #[serde(default = "default_feedback_list_limit")]
    pub limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_commit_seq: Option<i64>,
}

fn default_feedback_list_limit() -> usize {
    MAX_FEEDBACK_ITEMS
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EvidenceLocator {
    pub artifact_id: String,
    pub revision_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_byte: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_byte: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClaimAssertInput {
    pub claim_type: ClaimType,
    pub statement: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    #[serde(default)]
    pub evidence: Vec<EvidenceLocator>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_from: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_until: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ClaimType {
    Fact,
    Decision,
    Constraint,
    Preference,
    Procedure,
    Observation,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ClaimStatus {
    Active,
    Superseded,
    Retracted,
    Conflicted,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClaimGetInput {
    pub claim_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClaimHistoryInput {
    pub claim_id: String,
    #[serde(default = "default_history_limit")]
    pub limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_revision_number: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClaimReviseInput {
    pub claim_id: String,
    pub if_revision: String,
    pub statement: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    #[serde(default)]
    pub evidence: Vec<EvidenceLocator>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClaimRetractInput {
    pub claim_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EntityType {
    Artifact,
    Claim,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RelationType {
    DerivedFrom,
    Supports,
    Contradicts,
    Supersedes,
    References,
    Duplicates,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RelationPutInput {
    pub source_type: EntityType,
    pub source_id: String,
    pub relation: RelationType,
    pub target_type: EntityType,
    pub target_id: String,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RelationDirection {
    Incoming,
    Outgoing,
    #[default]
    Both,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RelationListInput {
    pub entity_type: EntityType,
    pub entity_id: String,
    #[serde(default)]
    pub direction: RelationDirection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relation: Option<RelationType>,
    #[serde(default)]
    pub horizon: Horizon,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default = "default_relation_list_limit")]
    pub limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_commit_seq: Option<i64>,
}

fn default_relation_list_limit() -> usize {
    MAX_RELATION_LIST_ITEMS
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RelationListItem {
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
    pub relation_commit_seq: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RelationListResult {
    pub entity_type: EntityType,
    pub entity_id: String,
    pub direction: RelationDirection,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relation: Option<RelationType>,
    pub horizon: Horizon,
    pub relations: Vec<RelationListItem>,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_before_commit_seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broaden_hint: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConflictState {
    Open,
    Stale,
    Resolved,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConflictListInput {
    #[serde(default)]
    pub horizon: Horizon,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Include cases made stale by a claim revision. Resolved cases are
    /// terminal audit history and are not actionable list results.
    #[serde(default)]
    pub include_stale: bool,
    #[serde(default = "default_conflict_list_limit")]
    pub limit: usize,
    /// Exclusive cursor over the durable conflict-case creation order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_case_sequence: Option<i64>,
}

fn default_conflict_list_limit() -> usize {
    MAX_CONFLICT_LIST_ITEMS
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchInput {
    pub query: String,
    #[serde(default)]
    pub horizon: Horizon,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default = "default_search_limit")]
    pub limit: usize,
    #[serde(default)]
    pub include_historical: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_commit_seq: Option<i64>,
    /// Apply the bounded, deterministic recency policy after lexical top-K
    /// membership has been selected. The default keeps recency enabled while
    /// preserving ambient scope and every lexical candidate.
    #[serde(default)]
    pub recency: RecencyBiasInput,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecallInput {
    pub query: String,
    #[serde(default)]
    pub horizon: Horizon,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default = "default_recall_claims")]
    pub max_claims: usize,
    #[serde(default = "default_recall_artifact_refs")]
    pub max_artifact_refs: usize,
    #[serde(default = "default_recall_excerpt_bytes")]
    pub max_excerpt_bytes: usize,
    /// Unqualified retrieval suggestions to expose separately from claims.
    /// Zero disables candidate claim suggestions.
    #[serde(default = "default_recall_candidate_claims")]
    pub max_candidate_claims: usize,
    /// Unqualified retrieval suggestions to expose separately from artifact
    /// references. Zero disables candidate artifact suggestions.
    #[serde(default = "default_recall_candidate_artifact_refs")]
    pub max_candidate_artifact_refs: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_commit_seq: Option<i64>,
    #[serde(default)]
    pub recency: RecencyBiasInput,
}

fn default_recall_claims() -> usize {
    5
}

fn default_recall_artifact_refs() -> usize {
    3
}

fn default_recall_excerpt_bytes() -> usize {
    320
}

fn default_recall_candidate_claims() -> usize {
    0
}

fn default_recall_candidate_artifact_refs() -> usize {
    0
}

fn default_search_limit() -> usize {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecencyBiasInput {
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl Default for RecencyBiasInput {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ContextBuildInput {
    #[serde(flatten)]
    pub search: SearchInput,
    #[serde(default = "default_context_bytes")]
    pub max_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProbeInput {
    /// The single query used for candidate routing. When reformulated, the
    /// caller also supplies original_query for an auditable relevance check.
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub original_query: Option<String>,
    #[serde(default)]
    pub horizon: Horizon,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default = "default_probe_items")]
    pub max_leads: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_commit_seq: Option<i64>,
    #[serde(default)]
    pub recency: RecencyBiasInput,
}

fn default_probe_items() -> usize {
    8
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BackupCreateInput {
    pub destination: String,
}

fn default_context_bytes() -> usize {
    16 * 1024
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SearchHit {
    pub entity_type: EntityType,
    pub entity_id: String,
    pub revision_id: String,
    pub status: String,
    pub title: String,
    pub excerpt: String,
    pub citation: String,
    pub context: AmbientContext,
    /// Final ranking score after the bounded recency policy. See `ranking` for
    /// the original lexical score and every applied component.
    pub score: f64,
    pub ranking: SearchRanking,
    pub matched_by: Vec<String>,
    #[serde(default)]
    pub provenance: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SearchRanking {
    pub policy_version: String,
    pub lexical_policy_version: String,
    pub trigram_policy_version: String,
    pub fusion_policy_version: String,
    pub query_unit_count: usize,
    pub matched_unit_count: usize,
    pub required_matches: usize,
    pub lexical_coverage: f64,
    pub phrase_group_count: usize,
    pub matched_phrase_group_count: usize,
    pub lexical_qualified: bool,
    pub trigram_qualified: bool,
    pub semantic_qualified: bool,
    pub qualified: bool,
    #[serde(default)]
    pub matched_terms: Vec<String>,
    #[serde(default)]
    pub matched_phrase_groups: Vec<String>,
    #[serde(default)]
    pub trigram_matched_terms: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigram_similarity: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub semantic_similarity: Option<f64>,
    pub exact_tier: bool,
    pub fusion_score: f64,
    pub recency_enabled: bool,
    pub recency_eligible: bool,
    pub lexical_score: f64,
    pub recency_bonus: f64,
    pub lexical_position: usize,
    pub final_position: usize,
    pub max_promotion: usize,
    pub effective_at: DateTime<Utc>,
    pub effective_at_basis: RecencyTimestampBasis,
    pub evaluated_at: DateTime<Utc>,
    pub decay_class: RecencyDecayClass,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct QueryAnalysis {
    pub policy_version: String,
    pub normalized_query: String,
    #[serde(default)]
    pub content_units: Vec<String>,
    #[serde(default)]
    pub phrase_groups: Vec<String>,
    #[serde(default)]
    pub dropped_stopwords: Vec<String>,
    pub required_matches: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RecencyTimestampBasis {
    RevisionCreatedAt,
    ValidFrom,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RecencyDecayClass {
    Ephemeral,
    Observation,
    General,
    Preference,
    Procedure,
    Decision,
    Constraint,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SemanticRetrievalStatus {
    pub state: String,
    pub policy_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_revision: Option<String>,
    pub indexed_commit_seq: i64,
    pub current_commit_seq: i64,
    pub eligible_revision_count: usize,
    pub indexed_revision_count: usize,
    pub coverage: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RerankerCircuitBreakerStatus {
    pub state: String,
    pub budget_ms: f64,
    pub trip_threshold: usize,
    pub consecutive_over_budget: usize,
    pub probe_after_skips: usize,
    pub skipped_since_open: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RerankerRetrievalStatus {
    pub state: String,
    pub policy_version: String,
    pub role: String,
    /// Retrieval surface governed by this status: `claim`, `artifact`, or
    /// `control_plane` for a status-only request.
    pub surface: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_revision: Option<String>,
    pub candidate_count: usize,
    pub scored_candidate_count: usize,
    pub ordering_applied: bool,
    pub candidate_limit: usize,
    pub candidate_limit_reached: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inference_latency_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_load_latency_ms: Option<f64>,
    pub breaker: RerankerCircuitBreakerStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProjectionRetrievalStatus {
    pub state: String,
    pub policy_version: String,
    pub candidate_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SearchResult {
    pub query: String,
    pub query_analysis: QueryAnalysis,
    pub horizon: Horizon,
    pub retrieval_mode: String,
    pub projection: ProjectionRetrievalStatus,
    pub semantic: SemanticRetrievalStatus,
    pub reranker: RerankerRetrievalStatus,
    pub qualification_applied: bool,
    pub unqualified_candidate_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub best_unqualified_coverage: Option<f64>,
    pub hits: Vec<SearchHit>,
    /// Internal single-snapshot partition consumed by `recall`; never emitted
    /// from the search protocol surface.
    #[serde(skip)]
    #[schemars(skip)]
    pub candidate_hits: Vec<SearchHit>,
    #[serde(skip)]
    #[schemars(skip)]
    pub candidate_hits_truncated: bool,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refine_hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broaden_hint: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RecallPresence {
    Claims,
    ArtifactsOnly,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RecallClaimStatus {
    Current,
    Disputed,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RecallEvidenceReference {
    pub artifact_id: String,
    pub revision_id: String,
    pub citation: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_byte: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_byte: Option<u64>,
    pub title: String,
    pub media_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub excerpt: Option<String>,
    pub excerpt_truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RecallClaim {
    pub claim_id: String,
    pub revision_id: String,
    pub claim_type: ClaimType,
    pub status: RecallClaimStatus,
    pub statement: String,
    pub citation: String,
    #[serde(default)]
    pub evidence: Vec<RecallEvidenceReference>,
    #[serde(default)]
    pub conflict_relation_ids: Vec<String>,
    pub score: f64,
    pub matched_by: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RecallArtifactReference {
    pub artifact_id: String,
    pub revision_id: String,
    pub citation: String,
    pub title: String,
    pub status: String,
    pub excerpt: String,
    pub excerpt_truncated: bool,
    pub score: f64,
    pub matched_by: Vec<String>,
    #[serde(default)]
    pub risk_signals: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CandidateRankingSignals {
    pub lexical_coverage: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigram_similarity: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub semantic_similarity: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RecallCandidateClaim {
    /// Always `unqualified_candidate`; serialized per item so the warning
    /// survives extraction from the surrounding recall response.
    pub retrieval_tier: String,
    pub claim_id: String,
    pub revision_id: String,
    pub claim_type: ClaimType,
    pub statement: String,
    pub statement_truncated: bool,
    pub citation: String,
    pub matched_by: Vec<String>,
    pub ranking_signals: CandidateRankingSignals,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RecallCandidateArtifactReference {
    /// Always `unqualified_candidate`; this is a suggestion, not remembered
    /// fact or qualified evidence.
    pub retrieval_tier: String,
    pub artifact_id: String,
    pub revision_id: String,
    pub title: String,
    pub citation: String,
    pub excerpt: String,
    pub excerpt_truncated: bool,
    pub matched_by: Vec<String>,
    #[serde(default)]
    pub risk_signals: Vec<String>,
    pub ranking_signals: CandidateRankingSignals,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RecallResult {
    pub content_is_untrusted: bool,
    pub query: String,
    pub query_analysis: QueryAnalysis,
    pub searched_horizons: Vec<Horizon>,
    pub semantic_claims: SemanticRetrievalStatus,
    pub semantic_artifacts: SemanticRetrievalStatus,
    pub reranker_claims: RerankerRetrievalStatus,
    pub reranker_artifacts: RerankerRetrievalStatus,
    /// Qualified results only. Candidate lists never affect presence.
    pub presence: RecallPresence,
    pub claims: Vec<RecallClaim>,
    pub conflicts: Vec<ConflictSummary>,
    pub artifact_refs: Vec<RecallArtifactReference>,
    pub candidate_claims: Vec<RecallCandidateClaim>,
    pub candidate_artifact_refs: Vec<RecallCandidateArtifactReference>,
    pub candidate_claims_truncated: bool,
    pub candidate_artifact_refs_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub candidates_hint: Option<String>,
    pub unqualified_claim_candidates: usize,
    pub unqualified_artifact_candidates: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub best_unqualified_claim_coverage: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub best_unqualified_artifact_coverage: Option<f64>,
    pub claims_truncated: bool,
    pub artifact_refs_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claims_refine_hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_refs_refine_hint: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProbeLocatorOrigin {
    /// The claim already carried exact evidence bytes.
    ClaimExact,
    /// Artifact candidate retrieval selected exact body bytes.
    ArtifactExact,
    /// A local, versioned projection resolved bytes inside the claim's single
    /// already-cited revision. This remains unqualified candidate routing.
    SemanticResolved,
    /// A local, versioned projection selected a strict subrange of an already
    /// exact artifact candidate. The parent citation remains available for one
    /// bounded deterministic expansion.
    SemanticWindowed,
    /// Only revision metadata is available; citation.get must refuse it.
    RevisionOnly,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProbeSourceLocator {
    /// Exact immutable pointer. Revision-only metadata pointers deliberately
    /// remain unfetchable through citation.get.
    pub citation: String,
    /// How the source pointer acquired its byte precision. This provenance is
    /// mandatory even though every probe lead remains untrusted and
    /// unqualified.
    pub locator_origin: ProbeLocatorOrigin,
    /// Present only for semantic_resolved locators. It versions both the
    /// deterministic locator template and local projection policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locator_policy_version: Option<String>,
    /// Exact authority revision hash used to validate a semantic_resolved
    /// locator against the disposable projection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_revision_hash: Option<String>,
    /// Present for a query-conditioned derived locator. The derived citation
    /// is always a strict exact-byte subrange of this immutable parent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_citation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProbeLead {
    /// A trailing ellipsis means the untrusted source title was truncated.
    pub title: String,
    /// Up to three locators belonging only to this lead's single winning claim,
    /// or one locator for a raw artifact lead. Claim locators are returned in
    /// deterministic document order.
    pub sources: Vec<ProbeSourceLocator>,
    /// False when evidence locators were omitted by the three-reference or
    /// four-KiB-per-lead caps. This is routing completeness, never answer
    /// qualification.
    pub evidence_locator_set_complete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ProbeResult {
    pub content_is_untrusted: bool,
    /// Applies to every lead; no probe result qualifies an answer.
    pub retrieval_tier: String,
    /// Human/task question against which final relevance must be judged.
    pub original_query: String,
    /// The one query actually used to route this probe.
    pub probe_query: String,
    pub reformulation_applied: bool,
    pub leads: Vec<ProbeLead>,
    pub available_count: usize,
    pub truncated: bool,
}

/// Exact untrusted bytes fetched by immutable citation. This envelope is
/// deliberately distinct from qualified recall results: existence at a
/// source location does not establish relevance or answer qualification.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CitationGetResult {
    pub content_is_untrusted: bool,
    /// Normalized citation describing exactly the returned bytes. When an
    /// oversized requested span is truncated this citation is narrowed.
    pub citation: String,
    pub content: String,
    pub byte_count: usize,
    pub media_type: String,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remaining_bytes: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ContextBundle {
    pub content_is_untrusted: bool,
    pub query: String,
    pub max_bytes: usize,
    pub used_bytes: usize,
    pub rendered_markdown: String,
    pub semantic: SemanticRetrievalStatus,
    pub reranker: RerankerRetrievalStatus,
    pub manifest: Vec<BundleManifestItem>,
    /// Search had more matches than its retrieval limit. This is independent
    /// from `omitted_count`, which only reports byte-budget omissions.
    pub retrieval_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refine_hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broaden_hint: Option<String>,
    pub omitted_count: usize,
    pub conflicts: Vec<ConflictSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BundleManifestItem {
    pub citation: String,
    pub entity_type: EntityType,
    pub entity_id: String,
    pub revision_id: String,
    pub status: String,
    pub context: AmbientContext,
    #[serde(default)]
    pub provenance: BTreeMap<String, Value>,
    /// Deterministic heuristic signals found in the retrieved text. Absence of
    /// signals never changes the content's untrusted status.
    #[serde(default)]
    pub risk_signals: Vec<String>,
    pub source_excerpt_bytes: usize,
    pub included_bytes: usize,
    pub excerpt_available: bool,
    pub excerpt_truncated: bool,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ConflictSummary {
    pub left_id: String,
    pub right_id: String,
    pub relation_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_resource_envelope_leaves_frame_headroom() {
        assert_eq!(MAX_ARTIFACT_BYTES, 8 * 1024 * 1024);
        assert_eq!(MAX_ENCODED_CONTENT_BYTES, 12 * 1024 * 1024);
        assert_eq!(MAX_FRAME_BYTES, 24 * 1024 * 1024);

        let maximum_base64_bytes = MAX_ARTIFACT_BYTES.div_ceil(3) * 4;
        assert!(maximum_base64_bytes <= MAX_ENCODED_CONTENT_BYTES);
    }

    #[test]
    fn nested_machine_inputs_reject_unknown_fields() {
        let claim = serde_json::json!({
            "claim_type": "fact",
            "statement": "A durable claim",
            "evidence": [{
                "artifact_id": "art_1",
                "revision_id": "arev_1",
                "bogus": true
            }]
        });
        assert!(serde_json::from_value::<ClaimAssertInput>(claim).is_err());

        let artifact = serde_json::json!({
            "kind": "document",
            "title": "Strict content",
            "content": {"type": "text", "data": "body", "bogus": true}
        });
        assert!(serde_json::from_value::<ArtifactPutInput>(artifact).is_err());

        let relation_list = serde_json::json!({
            "entity_type": "artifact",
            "entity_id": "art_1",
            "bogus": true
        });
        assert!(serde_json::from_value::<RelationListInput>(relation_list).is_err());

        let claim_history = serde_json::json!({
            "claim_id": "clm_1",
            "bogus": true
        });
        assert!(serde_json::from_value::<ClaimHistoryInput>(claim_history).is_err());

        let conflict_list = serde_json::json!({"bogus": true});
        assert!(serde_json::from_value::<ConflictListInput>(conflict_list).is_err());
    }

    #[test]
    fn claim_history_defaults_are_bounded_and_need_no_ambient_context() {
        let input: ClaimHistoryInput = serde_json::from_value(serde_json::json!({
            "claim_id": "clm_1"
        }))
        .unwrap();
        assert_eq!(input.limit, 50);
        assert!(input.before_revision_number.is_none());
        assert!(!Operation::ClaimHistory.is_mutating());
        assert!(!Operation::ClaimHistory.needs_context());
        assert_eq!(
            serde_json::to_value(Operation::ClaimHistory).unwrap(),
            "claim.history"
        );
    }

    #[test]
    fn relation_list_defaults_to_bounded_ambient_both_direction() {
        let input: RelationListInput = serde_json::from_value(serde_json::json!({
            "entity_type": "claim",
            "entity_id": "clm_1"
        }))
        .unwrap();
        assert!(matches!(input.direction, RelationDirection::Both));
        assert!(matches!(input.horizon, Horizon::Ambient));
        assert_eq!(input.limit, MAX_RELATION_LIST_ITEMS);
        assert!(input.relation.is_none());
        assert!(input.reason.is_none());
        assert!(input.before_commit_seq.is_none());
    }

    #[test]
    fn conflict_list_defaults_to_open_ambient_cases_and_is_non_mutating() {
        let input: ConflictListInput = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(matches!(input.horizon, Horizon::Ambient));
        assert!(!input.include_stale);
        assert_eq!(input.limit, MAX_CONFLICT_LIST_ITEMS);
        assert!(input.reason.is_none());
        assert!(input.before_case_sequence.is_none());
        assert!(!Operation::ConflictList.is_mutating());
        assert!(Operation::ConflictList.needs_context());
    }

    #[test]
    fn search_recency_defaults_enabled_and_rejects_unknown_settings() {
        let input: SearchInput = serde_json::from_value(serde_json::json!({
            "query": "durable memory"
        }))
        .unwrap();
        assert!(input.recency.enabled);
        assert!(matches!(input.horizon, Horizon::Ambient));

        let disabled: SearchInput = serde_json::from_value(serde_json::json!({
            "query": "durable memory",
            "recency": {"enabled": false}
        }))
        .unwrap();
        assert!(!disabled.recency.enabled);

        let unknown = serde_json::json!({
            "query": "durable memory",
            "recency": {"enabled": true, "unbounded": true}
        });
        assert!(serde_json::from_value::<SearchInput>(unknown).is_err());
    }

    #[test]
    fn recall_defaults_to_small_current_claim_first_retrieval() {
        let input: RecallInput = serde_json::from_value(serde_json::json!({
            "query": "storage decision"
        }))
        .unwrap();
        assert!(matches!(input.horizon, Horizon::Ambient));
        assert_eq!(input.max_claims, 5);
        assert_eq!(input.max_artifact_refs, 3);
        assert_eq!(input.max_excerpt_bytes, 320);
        assert_eq!(input.max_candidate_claims, 0);
        assert_eq!(input.max_candidate_artifact_refs, 0);
        assert!(input.recency.enabled);
        assert!(!Operation::MemoryRecall.is_mutating());
        assert!(Operation::MemoryRecall.needs_context());
        assert_eq!(
            serde_json::to_value(Operation::MemoryRecall).unwrap(),
            "memory.recall"
        );
        assert!(
            serde_json::from_value::<RecallInput>(serde_json::json!({
                "query": "storage",
                "include_historical": true
            }))
            .is_err()
        );
    }

    #[test]
    fn probe_defaults_to_a_small_explicit_unqualified_read() {
        let input: ProbeInput = serde_json::from_value(serde_json::json!({
            "query": "paraphrased deployment decision"
        }))
        .unwrap();
        assert_eq!(input.max_leads, 8);
        assert!(!Operation::MemoryProbe.is_mutating());
        assert!(Operation::MemoryProbe.needs_context());
        assert_eq!(
            serde_json::to_value(Operation::MemoryProbe).unwrap(),
            "memory.probe"
        );

        let unknown = serde_json::json!({
            "query": "deployment",
            "trust_candidates": true
        });
        assert!(serde_json::from_value::<ProbeInput>(unknown).is_err());
    }

    #[test]
    fn citation_get_is_context_free_and_bounded_by_default() {
        let input: CitationGetInput = serde_json::from_value(serde_json::json!({
            "citation": "memoree://artifact/art_1@arev_1#4-12"
        }))
        .unwrap();
        assert_eq!(input.max_bytes, MAX_CITATION_FETCH_BYTES);
        assert!(!Operation::CitationGet.is_mutating());
        assert!(!Operation::CitationGet.needs_context());
        assert_eq!(
            serde_json::to_value(Operation::CitationGet).unwrap(),
            "citation.get"
        );
    }

    #[test]
    fn retry_decision_errors_expose_machine_readable_details() {
        let revision = Response::failure(
            "req_revision",
            &crate::error::MemoryError::RevisionConflict {
                entity_type: "artifact",
                entity_id: "art_1".into(),
                current_revision: "arev_2".into(),
                requested_revision: "arev_1".into(),
            },
        );
        let details = &revision.error.unwrap().details;
        assert_eq!(details["entity_type"], "artifact");
        assert_eq!(details["current_revision"], "arev_2");
        assert_eq!(details["requested_revision"], "arev_1");

        let citation = Response::failure(
            "req_citation",
            &crate::error::MemoryError::Citation {
                kind: "range_required",
                message: "ranged evidence is required".into(),
                details: serde_json::json!({
                    "citation": "memoree://artifact/art_1@arev_1",
                    "total_byte_count": 40000,
                }),
            },
        );
        let error = citation.error.unwrap();
        assert_eq!(error.code, ErrorCode::CitationError);
        assert_eq!(error.details["citation_error"], "range_required");
        assert_eq!(error.details["total_byte_count"], 40000);

        let index = Response::failure(
            "req_index",
            &crate::error::MemoryError::IndexNotReady {
                requested: 9,
                current: 7,
            },
        );
        let details = &index.error.unwrap().details;
        assert_eq!(details["requested_commit_seq"], 9);
        assert_eq!(details["current_commit_seq"], 7);
    }
}
