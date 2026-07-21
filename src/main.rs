use std::{
    collections::BTreeMap,
    env,
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command as ProcessCommand, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand, ValueEnum};
use fs2::FileExt;
use memoree::{
    checkpoint::{CheckpointStore, MAX_CHECKPOINT_INPUT_BYTES},
    compiler::{
        CompilerProvider, CompilerRegistry, SelectionOrigin, interactive_selection_available,
    },
    context::{
        AppPaths, ContextResolver, MARKER_FILE, MEMOREE_CONTEXT_ENV, encode_memory_context,
        init_marker, task_context,
    },
    error::{MemoryError, Result},
    eval::run_retrieval_eval_with_models,
    protocol::{
        AmbientContext, ArtifactContent, ArtifactForgetInput, ArtifactGetInput,
        ArtifactHistoryInput, ArtifactPutInput, ArtifactReviseInput, BackupCreateInput,
        CitationGetInput, ClaimAssertInput, ClaimGetInput, ClaimHistoryInput, ClaimRetractInput,
        ClaimReviseInput, ClaimType, ConflictListInput, ContextBuildInput, ContextSource,
        DoctorResult, EntityType, EvidenceLocator, Horizon, MAX_ARTIFACT_BYTES,
        MAX_ENCODED_CONTENT_BYTES, Operation, ProbeInput, RecallInput, RecencyBiasInput,
        RelationDirection, RelationListInput, RelationPutInput, RelationType, Request, Response,
        SearchInput, Warning,
    },
    remember::{
        REMEMBER_SCHEMA_VERSION, ValidatedClaim, ValidatedCompilation, deterministic_title,
        input_digest,
    },
    reranker_eval::evaluate_reranker_pairs,
    service::MemoryService,
    store::{
        ArtifactRecord, ClaimRecord, MEMOREE_DATABASE_FILE, MutationResult, SCHEMA_VERSION, Store,
    },
    transport::{self, Endpoint},
    update::{
        AutoUpdateOutcome, apply_available_update, check_for_update, check_report,
        maybe_auto_update, record_managed_install, reexec_current_process, update_status,
    },
    upgrade::{
        SkillSyncReport, UpgradeLock, UpgradeState, ensure_upgrade_not_in_progress,
        load_upgrade_state, sync_skills, write_upgrade_state,
    },
};
use serde::Serialize;
use serde_json::{Value, json};

const ENDPOINT_ENV: &str = "MEMOREE_ENDPOINT";
const ACTOR_ENV: &str = "MEMOREE_ACTOR";
const NO_AUTOSTART_ENV: &str = "MEMOREE_NO_AUTOSTART";
const DAEMON_CHILD_ENV: &str = "MEMOREE_DAEMON_CHILD";
const DAEMON_START_TIMEOUT: Duration = Duration::from_secs(30);
const DAEMON_START_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug, Parser)]
#[command(
    name = "memoree",
    version,
    about = "Memoree: artifact-first local memory for machine agents",
    after_help = "Website: https://memoree.dev"
)]
struct Cli {
    /// Daemon endpoint. Defaults to a local Unix socket.
    #[arg(long, global = true, env = ENDPOINT_ENV)]
    endpoint: Option<String>,
    /// Pretty-print the otherwise compact JSON response.
    #[arg(long, global = true)]
    pretty: bool,
    /// Do not automatically start the local daemon.
    #[arg(long, global = true, env = NO_AUTOSTART_ENV)]
    no_autostart: bool,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Initialize ambient memory context in the current project.
    Init(InitArgs),
    /// Turn natural language into a durable artifact and grounded claims.
    Remember(RememberArgs),
    /// Stage a bounded agent-authored continuity note outside recall.
    Checkpoint(CheckpointArgs),
    /// Inspect, review, promote, or drop staged checkpoints.
    Pending {
        #[command(subcommand)]
        command: PendingCommands,
    },
    /// Execute one canonical JSON request from stdin.
    Call,
    /// Run the local memory daemon.
    Serve(ServeArgs),
    /// Inspect or control the auto-started local daemon.
    Daemon {
        #[command(subcommand)]
        command: DaemonCommands,
    },
    /// Reconcile binaries, storage, projections, daemon state, and agent skills.
    Upgrade {
        #[command(subcommand)]
        command: UpgradeCommands,
    },
    /// Inspect or apply signed Memoree releases.
    Update {
        #[command(subcommand)]
        command: UpdateCommands,
    },
    /// Install or refresh Memoree's canonical agent integrations.
    Skills {
        #[command(subcommand)]
        command: SkillsCommands,
    },
    /// Inspect or choose the authenticated CLI used for claim compilation.
    Compiler {
        #[command(subcommand)]
        command: CompilerCommands,
    },
    /// Inspect ambient context or build a bounded context bundle.
    Context {
        #[command(subcommand)]
        command: ContextCommands,
    },
    /// Install, inspect, or rebuild the optional local semantic index.
    Semantic {
        #[command(subcommand)]
        command: SemanticCommands,
    },
    /// Run a child process with an inherited task context.
    Session {
        #[command(subcommand)]
        command: SessionCommands,
    },
    /// Create, retrieve, revise, or forget artifacts.
    Artifact {
        #[command(subcommand)]
        command: ArtifactCommands,
    },
    /// Fetch an exact bounded UTF-8 span from an immutable citation.
    Citation {
        #[command(subcommand)]
        command: CitationCommands,
    },
    /// Create and manage atomic claims.
    Claim {
        #[command(subcommand)]
        command: ClaimCommands,
    },
    /// Create a typed relation between artifacts or claims.
    Link(LinkArgs),
    /// Inspect typed relations for an artifact or claim.
    Relation {
        #[command(subcommand)]
        command: RelationCommands,
    },
    /// Inspect revision-aware contradiction cases without changing memory.
    Conflict {
        #[command(subcommand)]
        command: ConflictCommands,
    },
    /// Search the ambient context, or explicitly request a broader horizon.
    Search(SearchArgs),
    /// Ask whether current memory contains relevant claims or source artifacts.
    Recall(RecallArgs),
    /// Explicitly inspect compact unqualified retrieval leads after weak recall.
    Probe(ProbeArgs),
    /// Print vendor-neutral, versioned instructions for language models.
    Instructions(InstructionsArgs),
    /// Print the protocol JSON Schema bundle.
    Schema,
    /// Print implemented protocol capabilities.
    Capabilities,
    /// Run a versioned retrieval corpus in an isolated temporary store.
    Eval(EvalArgs),
    /// Check daemon/database health.
    Doctor,
    /// Verify database, index, and blob integrity.
    Verify,
    /// Create a consistent database and blob backup.
    Backup {
        #[command(subcommand)]
        command: BackupCommands,
    },
}

#[derive(Debug, Args)]
struct RememberArgs {
    /// Text to remember. Use `-` to read UTF-8 text from stdin.
    #[arg(value_name = "TEXT", conflicts_with = "file")]
    text: Vec<String>,
    /// Read UTF-8 text from a file instead of the command line.
    #[arg(long, value_name = "PATH", conflicts_with = "text")]
    file: Option<PathBuf>,
    /// Store the source without invoking a compiler CLI or creating claims.
    #[arg(long)]
    raw: bool,
    /// Explicitly use Codex with one-run API-key auth instead of a persisted CLI selection.
    #[arg(long, conflicts_with = "raw")]
    allow_api_key: bool,
    /// Apply the displayed plan. Without this flag, the command is read-only.
    #[arg(long)]
    apply: bool,
    /// Override the deterministic title derived from the first non-empty line.
    #[arg(long)]
    title: Option<String>,
    /// Artifact kind. Defaults to `memory_note`.
    #[arg(long, default_value = "memory_note")]
    kind: String,
    /// Stable logical-operation key for exact retries.
    #[arg(long)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Args)]
struct EvalArgs {
    /// Corpus directory containing seed.jsonl, cases.jsonl, and baseline.json.
    #[arg(value_name = "CORPUS_DIRECTORY")]
    corpus: PathBuf,
    /// Verified local semantic model directory; enables dense evaluation without downloads.
    #[arg(long, value_name = "DIRECTORY")]
    semantic_model: Option<PathBuf>,
    /// Verified local ordering-only reranker directory; no downloads or qualification.
    #[arg(long, value_name = "DIRECTORY")]
    reranker_model: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct CheckpointArgs {
    /// Stable agent session/thread identifier; one pending slot is kept per session.
    #[arg(long)]
    session: String,
    /// Optional task label for human review.
    #[arg(long)]
    task: Option<String>,
    /// Deliberate checkpoint text. Use `-` to read UTF-8 from stdin.
    #[arg(required = true)]
    text: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum PendingCommands {
    /// List pending checkpoint metadata without exposing text.
    List {
        /// Include checkpoints older than the review window.
        #[arg(long)]
        all: bool,
    },
    /// Read one pending checkpoint, including its staged text.
    Show { checkpoint: String },
    /// Preview claim compilation without making the checkpoint recallable.
    Preview(PendingPromoteArgs),
    /// Explicitly promote a checkpoint through `memoree remember --apply`.
    Apply(PendingPromoteArgs),
    /// Explicitly remove one pending checkpoint.
    Drop { checkpoint: String },
}

#[derive(Debug, Args)]
struct PendingPromoteArgs {
    checkpoint: String,
    /// Permit promotion despite deterministic sensitive-data flags.
    #[arg(long)]
    allow_flagged: bool,
    /// Explicitly permit API-key auth if cached ChatGPT CLI auth is unavailable.
    #[arg(long)]
    allow_api_key: bool,
    /// Stable logical-operation key for exact apply retries.
    #[arg(long)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Args)]
struct InitArgs {
    #[arg(long)]
    name: Option<String>,
    /// Existing stable workspace id; a new workspace is created when omitted.
    #[arg(long)]
    workspace: Option<String>,
    #[arg(long, default_value = ".")]
    directory: PathBuf,
}

#[derive(Debug, Args)]
struct ServeArgs {
    #[arg(long)]
    listen: Option<String>,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// DANGEROUS: expose the unauthenticated protocol beyond host loopback.
    /// Use only behind a trusted container/network boundary.
    #[arg(long)]
    dangerously_allow_non_loopback_tcp: bool,
    #[arg(long, value_enum, default_value_t = DaemonLifecycleOwner::External, hide = true)]
    lifecycle_owner: DaemonLifecycleOwner,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum DaemonLifecycleOwner {
    Memoree,
    External,
}

impl DaemonLifecycleOwner {
    fn as_str(self) -> &'static str {
        match self {
            Self::Memoree => "memoree",
            Self::External => "external",
        }
    }
}

#[derive(Debug, Clone, Copy, Subcommand)]
enum DaemonCommands {
    /// Report whether the daemon is reachable; never auto-start it.
    Status,
    /// Gracefully stop the default local daemon.
    Stop,
    /// Gracefully replace the default local daemon with this binary.
    Restart,
}

#[derive(Debug, Subcommand)]
enum UpgradeCommands {
    /// Apply the idempotent local reconciliation required by an installed update.
    Apply {
        /// Version replaced by the installer, when known.
        #[arg(long)]
        previous_version: Option<String>,
        /// Permit the one-time 0.2 legacy default daemon restart.
        #[arg(long)]
        legacy_default_was_running: bool,
        /// Keep deterministic retrieval and do not install the default local
        /// ordering model during this confirmed reconciliation.
        #[arg(long)]
        without_reranker: bool,
    },
    /// Show the durable reconciliation state without starting a daemon.
    Status,
    /// Installer-only downgrade guard after a failed reconciliation.
    #[command(hide = true)]
    RollbackSafe,
}

#[derive(Debug, Subcommand)]
enum UpdateCommands {
    /// Show local automatic-update eligibility and cached state without network I/O.
    Status,
    /// Fetch and verify the latest signed release manifest without installing it.
    Check,
    /// Install and reconcile the latest signed release; invoking this is confirmation.
    Apply,
}

#[derive(Debug, Subcommand)]
enum SkillsCommands {
    /// Atomically synchronize the embedded use-memoree skill to installed agent homes.
    Sync,
}

#[derive(Debug, Subcommand)]
enum CompilerCommands {
    /// Discover installed CLIs, login state, live models, and the saved selection.
    Status,
    /// Validate and persist the preferred compiler provider and model.
    Configure {
        /// Authenticated CLI provider. Prompted when omitted in a terminal.
        #[arg(long, value_enum)]
        provider: Option<CompilerProviderArg>,
        /// Live model id or alias reported by the selected CLI.
        #[arg(long)]
        model: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CompilerProviderArg {
    Codex,
    Claude,
}

impl From<CompilerProviderArg> for CompilerProvider {
    fn from(value: CompilerProviderArg) -> Self {
        match value {
            CompilerProviderArg::Codex => Self::Codex,
            CompilerProviderArg::Claude => Self::Claude,
        }
    }
}

#[derive(Debug, Subcommand)]
enum ContextCommands {
    Show,
    Explain,
    Build(ContextBuildArgs),
}

#[derive(Debug, Clone, Subcommand)]
enum SemanticCommands {
    /// Explicitly download and verify the pinned local model, then index memory.
    Enable {
        /// Install a previously downloaded model directory without network I/O.
        #[arg(long, value_name = "DIRECTORY")]
        from_directory: Option<PathBuf>,
    },
    /// Rebuild the semantic projection from local authority without network I/O.
    Rebuild,
    /// Inspect the installed model, projection coverage, and freshness.
    Status,
    /// Install the pinned local cross-encoder for ordering only.
    EnableReranker {
        /// Install previously downloaded pinned model bytes without network I/O.
        #[arg(long, value_name = "DIRECTORY")]
        from_directory: Option<PathBuf>,
    },
    /// Inspect the ordering-only reranker and latency breaker.
    RerankerStatus,
    /// Calibrate and evaluate a local reranker against disjoint pair corpora.
    EvaluateReranker {
        /// Verified local reranker model directory; no downloads are performed.
        #[arg(long, value_name = "DIRECTORY")]
        model_directory: PathBuf,
        /// JSONL pair corpus used to choose raw-logit thresholds.
        #[arg(long, value_name = "JSONL")]
        calibration: PathBuf,
        /// Disjoint JSONL pair corpus evaluated only after threshold selection.
        #[arg(long, value_name = "JSONL")]
        heldout: PathBuf,
        /// Exact upstream model identifier recorded in calibration output.
        #[arg(long, default_value = memoree::semantic::RERANKER_MODEL_ID)]
        model_id: String,
        /// Exact immutable upstream model revision recorded in calibration output.
        #[arg(long, default_value = memoree::semantic::RERANKER_MODEL_REVISION)]
        model_revision: String,
    },
}

#[derive(Debug, Args)]
struct ContextBuildArgs {
    #[arg(required = true)]
    query: Vec<String>,
    #[arg(long, value_enum, default_value_t = HorizonArg::Ambient)]
    horizon: HorizonArg,
    #[arg(long)]
    reason: Option<String>,
    #[arg(long, default_value_t = 10)]
    limit: usize,
    #[arg(long, default_value_t = 16 * 1024)]
    max_bytes: usize,
    #[arg(long)]
    include_historical: bool,
    #[arg(long)]
    min_commit_seq: Option<i64>,
    /// Disable the small deterministic recency rerank for this retrieval.
    #[arg(long)]
    no_recency: bool,
}

#[derive(Debug, Subcommand)]
enum SessionCommands {
    Exec(SessionExecArgs),
}

#[derive(Debug, Args)]
struct SessionExecArgs {
    #[arg(long)]
    task: String,
    #[arg(required = true, trailing_var_arg = true)]
    command: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum ArtifactCommands {
    Put(ArtifactPutArgs),
    Get(ArtifactGetArgs),
    Revise(ArtifactReviseArgs),
    History {
        artifact_id: String,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long)]
        before_revision_number: Option<i64>,
    },
    Forget {
        artifact_id: String,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        idempotency_key: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum CitationCommands {
    Get(CitationGetArgs),
}

#[derive(Debug, Args)]
struct CitationGetArgs {
    citation: String,
    /// Maximum exact UTF-8 bytes to return; oversized ranges are narrowed.
    #[arg(long, default_value_t = memoree::protocol::MAX_CITATION_FETCH_BYTES)]
    max_bytes: usize,
}

#[derive(Debug, Args)]
struct ArtifactPutArgs {
    /// File path, or `-` to read bytes from stdin.
    path: String,
    #[arg(long, default_value = "document")]
    kind: String,
    #[arg(long)]
    title: Option<String>,
    #[arg(long)]
    media_type: Option<String>,
    #[arg(long)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Args)]
struct ArtifactGetArgs {
    artifact_id: String,
    #[arg(long)]
    revision: Option<String>,
    /// Materialize content to this local path after retrieval.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Replace an existing output path atomically.
    #[arg(long, requires = "output")]
    force: bool,
    #[arg(long)]
    metadata_only: bool,
}

#[derive(Debug, Args)]
struct ArtifactReviseArgs {
    artifact_id: String,
    path: String,
    #[arg(long)]
    if_revision: String,
    #[arg(long)]
    title: Option<String>,
    #[arg(long)]
    media_type: Option<String>,
    #[arg(long)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Subcommand)]
enum ClaimCommands {
    Assert(ClaimAssertArgs),
    Get {
        claim_id: String,
        #[arg(long)]
        revision: Option<String>,
    },
    History {
        claim_id: String,
        #[arg(long, default_value_t = 50)]
        limit: usize,
        #[arg(long)]
        before_revision_number: Option<i64>,
    },
    Revise(ClaimReviseArgs),
    Retract {
        claim_id: String,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        idempotency_key: Option<String>,
    },
}

#[derive(Debug, Args)]
struct ClaimAssertArgs {
    #[arg(value_enum)]
    claim_type: ClaimTypeArg,
    #[arg(required = true)]
    statement: Vec<String>,
    #[arg(long)]
    confidence: Option<f64>,
    /// Evidence as `ARTIFACT_ID@REVISION_ID` or with an exact `#START-END` byte span. Repeatable.
    #[arg(long)]
    evidence: Vec<String>,
    /// Inclusive RFC 3339 validity start for time-bounded knowledge.
    #[arg(long)]
    valid_from: Option<DateTime<Utc>>,
    /// Exclusive RFC 3339 validity end for observations that must expire.
    #[arg(long)]
    valid_until: Option<DateTime<Utc>>,
    #[arg(long)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Args)]
struct ClaimReviseArgs {
    claim_id: String,
    #[arg(required = true)]
    statement: Vec<String>,
    #[arg(long)]
    if_revision: String,
    #[arg(long)]
    confidence: Option<f64>,
    #[arg(long)]
    /// Evidence as `ARTIFACT_ID@REVISION_ID` or with an exact `#START-END` byte span. Repeatable.
    evidence: Vec<String>,
    #[arg(long)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Args)]
struct LinkArgs {
    /// `artifact:ID` or `claim:ID`.
    source: String,
    #[arg(value_enum)]
    relation: RelationTypeArg,
    /// `artifact:ID` or `claim:ID`.
    target: String,
    #[arg(long)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Subcommand)]
enum RelationCommands {
    /// List one-hop incoming or outgoing relations for an entity.
    List(RelationListArgs),
}

#[derive(Debug, Subcommand)]
enum ConflictCommands {
    /// List actionable contradictions in the ambient scope.
    List(ConflictListArgs),
}

#[derive(Debug, Args)]
struct RelationListArgs {
    /// `artifact:ID` or `claim:ID`.
    entity: String,
    #[arg(long, value_enum, default_value_t = RelationDirectionArg::Both)]
    direction: RelationDirectionArg,
    #[arg(long, value_enum)]
    relation: Option<RelationTypeArg>,
    #[arg(long, value_enum, default_value_t = HorizonArg::Ambient)]
    horizon: HorizonArg,
    #[arg(long)]
    reason: Option<String>,
    #[arg(long, default_value_t = 100)]
    limit: usize,
    #[arg(long)]
    before_commit_seq: Option<i64>,
}

#[derive(Debug, Args)]
struct ConflictListArgs {
    #[arg(long, value_enum, default_value_t = HorizonArg::Ambient)]
    horizon: HorizonArg,
    #[arg(long)]
    reason: Option<String>,
    /// Include cases made stale by a later claim revision.
    #[arg(long)]
    include_stale: bool,
    #[arg(long, default_value_t = 100)]
    limit: usize,
    #[arg(long)]
    before_case_sequence: Option<i64>,
}

#[derive(Debug, Args)]
struct SearchArgs {
    #[arg(required = true)]
    query: Vec<String>,
    #[arg(long, value_enum, default_value_t = HorizonArg::Ambient)]
    horizon: HorizonArg,
    #[arg(long)]
    reason: Option<String>,
    #[arg(long, default_value_t = 10)]
    limit: usize,
    #[arg(long)]
    include_historical: bool,
    #[arg(long)]
    min_commit_seq: Option<i64>,
    /// Disable the small deterministic recency rerank for this retrieval.
    #[arg(long)]
    no_recency: bool,
}

#[derive(Debug, Args)]
struct RecallArgs {
    #[arg(required = true)]
    query: Vec<String>,
    #[arg(long, value_enum, default_value_t = HorizonArg::Ambient)]
    horizon: HorizonArg,
    #[arg(long)]
    reason: Option<String>,
    #[arg(long, default_value_t = 5)]
    max_claims: usize,
    #[arg(long, default_value_t = 3)]
    max_artifact_refs: usize,
    #[arg(long, default_value_t = 320)]
    max_excerpt_bytes: usize,
    /// Maximum unqualified claim suggestions (0 disables, hard maximum 16).
    #[arg(long, default_value_t = 0)]
    max_candidate_claims: usize,
    /// Maximum unqualified artifact suggestions (0 disables, hard maximum 16).
    #[arg(long, default_value_t = 0)]
    max_candidate_artifact_refs: usize,
    #[arg(long)]
    min_commit_seq: Option<i64>,
    /// Disable the small deterministic recency rerank for this retrieval.
    #[arg(long)]
    no_recency: bool,
}

#[derive(Debug, Args)]
struct ProbeArgs {
    #[arg(required = true)]
    query: Vec<String>,
    /// Original question when QUERY is one meaning-preserving reformulation.
    #[arg(long)]
    original_query: Option<String>,
    #[arg(long, value_enum, default_value_t = HorizonArg::Ambient)]
    horizon: HorizonArg,
    #[arg(long)]
    reason: Option<String>,
    #[arg(long, default_value_t = 8)]
    max_leads: usize,
    #[arg(long)]
    min_commit_seq: Option<i64>,
    /// Disable the small deterministic recency rerank for this retrieval.
    #[arg(long)]
    no_recency: bool,
}

#[derive(Debug, Args)]
struct InstructionsArgs {
    #[arg(long, default_value = "markdown")]
    format: String,
}

#[derive(Debug, Subcommand)]
enum BackupCommands {
    Create { destination: PathBuf },
}

#[derive(Debug, Clone, Copy, ValueEnum, Default)]
enum HorizonArg {
    #[default]
    Ambient,
    Workspace,
    Personal,
}

impl From<HorizonArg> for Horizon {
    fn from(value: HorizonArg) -> Self {
        match value {
            HorizonArg::Ambient => Self::Ambient,
            HorizonArg::Workspace => Self::Workspace,
            HorizonArg::Personal => Self::Personal,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, Default)]
enum RelationDirectionArg {
    Incoming,
    Outgoing,
    #[default]
    Both,
}

impl From<RelationDirectionArg> for RelationDirection {
    fn from(value: RelationDirectionArg) -> Self {
        match value {
            RelationDirectionArg::Incoming => Self::Incoming,
            RelationDirectionArg::Outgoing => Self::Outgoing,
            RelationDirectionArg::Both => Self::Both,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ClaimTypeArg {
    Fact,
    Decision,
    Constraint,
    Preference,
    Procedure,
    Observation,
}

impl From<ClaimTypeArg> for ClaimType {
    fn from(value: ClaimTypeArg) -> Self {
        match value {
            ClaimTypeArg::Fact => Self::Fact,
            ClaimTypeArg::Decision => Self::Decision,
            ClaimTypeArg::Constraint => Self::Constraint,
            ClaimTypeArg::Preference => Self::Preference,
            ClaimTypeArg::Procedure => Self::Procedure,
            ClaimTypeArg::Observation => Self::Observation,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RelationTypeArg {
    DerivedFrom,
    Supports,
    Contradicts,
    Supersedes,
    References,
    Duplicates,
}

impl From<RelationTypeArg> for RelationType {
    fn from(value: RelationTypeArg) -> Self {
        match value {
            RelationTypeArg::DerivedFrom => Self::DerivedFrom,
            RelationTypeArg::Supports => Self::Supports,
            RelationTypeArg::Contradicts => Self::Contradicts,
            RelationTypeArg::Supersedes => Self::Supersedes,
            RelationTypeArg::References => Self::References,
            RelationTypeArg::Duplicates => Self::Duplicates,
        }
    }
}

struct PreparedRequest {
    request: Request,
    materialize: Option<MaterializeTarget>,
    auto_idempotency: bool,
}

struct MaterializeTarget {
    path: PathBuf,
    force: bool,
}

#[derive(Debug, Serialize)]
struct RememberCompilerReport {
    mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider: Option<CompilerProvider>,
    #[serde(skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cli_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    selection_origin: Option<SelectionOrigin>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    resolved_model_ids: Vec<String>,
    schema_version: u32,
}

#[derive(Debug, Serialize)]
struct RememberPlan {
    title: String,
    kind: String,
    media_type: &'static str,
    size_bytes: usize,
    claims: Vec<ValidatedClaim>,
    quality: RememberQualityReport,
}

#[derive(Debug, Clone, Serialize)]
struct RememberQualityFinding {
    code: &'static str,
    severity: &'static str,
    message: &'static str,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    claim_indexes: Vec<usize>,
}

#[derive(Debug, Clone, Serialize)]
struct RememberQualityReport {
    evidence_basis: &'static str,
    source_capture: &'static str,
    requires_review: bool,
    findings: Vec<RememberQualityFinding>,
}

#[derive(Debug, Serialize)]
struct RememberResult {
    applied: bool,
    input_digest: String,
    compiler: RememberCompilerReport,
    plan: RememberPlan,
    #[serde(skip_serializing_if = "Option::is_none")]
    artifact: Option<MutationResult<ArtifactRecord>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    stored_claims: Vec<MutationResult<ClaimRecord>>,
}

#[derive(Debug, Serialize)]
struct UpgradeApplyReport {
    from_version: Option<String>,
    to_version: String,
    authority: Value,
    daemon: Value,
    semantic: Value,
    reranker: Value,
    compiler: Value,
    skills: Option<SkillSyncReport>,
    state: UpgradeState,
    warnings: Vec<String>,
}

const UPGRADE_POST_COMMIT_EXIT_CODE: i32 = 20;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "memoree=warn".into()),
        )
        .with_writer(io::stderr)
        .init();

    let cli = Cli::parse();
    let pretty = cli.pretty;
    let code = match run(cli).await {
        Ok(code) => code,
        Err(error) => {
            let response = Response::failure("req_cli", &error);
            print_json(&response, pretty);
            error.exit_code()
        }
    };
    std::process::exit(code);
}

async fn run(cli: Cli) -> Result<i32> {
    let paths = AppPaths::discover()?;
    let upgrade_apply = matches!(
        &cli.command,
        Commands::Upgrade {
            command: UpgradeCommands::Apply { .. } | UpgradeCommands::RollbackSafe
        }
    );
    let update_command = matches!(&cli.command, Commands::Update { .. });
    let internal_daemon_child =
        env::var_os(DAEMON_CHILD_ENV).is_some() && matches!(&cli.command, Commands::Serve(_));
    if !upgrade_apply && !update_command && !internal_daemon_child {
        ensure_upgrade_not_in_progress(&paths)?;
    }
    let auto_update_eligible = cli.endpoint.is_none()
        && !cli.no_autostart
        && !matches!(
            &cli.command,
            Commands::Call
                | Commands::Serve(_)
                | Commands::Daemon { .. }
                | Commands::Upgrade { .. }
                | Commands::Update { .. }
                | Commands::Eval(_)
                | Commands::Session { .. }
        );
    if let AutoUpdateOutcome::Reexec(executable) = maybe_auto_update(&paths, auto_update_eligible)?
    {
        return reexec_current_process(&executable);
    }
    match cli.command {
        Commands::Init(args) => {
            let directory = fs::canonicalize(args.directory)?;
            let name = args.name.unwrap_or_else(|| {
                directory
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("project")
                    .to_owned()
            });
            let marker = init_marker(&directory, name, args.workspace.as_deref())?;
            let request = Request::new(Operation::ContextResolve, json!({}))?;
            let response = Response::success(
                &request,
                json!({
                    "marker": directory.join(MARKER_FILE),
                    "context": marker.context(),
                    "created": true,
                }),
            )?;
            print_json(&response, cli.pretty);
            Ok(0)
        }
        Commands::Remember(args) => {
            remember_command(
                args,
                cli.endpoint.as_deref(),
                cli.no_autostart,
                &paths,
                cli.pretty,
            )
            .await
        }
        Commands::Checkpoint(args) => checkpoint_command(args, &paths, cli.pretty),
        Commands::Pending { command } => {
            pending_command(
                command,
                cli.endpoint.as_deref(),
                cli.no_autostart,
                &paths,
                cli.pretty,
            )
            .await
        }
        Commands::Serve(args) => serve_daemon(args, &paths).await,
        Commands::Daemon { command } => {
            daemon_command(command, cli.endpoint.as_deref(), &paths, cli.pretty).await
        }
        Commands::Upgrade { command } => {
            upgrade_command(command, cli.endpoint.as_deref(), &paths, cli.pretty).await
        }
        Commands::Update { command } => update_command_handler(command, &paths, cli.pretty),
        Commands::Skills { command } => skills_command(command, &paths, cli.pretty),
        Commands::Compiler { command } => compiler_command(command, &paths, cli.pretty).await,
        Commands::Semantic { command } => semantic_command(command, &paths, cli.pretty).await,
        Commands::Eval(args) => {
            let report = run_retrieval_eval_with_models(
                &args.corpus,
                args.semantic_model.as_deref(),
                args.reranker_model.as_deref(),
            )
            .await?;
            print_json(&report, cli.pretty);
            Ok(i32::from(!report.passed))
        }
        Commands::Context {
            command: ContextCommands::Show,
        } => {
            let resolved = ContextResolver::new(env::current_dir()?)?.resolve()?;
            let request = Request::new(Operation::ContextResolve, json!({}))?;
            let mut response = Response::success(&request, &resolved.context)?;
            response.context = Some(resolved.protocol_context());
            print_json(&response, cli.pretty);
            Ok(0)
        }
        Commands::Context {
            command: ContextCommands::Explain,
        } => {
            let resolved = ContextResolver::new(env::current_dir()?)?.resolve()?;
            let request = Request::new(Operation::ContextResolve, json!({}))?;
            let mut response = Response::success(&request, &resolved.explanation)?;
            response.context = Some(resolved.protocol_context());
            print_json(&response, cli.pretty);
            Ok(0)
        }
        Commands::Session {
            command: SessionCommands::Exec(args),
        } => session_exec(args, cli.endpoint.as_deref(), cli.no_autostart, &paths),
        command => {
            let mut prepared = prepare_command(command)?;
            let request_id = prepared.request.request_id.clone();
            let response = async {
                attach_ambient_context(&mut prepared.request)?;
                if prepared.auto_idempotency
                    && prepared.request.op.is_mutating()
                    && prepared.request.idempotency_key.is_none()
                {
                    prepared.request.idempotency_key =
                        Some(default_idempotency(&prepared.request)?);
                }
                let endpoint = resolve_endpoint(cli.endpoint.as_deref(), &paths)?;
                let mut response = dispatch(
                    &endpoint,
                    &prepared.request,
                    !cli.no_autostart && cli.endpoint.is_none(),
                    &paths,
                )
                .await?;
                if response.ok
                    && let Some(target) = prepared.materialize
                {
                    materialize_artifact(&mut response, &target.path, target.force)?;
                }
                Ok::<Response, MemoryError>(response)
            }
            .await
            .unwrap_or_else(|error| Response::failure(request_id, &error));
            let code = response_exit_code(&response);
            print_json(&response, cli.pretty);
            Ok(code)
        }
    }
}

fn prepare_command(command: Commands) -> Result<PreparedRequest> {
    let mut materialize = None;
    let mut auto_idempotency = true;
    let request = match command {
        Commands::Call => {
            auto_idempotency = false;
            let mut source = String::new();
            io::stdin().read_to_string(&mut source)?;
            serde_json::from_str(&source).map_err(|error| {
                MemoryError::InvalidRequest(format!("stdin is not a valid request: {error}"))
            })?
        }
        Commands::Context {
            command: ContextCommands::Build(args),
        } => Request::new(
            Operation::ContextBuild,
            ContextBuildInput {
                search: SearchInput {
                    query: args.query.join(" "),
                    horizon: args.horizon.into(),
                    reason: args.reason,
                    limit: args.limit,
                    include_historical: args.include_historical,
                    min_commit_seq: args.min_commit_seq,
                    recency: RecencyBiasInput {
                        enabled: !args.no_recency,
                    },
                },
                max_bytes: args.max_bytes,
            },
        )?,
        Commands::Artifact { command } => match command {
            ArtifactCommands::Put(args) => {
                let (content, media_type, source) = read_artifact(&args.path, args.media_type)?;
                let title = args.title.unwrap_or_else(|| source.clone());
                let mut provenance = BTreeMap::new();
                provenance.insert("source".into(), Value::String(source));
                let mut request = Request::new(
                    Operation::ArtifactPut,
                    ArtifactPutInput {
                        kind: args.kind,
                        title,
                        media_type,
                        content,
                        provenance,
                        actor: env::var(ACTOR_ENV).ok(),
                    },
                )?;
                request.idempotency_key = args.idempotency_key;
                request
            }
            ArtifactCommands::Get(args) => {
                materialize = args.output.map(|path| MaterializeTarget {
                    path,
                    force: args.force,
                });
                Request::new(
                    Operation::ArtifactGet,
                    ArtifactGetInput {
                        artifact_id: args.artifact_id,
                        revision_id: args.revision,
                        include_content: !args.metadata_only || materialize.is_some(),
                    },
                )?
            }
            ArtifactCommands::Revise(args) => {
                let requested_media_type = args.media_type.clone();
                let (content, _detected_media_type, source) =
                    read_artifact(&args.path, args.media_type)?;
                let mut provenance = BTreeMap::new();
                provenance.insert("source".into(), Value::String(source));
                let mut request = Request::new(
                    Operation::ArtifactRevise,
                    ArtifactReviseInput {
                        artifact_id: args.artifact_id,
                        if_revision: args.if_revision,
                        title: args.title,
                        // Omission means "preserve the previous revision's
                        // media type"; guessing from a new path must not
                        // silently change indexing semantics.
                        media_type: requested_media_type,
                        content,
                        provenance,
                        actor: env::var(ACTOR_ENV).ok(),
                    },
                )?;
                request.idempotency_key = args.idempotency_key;
                request
            }
            ArtifactCommands::History {
                artifact_id,
                limit,
                before_revision_number,
            } => Request::new(
                Operation::ArtifactHistory,
                ArtifactHistoryInput {
                    artifact_id,
                    limit,
                    before_revision_number,
                },
            )?,
            ArtifactCommands::Forget {
                artifact_id,
                reason,
                idempotency_key,
            } => {
                let mut request = Request::new(
                    Operation::ArtifactForget,
                    ArtifactForgetInput {
                        artifact_id,
                        reason,
                    },
                )?;
                request.idempotency_key = idempotency_key;
                request
            }
        },
        Commands::Citation {
            command: CitationCommands::Get(args),
        } => Request::new(
            Operation::CitationGet,
            CitationGetInput {
                citation: args.citation,
                max_bytes: args.max_bytes,
            },
        )?,
        Commands::Claim { command } => match command {
            ClaimCommands::Assert(args) => {
                let mut request = Request::new(
                    Operation::ClaimAssert,
                    ClaimAssertInput {
                        claim_type: args.claim_type.into(),
                        statement: args.statement.join(" "),
                        confidence: args.confidence,
                        evidence: parse_evidence(&args.evidence)?,
                        valid_from: args.valid_from,
                        valid_until: args.valid_until,
                        actor: env::var(ACTOR_ENV).ok(),
                    },
                )?;
                request.idempotency_key = args.idempotency_key;
                request
            }
            ClaimCommands::Get { claim_id, revision } => Request::new(
                Operation::ClaimGet,
                ClaimGetInput {
                    claim_id,
                    revision_id: revision,
                },
            )?,
            ClaimCommands::History {
                claim_id,
                limit,
                before_revision_number,
            } => Request::new(
                Operation::ClaimHistory,
                ClaimHistoryInput {
                    claim_id,
                    limit,
                    before_revision_number,
                },
            )?,
            ClaimCommands::Revise(args) => {
                let mut request = Request::new(
                    Operation::ClaimRevise,
                    ClaimReviseInput {
                        claim_id: args.claim_id,
                        if_revision: args.if_revision,
                        statement: args.statement.join(" "),
                        confidence: args.confidence,
                        evidence: parse_evidence(&args.evidence)?,
                        actor: env::var(ACTOR_ENV).ok(),
                    },
                )?;
                request.idempotency_key = args.idempotency_key;
                request
            }
            ClaimCommands::Retract {
                claim_id,
                reason,
                idempotency_key,
            } => {
                let mut request = Request::new(
                    Operation::ClaimRetract,
                    ClaimRetractInput { claim_id, reason },
                )?;
                request.idempotency_key = idempotency_key;
                request
            }
        },
        Commands::Link(args) => {
            let (source_type, source_id) = parse_entity_ref(&args.source)?;
            let (target_type, target_id) = parse_entity_ref(&args.target)?;
            let mut request = Request::new(
                Operation::RelationPut,
                RelationPutInput {
                    source_type,
                    source_id,
                    relation: args.relation.into(),
                    target_type,
                    target_id,
                    metadata: BTreeMap::new(),
                },
            )?;
            request.idempotency_key = args.idempotency_key;
            request
        }
        Commands::Relation {
            command: RelationCommands::List(args),
        } => {
            let (entity_type, entity_id) = parse_entity_ref(&args.entity)?;
            Request::new(
                Operation::RelationList,
                RelationListInput {
                    entity_type,
                    entity_id,
                    direction: args.direction.into(),
                    relation: args.relation.map(Into::into),
                    horizon: args.horizon.into(),
                    reason: args.reason,
                    limit: args.limit,
                    before_commit_seq: args.before_commit_seq,
                },
            )?
        }
        Commands::Conflict {
            command: ConflictCommands::List(args),
        } => Request::new(
            Operation::ConflictList,
            ConflictListInput {
                horizon: args.horizon.into(),
                reason: args.reason,
                include_stale: args.include_stale,
                limit: args.limit,
                before_case_sequence: args.before_case_sequence,
            },
        )?,
        Commands::Search(args) => Request::new(
            Operation::Search,
            SearchInput {
                query: args.query.join(" "),
                horizon: args.horizon.into(),
                reason: args.reason,
                limit: args.limit,
                include_historical: args.include_historical,
                min_commit_seq: args.min_commit_seq,
                recency: RecencyBiasInput {
                    enabled: !args.no_recency,
                },
            },
        )?,
        Commands::Recall(args) => Request::new(
            Operation::MemoryRecall,
            RecallInput {
                query: args.query.join(" "),
                horizon: args.horizon.into(),
                reason: args.reason,
                max_claims: args.max_claims,
                max_artifact_refs: args.max_artifact_refs,
                max_excerpt_bytes: args.max_excerpt_bytes,
                max_candidate_claims: args.max_candidate_claims,
                max_candidate_artifact_refs: args.max_candidate_artifact_refs,
                min_commit_seq: args.min_commit_seq,
                recency: RecencyBiasInput {
                    enabled: !args.no_recency,
                },
            },
        )?,
        Commands::Probe(args) => Request::new(
            Operation::MemoryProbe,
            ProbeInput {
                query: args.query.join(" "),
                original_query: args.original_query,
                horizon: args.horizon.into(),
                reason: args.reason,
                max_leads: args.max_leads,
                min_commit_seq: args.min_commit_seq,
                recency: RecencyBiasInput {
                    enabled: !args.no_recency,
                },
            },
        )?,
        Commands::Instructions(args) => {
            Request::new(Operation::Instructions, json!({"format": args.format}))?
        }
        Commands::Schema => Request::new(Operation::Schema, json!({}))?,
        Commands::Capabilities => Request::new(Operation::Capabilities, json!({}))?,
        Commands::Doctor => Request::new(Operation::Doctor, json!({}))?,
        Commands::Verify => Request::new(Operation::Verify, json!({}))?,
        Commands::Backup {
            command: BackupCommands::Create { destination },
        } => {
            let destination = if destination.is_absolute() {
                destination
            } else {
                env::current_dir()?.join(destination)
            };
            Request::new(
                Operation::BackupCreate,
                BackupCreateInput {
                    destination: destination.display().to_string(),
                },
            )?
        }
        Commands::Init(_)
        | Commands::Remember(_)
        | Commands::Checkpoint(_)
        | Commands::Pending { .. }
        | Commands::Serve(_)
        | Commands::Daemon { .. }
        | Commands::Upgrade { .. }
        | Commands::Update { .. }
        | Commands::Skills { .. }
        | Commands::Compiler { .. }
        | Commands::Semantic { .. }
        | Commands::Eval(_)
        | Commands::Context {
            command: ContextCommands::Show | ContextCommands::Explain,
        }
        | Commands::Session { .. } => {
            return Err(MemoryError::InvalidRequest(
                "command is handled locally and cannot be converted to a daemon request".into(),
            ));
        }
    };

    Ok(PreparedRequest {
        request,
        materialize,
        auto_idempotency,
    })
}

async fn semantic_command(
    command: SemanticCommands,
    paths: &AppPaths,
    pretty: bool,
) -> Result<i32> {
    if let SemanticCommands::EvaluateReranker {
        model_directory,
        calibration,
        heldout,
        model_id,
        model_revision,
    } = &command
    {
        let report = evaluate_reranker_pairs(
            model_directory,
            calibration,
            heldout,
            model_id,
            model_revision,
        )?;
        print_json(&serde_json::to_value(&report)?, pretty);
        return Ok(if report.passed { 0 } else { 1 });
    }
    let default_endpoint = resolve_endpoint(None, paths)?;
    match probe_daemon(&default_endpoint).await {
        Ok(response) => {
            require_matching_daemon(&response, &default_endpoint)?;
        }
        Err(error) if daemon_is_unreachable(&error) => {}
        Err(error) => return Err(error),
    }
    let store = Store::open(&paths.data_dir)?;
    let output = match command {
        SemanticCommands::Enable { from_directory } => {
            let report = match from_directory {
                Some(directory) => store.semantic_enable_from_directory(&directory)?,
                None => store.semantic_enable()?,
            };
            serde_json::to_value(report)?
        }
        SemanticCommands::Rebuild => serde_json::to_value(store.semantic_rebuild()?)?,
        SemanticCommands::Status => serde_json::to_value(store.semantic_status()?)?,
        SemanticCommands::EnableReranker { from_directory } => {
            let report = match from_directory {
                Some(directory) => store.reranker_enable_from_directory(&directory)?,
                None => store.reranker_enable()?,
            };
            serde_json::to_value(report)?
        }
        SemanticCommands::RerankerStatus => serde_json::to_value(store.reranker_status()?)?,
        SemanticCommands::EvaluateReranker { .. } => unreachable!("handled above"),
    };
    print_json(&output, pretty);
    Ok(0)
}

fn skills_command(command: SkillsCommands, paths: &AppPaths, pretty: bool) -> Result<i32> {
    match command {
        SkillsCommands::Sync => {
            let report = sync_skills(paths)?;
            let request = Request::new(Operation::ContextResolve, json!({}))?;
            let response = Response::success(&request, report)?;
            print_json(&response, pretty);
            Ok(0)
        }
    }
}

async fn compiler_command(
    command: CompilerCommands,
    paths: &AppPaths,
    pretty: bool,
) -> Result<i32> {
    let registry = CompilerRegistry::default();
    let result = match command {
        CompilerCommands::Status => serde_json::to_value(registry.status(paths).await?)?,
        CompilerCommands::Configure { provider, model } => serde_json::to_value(
            registry
                .configure(
                    paths,
                    provider.map(Into::into),
                    model,
                    interactive_selection_available(),
                )
                .await?,
        )?,
    };
    let request = Request::new(Operation::ContextResolve, json!({}))?;
    let response = Response::success(&request, result)?;
    print_json(&response, pretty);
    Ok(0)
}

async fn upgrade_command(
    command: UpgradeCommands,
    endpoint_override: Option<&str>,
    paths: &AppPaths,
    pretty: bool,
) -> Result<i32> {
    match command {
        UpgradeCommands::Status => {
            let request = Request::new(Operation::ContextResolve, json!({}))?;
            let response = Response::success(
                &request,
                json!({
                    "binary_version": env!("CARGO_PKG_VERSION"),
                    "target_schema_version": SCHEMA_VERSION,
                    "state": load_upgrade_state(paths)?,
                }),
            )?;
            print_json(&response, pretty);
            Ok(0)
        }
        UpgradeCommands::RollbackSafe => {
            let schema = Store::inspect_schema_version(&paths.data_dir)?;
            let rollback_safe = schema.is_none_or(|version| version < SCHEMA_VERSION);
            let request = Request::new(Operation::ContextResolve, json!({}))?;
            let response = Response::success(
                &request,
                json!({
                    "rollback_safe": rollback_safe,
                    "store_schema_version": schema,
                    "current_schema_version": SCHEMA_VERSION,
                }),
            )?;
            print_json(&response, pretty);
            Ok(if rollback_safe {
                0
            } else {
                UPGRADE_POST_COMMIT_EXIT_CODE
            })
        }
        UpgradeCommands::Apply {
            previous_version,
            legacy_default_was_running,
            without_reranker,
        } => {
            apply_upgrade(
                paths,
                endpoint_override,
                previous_version,
                legacy_default_was_running,
                without_reranker,
                pretty,
            )
            .await
        }
    }
}

fn update_command_handler(command: UpdateCommands, paths: &AppPaths, pretty: bool) -> Result<i32> {
    let request = Request::new(Operation::ContextResolve, json!({}))?;
    let result = match command {
        UpdateCommands::Status => serde_json::to_value(update_status(paths)?)?,
        UpdateCommands::Check => serde_json::to_value(check_report(paths)?)?,
        UpdateCommands::Apply => match check_for_update(paths, true)? {
            Some(update) => {
                let version = update.manifest.version.clone();
                let target = update.target.triple.clone();
                let _ = apply_available_update(paths, &update)?;
                json!({
                    "updated": true,
                    "version": version,
                    "target": target,
                    "reconciled": true,
                    "next_command_uses_new_binary": true,
                })
            }
            None => json!({
                "updated": false,
                "version": memoree::update::current_version(),
                "reason": "already_current",
            }),
        },
    };
    print_json(&Response::success(&request, result)?, pretty);
    Ok(0)
}

async fn apply_upgrade(
    paths: &AppPaths,
    endpoint_override: Option<&str>,
    previous_version: Option<String>,
    legacy_default_was_running: bool,
    without_reranker: bool,
    pretty: bool,
) -> Result<i32> {
    let _upgrade_lock = UpgradeLock::acquire(paths)?;
    record_managed_install(paths)?;
    let endpoint = resolve_endpoint(endpoint_override, paths)?;
    let observed = match probe_daemon(&endpoint).await {
        Ok(response) => Some(doctor_result(&response)?),
        Err(error) if daemon_is_unreachable(&error) => None,
        Err(error) => return Err(error),
    };
    let previous_state = load_upgrade_state(paths)?;
    let resumable_running_state = previous_state
        .as_ref()
        .filter(|state| {
            state.target_version == env!("CARGO_PKG_VERSION") && state.phase != "complete"
        })
        .is_some_and(|state| state.prior_daemon_running);
    let prior_daemon_running =
        observed.is_some() || legacy_default_was_running || resumable_running_state;
    let previous_daemon_version = previous_version
        .or_else(|| {
            observed
                .as_ref()
                .filter(|doctor| !doctor.binary_version.is_empty())
                .map(|doctor| doctor.binary_version.clone())
        })
        .or_else(|| {
            previous_state
                .as_ref()
                .and_then(|state| state.previous_daemon_version.clone())
        });
    let mut state = UpgradeState::new(prior_daemon_running, previous_daemon_version.clone());
    if let Some(previous) = previous_state
        && previous.target_version == env!("CARGO_PKG_VERSION")
        && previous.phase != "complete"
    {
        state.prior_daemon_running |= previous.prior_daemon_running;
        state.migration_backup = previous.migration_backup;
        state.store_schema_version = previous.store_schema_version;
    }
    write_upgrade_state(paths, &state)?;

    let mut warnings = Vec::new();
    let reranker_install_opted_out = without_reranker
        || env::var("MEMOREE_SKIP_RERANKER_INSTALL")
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);
    let compiler = match CompilerRegistry::default()
        .reconcile_upgrade(paths, previous_daemon_version.as_deref())
        .await
    {
        Ok(report) => serde_json::to_value(report)?,
        Err(error) => {
            warnings.push(format!(
                "compiler preference reconciliation failed; `memoree remember` will require `memoree compiler configure`: {error}"
            ));
            json!({"state": "error", "reason": error.to_string()})
        }
    };
    if endpoint_override.is_some() {
        state.set_phase("external_daemon_action_required");
        write_upgrade_state(paths, &state)?;
        let skills = match sync_skills(paths) {
            Ok(report) => Some(report),
            Err(error) => {
                warnings.push(format!("agent skill sync failed: {error}"));
                None
            }
        };
        let report = UpgradeApplyReport {
            from_version: previous_daemon_version,
            to_version: env!("CARGO_PKG_VERSION").into(),
            authority: json!({"state": "deferred", "reason": "an explicit endpoint is supervisor-owned"}),
            daemon: json!({
                "state": "external_action_required",
                "endpoint": endpoint.display(),
                "observed": observed,
                "remediation": "restart the supervisor that owns the explicit daemon, then run `memoree upgrade apply` without that endpoint on its host if local reconciliation is required"
            }),
            semantic: json!({"state": "deferred", "downloaded": false}),
            reranker: json!({"state": "deferred", "downloaded": false}),
            compiler: compiler.clone(),
            skills,
            state,
            warnings,
        };
        print_upgrade_report(report, pretty)?;
        return Ok(UPGRADE_POST_COMMIT_EXIT_CODE);
    }
    let mut daemon_stopped = false;
    if let Some(doctor) = &observed {
        let legacy_allowed = doctor.binary_version.is_empty()
            && legacy_default_was_running
            && previous_daemon_version
                .as_deref()
                .is_some_and(|version| version.trim_start_matches('v').starts_with("0.2."));
        let managed = doctor.lifecycle_owner == "memoree";
        if !managed && !legacy_allowed {
            state.set_phase("external_daemon_action_required");
            write_upgrade_state(paths, &state)?;
            let skills = match sync_skills(paths) {
                Ok(report) => Some(report),
                Err(error) => {
                    warnings.push(format!("agent skill sync failed: {error}"));
                    None
                }
            };
            let report = UpgradeApplyReport {
                from_version: previous_daemon_version,
                to_version: env!("CARGO_PKG_VERSION").into(),
                authority: json!({"state": "deferred", "reason": "an external daemon still owns the store"}),
                daemon: json!({
                    "state": "external_action_required",
                    "endpoint": endpoint.display(),
                    "running": true,
                    "binary_version": doctor.binary_version,
                    "lifecycle_owner": doctor.lifecycle_owner,
                    "remediation": "restart the supervisor that owns this daemon, then run `memoree upgrade apply` again"
                }),
                semantic: json!({"state": "deferred", "downloaded": false}),
                reranker: json!({"state": "deferred", "downloaded": false}),
                compiler: compiler.clone(),
                skills,
                state,
                warnings,
            };
            print_upgrade_report(report, pretty)?;
            return Ok(UPGRADE_POST_COMMIT_EXIT_CODE);
        }
        stop_local_daemon(&endpoint).await?;
        daemon_stopped = true;
        state.set_phase("daemon_stopped");
        write_upgrade_state(paths, &state)?;
    }

    let daemon_data_lock_path = paths.data_dir.join("memoreed.lock");
    let daemon_data_lock = open_private_lock_file(&daemon_data_lock_path)?;
    if let Err(error) = daemon_data_lock.try_lock_exclusive() {
        state.set_phase("external_daemon_action_required");
        write_upgrade_state(paths, &state)?;
        let skills = match sync_skills(paths) {
            Ok(report) => Some(report),
            Err(skill_error) => {
                warnings.push(format!("agent skill sync failed: {skill_error}"));
                None
            }
        };
        let report = UpgradeApplyReport {
            from_version: previous_daemon_version,
            to_version: env!("CARGO_PKG_VERSION").into(),
            authority: json!({
                "state": "deferred",
                "reason": "another daemon still holds the authoritative data directory"
            }),
            daemon: json!({
                "state": "external_action_required",
                "lock": daemon_data_lock_path,
                "reason": error.to_string(),
                "remediation": "stop the supervisor or non-default daemon that owns this data directory, then run `memoree upgrade apply` again"
            }),
            semantic: json!({"state": "deferred", "downloaded": false}),
            reranker: json!({"state": "deferred", "downloaded": false}),
            compiler: compiler.clone(),
            skills,
            state,
            warnings,
        };
        print_upgrade_report(report, pretty)?;
        return Ok(UPGRADE_POST_COMMIT_EXIT_CODE);
    }

    let database_path = paths.data_dir.join(MEMOREE_DATABASE_FILE);
    let mut authority = json!({"state": "not_initialized", "database_present": false});
    let mut semantic = json!({"state": "not_installed", "downloaded": false});
    let mut reranker = json!({"state": "not_installed", "downloaded": false});
    if database_path.is_file() {
        state.set_phase("reconciling_authority");
        write_upgrade_state(paths, &state)?;
        let store = Store::open(&paths.data_dir)?;
        let migration = store.schema_migration().cloned();
        state.migration_backup = migration
            .as_ref()
            .map(|report| report.backup_destination.clone())
            .or(state.migration_backup);
        state.store_schema_version = Some(store.schema_version()?);
        state.set_phase("authority_committed");
        write_upgrade_state(paths, &state)?;

        let verification = store.verify()?;
        authority = json!({
            "state": if verification.ok { "ready" } else { "verification_failed" },
            "database_present": true,
            "schema_version": verification.schema_version,
            "last_commit_seq": verification.last_commit_seq,
            "migration": migration,
            "verification": verification,
        });
        if !authority["verification"]["ok"].as_bool().unwrap_or(false) {
            state.set_phase("authority_verification_failed");
            write_upgrade_state(paths, &state)?;
            let report = UpgradeApplyReport {
                from_version: previous_daemon_version,
                to_version: env!("CARGO_PKG_VERSION").into(),
                authority,
                daemon: json!({"state": "stopped", "running_before": state.prior_daemon_running}),
                semantic,
                reranker,
                compiler: compiler.clone(),
                skills: None,
                state,
                warnings,
            };
            print_upgrade_report(report, pretty)?;
            return Ok(UPGRADE_POST_COMMIT_EXIT_CODE);
        }

        if store.semantic_model_installed() {
            semantic = match store.semantic_status() {
                Ok(status) if status.state == "ready" => serde_json::to_value(status)?,
                Ok(status) => match store.semantic_rebuild() {
                    Ok(rebuild) => json!({
                        "state": "rebuilt",
                        "downloaded": false,
                        "previous_status": status,
                        "rebuild": rebuild,
                    }),
                    Err(error) => {
                        warnings.push(format!(
                            "installed semantic projection could not be rebuilt; deterministic retrieval remains available: {error}"
                        ));
                        json!({"state": "error", "downloaded": false, "reason": error.to_string()})
                    }
                },
                Err(error) => {
                    warnings.push(format!(
                        "installed semantic projection could not be inspected; deterministic retrieval remains available: {error}"
                    ));
                    json!({"state": "error", "downloaded": false, "reason": error.to_string()})
                }
            };
        }
        reranker = if reranker_install_opted_out {
            json!({
                "state": "opted_out",
                "downloaded": false,
                "reason": "--without-reranker or MEMOREE_SKIP_RERANKER_INSTALL"
            })
        } else {
            match store.reranker_status() {
                Ok(status) if status.state == "ready" => json!({
                    "state": "ready",
                    "downloaded": false,
                    "status": status,
                }),
                previous_status => {
                    state.set_phase("installing_reranker");
                    write_upgrade_state(paths, &state)?;
                    match store.reranker_enable() {
                        Ok(installed) => json!({
                            "state": "installed",
                            "downloaded": true,
                            "previous_status": previous_status.ok(),
                            "install": installed,
                        }),
                        Err(error) => {
                            warnings.push(format!(
                                "the pinned local ordering model could not be installed; deterministic ordering remains available and a later `memoree upgrade apply` will retry: {error}"
                            ));
                            json!({
                                "state": "unavailable",
                                "downloaded": false,
                                "reason": error.to_string(),
                            })
                        }
                    }
                }
            }
        };
    }

    let skills = match sync_skills(paths) {
        Ok(report) => Some(report),
        Err(error) => {
            warnings.push(format!("agent skill sync failed: {error}"));
            None
        }
    };
    state.set_phase("local_state_reconciled");
    write_upgrade_state(paths, &state)?;

    drop(daemon_data_lock);
    let should_restart = state.prior_daemon_running && (daemon_stopped || observed.is_none());
    let daemon = if should_restart {
        let mut child = match start_daemon(&endpoint, paths) {
            Ok(child) => child,
            Err(error) => {
                state.set_phase("daemon_restart_failed");
                write_upgrade_state(paths, &state)?;
                warnings.push(format!("new daemon could not be started: {error}"));
                let report = UpgradeApplyReport {
                    from_version: previous_daemon_version,
                    to_version: env!("CARGO_PKG_VERSION").into(),
                    authority,
                    daemon: json!({
                        "state": "restart_failed",
                        "running_before": true,
                        "remediation": "run `memoree daemon restart`"
                    }),
                    semantic,
                    reranker,
                    compiler: compiler.clone(),
                    skills,
                    state,
                    warnings,
                };
                print_upgrade_report(report, pretty)?;
                return Ok(UPGRADE_POST_COMMIT_EXIT_CODE);
            }
        };
        match wait_for_daemon(&endpoint, &mut child).await {
            Ok(response) => {
                let doctor = require_matching_daemon(&response, &endpoint)?;
                if doctor.schema_version != SCHEMA_VERSION || doctor.lifecycle_owner != "memoree" {
                    state.set_phase("daemon_restart_failed");
                    write_upgrade_state(paths, &state)?;
                    warnings.push(format!(
                        "new daemon health mismatch: schema={}, owner={}",
                        doctor.schema_version, doctor.lifecycle_owner
                    ));
                    json!({"state": "restart_failed", "doctor": doctor})
                } else {
                    json!({"state": "restarted", "running_before": true, "doctor": doctor})
                }
            }
            Err(error) => {
                state.set_phase("daemon_restart_failed");
                write_upgrade_state(paths, &state)?;
                warnings.push(format!("new daemon failed its health check: {error}"));
                json!({
                    "state": "restart_failed",
                    "running_before": true,
                    "remediation": "run `memoree daemon restart`"
                })
            }
        }
    } else if let Some(doctor) = observed {
        json!({"state": "already_current", "running_before": true, "doctor": doctor})
    } else {
        json!({"state": "remained_stopped", "running_before": false})
    };

    let restart_failed = daemon["state"] == "restart_failed";
    state.set_phase(if restart_failed {
        "daemon_restart_failed"
    } else {
        "complete"
    });
    write_upgrade_state(paths, &state)?;
    let report = UpgradeApplyReport {
        from_version: previous_daemon_version,
        to_version: env!("CARGO_PKG_VERSION").into(),
        authority,
        daemon,
        semantic,
        reranker,
        compiler,
        skills,
        state,
        warnings,
    };
    print_upgrade_report(report, pretty)?;
    Ok(if restart_failed {
        UPGRADE_POST_COMMIT_EXIT_CODE
    } else {
        0
    })
}

fn print_upgrade_report(report: UpgradeApplyReport, pretty: bool) -> Result<()> {
    let request = Request::new(Operation::ContextResolve, json!({}))?;
    let response = Response::success(&request, report)?;
    print_json(&response, pretty);
    Ok(())
}

fn checkpoint_command(args: CheckpointArgs, paths: &AppPaths, pretty: bool) -> Result<i32> {
    let text = read_checkpoint_source(&args.text)?;
    let checkpoint =
        CheckpointStore::new(&paths.data_dir).put(&args.session, args.task.as_deref(), &text)?;
    let sensitive_flags = checkpoint.sensitive_flags.clone();
    let summary = checkpoint.summary(Utc::now());
    let request = Request::new(Operation::ContextResolve, json!({}))?;
    let mut response = Response::success(
        &request,
        json!({
            "pending": true,
            "recallable": false,
            "checkpoint": summary,
        }),
    )?;
    if !sensitive_flags.is_empty() {
        response.warnings.push(Warning {
            code: "CHECKPOINT_SENSITIVE_CONTENT".into(),
            message: format!(
                "Checkpoint is quarantined and cannot be promoted without --allow-flagged; flags: {}",
                sensitive_flags.join(", ")
            ),
        });
    }
    print_json(&response, pretty);
    Ok(0)
}

async fn pending_command(
    command: PendingCommands,
    endpoint_value: Option<&str>,
    no_autostart: bool,
    paths: &AppPaths,
    pretty: bool,
) -> Result<i32> {
    let store = CheckpointStore::new(&paths.data_dir);
    match command {
        PendingCommands::List { all } => {
            let request = Request::new(Operation::ContextResolve, json!({}))?;
            let response = Response::success(
                &request,
                json!({
                    "recallable": false,
                    "include_expired": all,
                    "checkpoints": store.list(all)?,
                }),
            )?;
            print_json(&response, pretty);
            Ok(0)
        }
        PendingCommands::Show { checkpoint } => {
            let request = Request::new(Operation::ContextResolve, json!({}))?;
            let response = Response::success(
                &request,
                json!({
                    "recallable": false,
                    "checkpoint": store.get(&checkpoint)?,
                }),
            )?;
            print_json(&response, pretty);
            Ok(0)
        }
        PendingCommands::Preview(args) => {
            promote_pending_checkpoint(
                &store,
                args,
                false,
                endpoint_value,
                no_autostart,
                paths,
                pretty,
            )
            .await
        }
        PendingCommands::Apply(args) => {
            promote_pending_checkpoint(
                &store,
                args,
                true,
                endpoint_value,
                no_autostart,
                paths,
                pretty,
            )
            .await
        }
        PendingCommands::Drop { checkpoint } => {
            let dropped = store.drop_checkpoint(&checkpoint)?;
            let request = Request::new(Operation::ContextResolve, json!({}))?;
            let response = Response::success(
                &request,
                json!({
                    "dropped": true,
                    "checkpoint_id": dropped.checkpoint_id,
                    "session_id": dropped.session_id,
                    "recoverable": false,
                }),
            )?;
            print_json(&response, pretty);
            Ok(0)
        }
    }
}

async fn promote_pending_checkpoint(
    store: &CheckpointStore,
    args: PendingPromoteArgs,
    apply: bool,
    endpoint_value: Option<&str>,
    no_autostart: bool,
    paths: &AppPaths,
    pretty: bool,
) -> Result<i32> {
    let checkpoint = store.get(&args.checkpoint)?;
    if !checkpoint.sensitive_flags.is_empty() && !args.allow_flagged {
        return Err(MemoryError::InvalidRequest(format!(
            "pending checkpoint {} has sensitive-content flags ({}); inspect it and pass --allow-flagged only if promotion is intentional",
            checkpoint.checkpoint_id,
            checkpoint.sensitive_flags.join(", ")
        )));
    }
    remember_command(
        RememberArgs {
            text: vec![checkpoint.text],
            file: None,
            raw: false,
            allow_api_key: args.allow_api_key,
            apply,
            title: Some(format!(
                "Checkpoint {} ({})",
                checkpoint.checkpoint_id, checkpoint.session_id
            )),
            kind: "agent_checkpoint".into(),
            idempotency_key: args.idempotency_key,
        },
        endpoint_value,
        no_autostart,
        paths,
        pretty,
    )
    .await
}

fn read_checkpoint_source(values: &[String]) -> Result<String> {
    if values == ["-"] {
        let mut bytes = Vec::new();
        io::stdin()
            .take((MAX_CHECKPOINT_INPUT_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_CHECKPOINT_INPUT_BYTES {
            return Err(MemoryError::ContentTooLarge);
        }
        String::from_utf8(bytes).map_err(|error| {
            MemoryError::InvalidRequest(format!("checkpoint stdin is not UTF-8: {error}"))
        })
    } else {
        let text = values.join(" ");
        if text.len() > MAX_CHECKPOINT_INPUT_BYTES {
            return Err(MemoryError::ContentTooLarge);
        }
        Ok(text)
    }
}

async fn remember_command(
    args: RememberArgs,
    endpoint_value: Option<&str>,
    no_autostart: bool,
    paths: &AppPaths,
    pretty: bool,
) -> Result<i32> {
    let source_capture = remember_source_capture(&args);
    let (source, source_label) = read_remember_source(&args)?;
    if source.trim().is_empty() {
        return Err(MemoryError::InvalidRequest(
            "remembered text must not be empty".to_owned(),
        ));
    }
    validate_remember_kind(&args.kind)?;
    let title = args.title.unwrap_or_else(|| deterministic_title(&source));
    if title.trim().is_empty() || title.len() > memoree::protocol::MAX_TITLE_BYTES {
        return Err(MemoryError::InvalidRequest(format!(
            "remember title must contain 1..={} UTF-8 bytes",
            memoree::protocol::MAX_TITLE_BYTES
        )));
    }

    // Resolve and freeze ambient scope before invoking a model. The compiler
    // never receives the scope and cannot influence where writes land.
    let resolved = ContextResolver::new(env::current_dir()?)?.resolve()?;
    let (compilation, compiler) = if args.raw {
        (
            ValidatedCompilation { claims: vec![] },
            RememberCompilerReport {
                mode: "raw".to_owned(),
                provider: None,
                model: None,
                cli_version: None,
                selection_origin: None,
                resolved_model_ids: Vec::new(),
                schema_version: REMEMBER_SCHEMA_VERSION,
            },
        )
    } else {
        let execution = CompilerRegistry::default()
            .compile(
                paths,
                &source,
                args.allow_api_key,
                interactive_selection_available(),
            )
            .await?;
        (
            execution.compilation,
            RememberCompilerReport {
                mode: execution.report.mode,
                provider: Some(execution.report.provider),
                model: Some(execution.report.model),
                cli_version: Some(execution.report.cli_version),
                selection_origin: Some(execution.report.selection_origin),
                resolved_model_ids: execution.report.resolved_model_ids,
                schema_version: REMEMBER_SCHEMA_VERSION,
            },
        )
    };

    let digest = input_digest(&source);
    let quality = remember_quality(source_capture, args.raw, &compilation.claims);
    let plan = RememberPlan {
        title: title.clone(),
        kind: args.kind.clone(),
        media_type: "text/plain; charset=utf-8",
        size_bytes: source.len(),
        claims: compilation.claims.clone(),
        quality: quality.clone(),
    };
    let mut artifact = None;
    let mut stored_claims = Vec::new();
    let mut last_commit_seq = None;
    let mut warnings = quality
        .findings
        .iter()
        .filter(|finding| finding.severity == "warning")
        .map(|finding| Warning {
            code: finding.code.to_owned(),
            message: finding.message.to_owned(),
        })
        .collect::<Vec<_>>();

    if args.apply {
        let endpoint = resolve_endpoint(endpoint_value, paths)?;
        let autostart = !no_autostart && endpoint_value.is_none();
        let logical_operation = args.idempotency_key.unwrap_or_else(|| {
            let identity = format!(
                "remember-v{}\0{}\0{}\0{}\0{}",
                REMEMBER_SCHEMA_VERSION, digest, title, args.kind, source_label
            );
            format!("remember:{}", blake3::hash(identity.as_bytes()).to_hex())
        });
        let actor = env::var(ACTOR_ENV)
            .ok()
            .unwrap_or_else(|| "memoree.remember".to_owned());
        let mut provenance = BTreeMap::new();
        provenance.insert("source".to_owned(), Value::String(source_label));
        provenance.insert("input_digest".to_owned(), Value::String(digest.clone()));
        provenance.insert(
            "remember_schema_version".to_owned(),
            Value::from(REMEMBER_SCHEMA_VERSION),
        );
        provenance.insert("compiler".to_owned(), Value::String(compiler.mode.clone()));
        if let Some(provider) = compiler.provider {
            provenance.insert(
                "compiler_provider".to_owned(),
                Value::String(provider.as_str().to_owned()),
            );
        }
        if let Some(model) = &compiler.model {
            provenance.insert("model".to_owned(), Value::String(model.clone()));
        }
        if let Some(cli_version) = &compiler.cli_version {
            provenance.insert(
                "compiler_cli_version".to_owned(),
                Value::String(cli_version.clone()),
            );
        }
        if let Some(origin) = compiler.selection_origin {
            provenance.insert("compiler_selection_origin".to_owned(), json!(origin));
        }
        if !compiler.resolved_model_ids.is_empty() {
            provenance.insert(
                "resolved_model_ids".to_owned(),
                serde_json::to_value(&compiler.resolved_model_ids)?,
            );
        }

        let mut artifact_request = Request::new(
            Operation::ArtifactPut,
            ArtifactPutInput {
                kind: args.kind,
                title,
                media_type: "text/plain; charset=utf-8".to_owned(),
                content: ArtifactContent::Text(source),
                provenance,
                actor: Some(actor.clone()),
            },
        )?;
        artifact_request.context = Some(resolved.context.clone());
        artifact_request.context_source = resolved.source.clone();
        artifact_request.idempotency_key =
            Some(remember_idempotency(&logical_operation, "artifact"));
        let artifact_response = dispatch(&endpoint, &artifact_request, autostart, paths).await?;
        if !artifact_response.ok {
            let code = response_exit_code(&artifact_response);
            print_json(&artifact_response, pretty);
            return Ok(code);
        }
        warnings.extend(artifact_response.warnings);
        let stored_artifact: MutationResult<ArtifactRecord> =
            serde_json::from_value(artifact_response.result.ok_or_else(|| {
                MemoryError::Integrity(
                    "artifact.put succeeded without a mutation result".to_owned(),
                )
            })?)?;
        last_commit_seq = Some(stored_artifact.commit_seq);

        for claim in &compilation.claims {
            let mut claim_request = Request::new(
                Operation::ClaimAssert,
                ClaimAssertInput {
                    claim_type: claim.claim_type,
                    statement: claim.statement.clone(),
                    confidence: None,
                    evidence: claim
                        .evidence
                        .iter()
                        .map(|evidence| EvidenceLocator {
                            artifact_id: stored_artifact.value.artifact_id.clone(),
                            revision_id: stored_artifact.value.revision_id.clone(),
                            start_byte: Some(evidence.start_byte),
                            end_byte: Some(evidence.end_byte),
                        })
                        .collect(),
                    valid_from: None,
                    valid_until: None,
                    actor: Some(actor.clone()),
                },
            )?;
            claim_request.context = Some(resolved.context.clone());
            claim_request.context_source = resolved.source.clone();
            // The durable identity is the exact source passage, not a model's
            // potentially variable type or paraphrase. A changed compilation
            // for the same span therefore conflicts instead of duplicating it.
            let claim_identity = format!(
                "claim-spans:{}",
                claim
                    .evidence
                    .iter()
                    .map(|evidence| format!("{}-{}", evidence.start_byte, evidence.end_byte))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            claim_request.idempotency_key =
                Some(remember_idempotency(&logical_operation, &claim_identity));
            let claim_response = dispatch(&endpoint, &claim_request, autostart, paths).await?;
            if !claim_response.ok {
                let code = response_exit_code(&claim_response);
                print_json(&claim_response, pretty);
                return Ok(code);
            }
            warnings.extend(claim_response.warnings);
            let stored_claim: MutationResult<ClaimRecord> =
                serde_json::from_value(claim_response.result.ok_or_else(|| {
                    MemoryError::Integrity(
                        "claim.assert succeeded without a mutation result".to_owned(),
                    )
                })?)?;
            last_commit_seq = Some(stored_claim.commit_seq);
            stored_claims.push(stored_claim);
        }
        artifact = Some(stored_artifact);
    }

    let request = Request::new(Operation::ArtifactPut, json!({}))?;
    let mut response = Response::success(
        &request,
        RememberResult {
            applied: args.apply,
            input_digest: digest,
            compiler,
            plan,
            artifact,
            stored_claims,
        },
    )?;
    response.context = Some(resolved.protocol_context());
    response.commit_seq = last_commit_seq;
    response.warnings = warnings;
    print_json(&response, pretty);
    Ok(0)
}

fn remember_source_capture(args: &RememberArgs) -> &'static str {
    if args.file.is_some() {
        "file_snapshot"
    } else if args.text.as_slice() == ["-"] {
        "stdin"
    } else {
        "inline"
    }
}

fn remember_quality(
    source_capture: &'static str,
    raw: bool,
    claims: &[ValidatedClaim],
) -> RememberQualityReport {
    let mut findings = Vec::new();
    if !raw && !claims.is_empty() && source_capture != "file_snapshot" {
        findings.push(RememberQualityFinding {
            code: "REMEMBER_SELF_ATTESTED_SOURCE",
            severity: "warning",
            message: "The claim basis is only this inline/stdin note. If it synthesizes external material, preserve only the relevant primary artifacts and link the summary to them.",
            claim_indexes: Vec::new(),
        });
    }

    let observation_indexes = claims
        .iter()
        .enumerate()
        .filter_map(|(index, claim)| {
            matches!(claim.claim_type, ClaimType::Observation).then_some(index + 1)
        })
        .collect::<Vec<_>>();
    if !observation_indexes.is_empty() {
        findings.push(RememberQualityFinding {
            code: "REMEMBER_MUTABLE_OBSERVATION",
            severity: "warning",
            message: "Observation claims created by remember have no validity window. Use explicit claim.assert with --valid-until when expiry is known, or revise, retract, or supersede them when verified state changes.",
            claim_indexes: observation_indexes,
        });
    }

    if !claims.is_empty() {
        findings.push(RememberQualityFinding {
            code: "REMEMBER_RELATIONS_NOT_CREATED",
            severity: "info",
            message: "memoree remember deliberately creates no graph relations. Add derived-from, references, or supports links only when they improve provenance or navigation.",
            claim_indexes: Vec::new(),
        });
    }

    RememberQualityReport {
        evidence_basis: "remember_source_revision",
        source_capture,
        requires_review: findings.iter().any(|finding| finding.severity == "warning"),
        findings,
    }
}

fn read_remember_source(args: &RememberArgs) -> Result<(String, String)> {
    let (bytes, source_label) = if let Some(path) = &args.file {
        let canonical = fs::canonicalize(path)?;
        if fs::metadata(&canonical)?.len() > MAX_ARTIFACT_BYTES as u64 {
            return Err(MemoryError::ContentTooLarge);
        }
        (fs::read(&canonical)?, canonical.display().to_string())
    } else if args.text.as_slice() == ["-"] {
        let mut bytes = Vec::new();
        io::stdin()
            .take((MAX_ARTIFACT_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        (bytes, "stdin".to_owned())
    } else if args.text.is_empty() {
        return Err(MemoryError::InvalidRequest(
            "provide text, `-` for stdin, or `--file PATH`".to_owned(),
        ));
    } else {
        (args.text.join(" ").into_bytes(), "inline".to_owned())
    };
    if bytes.len() > MAX_ARTIFACT_BYTES {
        return Err(MemoryError::ContentTooLarge);
    }
    let source = String::from_utf8(bytes).map_err(|_| {
        MemoryError::InvalidRequest(
            "memoree remember accepts UTF-8 text; use artifact.put for binary files".to_owned(),
        )
    })?;
    Ok((source, source_label))
}

fn validate_remember_kind(kind: &str) -> Result<()> {
    if kind.is_empty()
        || kind.len() > 128
        || !kind
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(MemoryError::InvalidRequest(
            "remember kind must contain 1..=128 ASCII letters, digits, `_`, `-`, or `.`".to_owned(),
        ));
    }
    Ok(())
}

fn remember_idempotency(logical_operation: &str, part: &str) -> String {
    let identity = format!("{logical_operation}\0{part}");
    format!("remember_{}", blake3::hash(identity.as_bytes()).to_hex())
}

fn attach_ambient_context(request: &mut Request) -> Result<()> {
    if request.context.is_some() || !request.op.needs_context() {
        if request.context.is_some() && matches!(request.context_source, ContextSource::None) {
            request.context_source = ContextSource::Explicit;
        }
        return Ok(());
    }
    let resolved = ContextResolver::new(env::current_dir()?)?.resolve()?;
    request.context = Some(resolved.context);
    request.context_source = resolved.source;
    Ok(())
}

fn default_idempotency(request: &Request) -> Result<String> {
    Ok(format!("auto_{}", request.request_id))
}

async fn dispatch(
    endpoint: &Endpoint,
    request: &Request,
    autostart: bool,
    paths: &AppPaths,
) -> Result<Response> {
    match probe_daemon(endpoint).await {
        Ok(doctor_response) => {
            require_matching_daemon(&doctor_response, endpoint)?;
            transport::request(endpoint, request).await
        }
        Err(_) if autostart => {
            ensure_upgrade_not_in_progress(paths)?;
            let mut child = start_daemon(endpoint, paths)?;
            let doctor_response = wait_for_daemon(endpoint, &mut child).await?;
            require_matching_daemon(&doctor_response, endpoint)?;
            transport::request(endpoint, request).await
        }
        Err(error) => Err(error),
    }
}

fn require_matching_daemon(response: &Response, endpoint: &Endpoint) -> Result<DoctorResult> {
    let doctor = doctor_result(response)?;
    let current = env!("CARGO_PKG_VERSION");
    if doctor.binary_version == current {
        return Ok(doctor);
    }
    let running = if doctor.binary_version.is_empty() {
        "a legacy daemon that does not report its version".to_owned()
    } else {
        format!("daemon version {}", doctor.binary_version)
    };
    Err(MemoryError::Config(format!(
        "{running} is still serving {}; this CLI is {current}. Run `memoree upgrade apply` for the default local daemon, or restart the supervisor that owns an explicit endpoint",
        endpoint.display()
    )))
}

fn start_daemon(endpoint: &Endpoint, paths: &AppPaths) -> Result<Child> {
    create_private_directory(&paths.data_dir)?;
    create_private_directory(&paths.runtime_dir)?;
    let log_path = paths.data_dir.join("memoreed.log");
    let stdout = open_private_append_file(&log_path)?;
    let stderr = stdout.try_clone()?;
    let mut command = ProcessCommand::new(env::current_exe()?);
    command
        .arg("serve")
        .arg("--listen")
        .arg(endpoint.display())
        .arg("--data-dir")
        .arg(&paths.data_dir)
        .arg("--lifecycle-owner")
        .arg("memoree")
        .env(DAEMON_CHILD_ENV, "1")
        .current_dir(&paths.data_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;

        // SAFETY: `setsid` is async-signal-safe and the closure performs no
        // application state access between fork and exec. A separate session
        // keeps the auto-started daemon alive when the invoking terminal exits.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
    }
    let child = command
        .spawn()
        .map_err(|error| MemoryError::Transport(format!("failed to start daemon: {error}")))?;
    Ok(child)
}

async fn daemon_command(
    command: DaemonCommands,
    endpoint_override: Option<&str>,
    paths: &AppPaths,
    pretty: bool,
) -> Result<i32> {
    let endpoint = resolve_endpoint(endpoint_override, paths)?;
    match command {
        DaemonCommands::Status => match probe_daemon(&endpoint).await {
            Ok(response) => {
                let code = response_exit_code(&response);
                print_json(&response, pretty);
                Ok(code)
            }
            Err(error) if daemon_is_unreachable(&error) => {
                let request = Request::new(Operation::Doctor, json!({}))?;
                let response = Response::success(
                    &request,
                    json!({
                        "status": "stopped",
                        "running": false,
                        "endpoint": endpoint.display(),
                    }),
                )?;
                print_json(&response, pretty);
                Ok(1)
            }
            Err(error) => Err(error),
        },
        DaemonCommands::Stop | DaemonCommands::Restart => {
            let default_endpoint = resolve_endpoint(None, paths)?;
            if endpoint != default_endpoint {
                return Err(MemoryError::InvalidRequest(
                    "daemon stop/restart controls only the default private Unix endpoint; use the owning supervisor for an explicit endpoint"
                        .into(),
                ));
            }
            let stopped_pid = stop_local_daemon(&endpoint).await?;
            if matches!(command, DaemonCommands::Stop) {
                let request = Request::new(Operation::Doctor, json!({}))?;
                let response = Response::success(
                    &request,
                    json!({
                        "status": "stopped",
                        "running": false,
                        "already_stopped": stopped_pid.is_none(),
                        "stopped_pid": stopped_pid,
                        "endpoint": endpoint.display(),
                    }),
                )?;
                print_json(&response, pretty);
                return Ok(0);
            }

            let mut child = start_daemon(&endpoint, paths)?;
            let response = wait_for_daemon(&endpoint, &mut child).await?;
            print_json(&response, pretty);
            Ok(response_exit_code(&response))
        }
    }
}

async fn probe_daemon(endpoint: &Endpoint) -> Result<Response> {
    let request = Request::new(Operation::Doctor, json!({}))?;
    transport::request(endpoint, &request).await
}

fn daemon_is_unreachable(error: &MemoryError) -> bool {
    matches!(error, MemoryError::Transport(_) | MemoryError::Io(_))
}

fn doctor_result(response: &Response) -> Result<DoctorResult> {
    if !response.ok {
        let message = response
            .error
            .as_ref()
            .map(|error| error.message.as_str())
            .unwrap_or("daemon doctor returned an invalid failure envelope");
        return Err(MemoryError::Transport(message.to_owned()));
    }
    let value = response
        .result
        .clone()
        .ok_or_else(|| MemoryError::Transport("daemon doctor returned no result".into()))?;
    serde_json::from_value(value)
        .map_err(|error| MemoryError::Transport(format!("invalid daemon doctor result: {error}")))
}

async fn stop_local_daemon(endpoint: &Endpoint) -> Result<Option<u32>> {
    let response = match probe_daemon(endpoint).await {
        Ok(response) => response,
        Err(error) if daemon_is_unreachable(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    let doctor = doctor_result(&response)?;
    if !doctor.running || doctor.daemon_pid <= 1 {
        return Err(MemoryError::Transport(format!(
            "daemon reported unsafe process id {}",
            doctor.daemon_pid
        )));
    }

    #[cfg(unix)]
    {
        let pid = i32::try_from(doctor.daemon_pid)
            .map_err(|_| MemoryError::Transport("daemon process id is out of range".into()))?;
        // SAFETY: `pid` came from a successful doctor response over the
        // default user-private Unix socket. SIGTERM is handled by the daemon,
        // which removes its socket and releases its locks before exiting.
        let result = unsafe { libc::kill(pid, libc::SIGTERM) };
        if result == -1 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(MemoryError::Io(error));
            }
        }
    }
    #[cfg(not(unix))]
    {
        return Err(MemoryError::InvalidRequest(
            "daemon stop/restart is not implemented on this platform".into(),
        ));
    }

    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        match probe_daemon(endpoint).await {
            Err(error) if daemon_is_unreachable(&error) => {
                #[cfg(unix)]
                if !unix_process_is_alive(doctor.daemon_pid)? {
                    return Ok(Some(doctor.daemon_pid));
                }
            }
            Err(error) => return Err(error),
            Ok(response) => {
                let current = doctor_result(&response)?;
                if current.daemon_pid != doctor.daemon_pid {
                    return Err(MemoryError::Transport(format!(
                        "a different daemon process {} appeared while stopping {}",
                        current.daemon_pid, doctor.daemon_pid
                    )));
                }
            }
        }
    }
    Err(MemoryError::Transport(format!(
        "daemon process {} did not stop within 5 seconds",
        doctor.daemon_pid
    )))
}

#[cfg(unix)]
fn unix_process_is_alive(pid: u32) -> Result<bool> {
    let pid = i32::try_from(pid)
        .map_err(|_| MemoryError::Transport("daemon process id is out of range".into()))?;
    // SAFETY: signal 0 performs existence/permission checking only.
    if unsafe { libc::kill(pid, 0) } == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ESRCH) => Ok(false),
        Some(libc::EPERM) => Ok(true),
        _ => Err(MemoryError::Io(error)),
    }
}

async fn wait_for_daemon(endpoint: &Endpoint, child: &mut Child) -> Result<Response> {
    let deadline = Instant::now() + DAEMON_START_TIMEOUT;
    let mut last_error = match probe_daemon(endpoint).await {
        Ok(response) => {
            let doctor = doctor_result(&response)?;
            if doctor.running {
                return Ok(response);
            }
            MemoryError::Transport("daemon reported running=false".into())
        }
        Err(error) => error,
    };
    loop {
        if let Some(status) = child.try_wait()? {
            return Err(MemoryError::Transport(format!(
                "daemon exited during startup with {status}"
            )));
        }
        if Instant::now() >= deadline {
            return Err(MemoryError::Transport(format!(
                "daemon did not become ready within {} seconds: {last_error}",
                DAEMON_START_TIMEOUT.as_secs()
            )));
        }
        tokio::time::sleep(DAEMON_START_POLL_INTERVAL).await;
        match probe_daemon(endpoint).await {
            Ok(response) => {
                let doctor = doctor_result(&response)?;
                if doctor.running {
                    return Ok(response);
                }
                last_error = MemoryError::Transport("daemon reported running=false".into());
            }
            Err(error) => last_error = error,
        }
    }
}

async fn serve_daemon(args: ServeArgs, paths: &AppPaths) -> Result<i32> {
    let data_dir = args.data_dir.unwrap_or_else(|| paths.data_dir.clone());
    create_private_directory(&data_dir)?;
    let lock_path = data_dir.join("memoreed.lock");
    let lock = open_private_lock_file(&lock_path)?;
    lock.try_lock_exclusive().map_err(|error| {
        MemoryError::Transport(format!(
            "another Memoree daemon owns {}: {error}",
            lock_path.display()
        ))
    })?;
    let endpoint = match args.listen {
        Some(value) => Endpoint::parse(&value)?,
        None => {
            create_private_directory(&paths.runtime_dir)?;
            Endpoint::Unix(paths.socket_path.clone())
        }
    };
    let service = Arc::new(MemoryService::with_lifecycle_owner(
        Store::open(&data_dir)?,
        args.lifecycle_owner.as_str(),
    ));
    transport::serve(
        endpoint,
        service,
        transport::ServePolicy {
            dangerously_allow_non_loopback_tcp: args.dangerously_allow_non_loopback_tcp,
        },
    )
    .await?;
    drop(lock);
    Ok(0)
}

fn create_private_directory(path: &Path) -> Result<()> {
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
                let is_memoree_directory = [
                    // macOS places data, configuration, and the fallback
                    // runtime directory under the same ProjectDirs root. A
                    // previous release may therefore have created only these
                    // layout entries before directory hardening was added.
                    "run",
                    "config.toml",
                    MEMOREE_DATABASE_FILE,
                    "memoreed.lock",
                    "memoree.sock",
                    "memoree.sock.lock",
                ]
                .iter()
                .any(|name| path.join(name).exists());
                if is_empty || is_memoree_directory {
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

fn open_private_lock_file(path: &Path) -> Result<std::fs::File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    open_private_file(&mut options, path)
}

fn open_private_append_file(path: &Path) -> Result<std::fs::File> {
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    open_private_file(&mut options, path)
}

fn open_private_file(options: &mut OpenOptions, path: &Path) -> Result<std::fs::File> {
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

fn session_exec(
    args: SessionExecArgs,
    endpoint: Option<&str>,
    no_autostart: bool,
    paths: &AppPaths,
) -> Result<i32> {
    let resolved = ContextResolver::new(env::current_dir()?)?.resolve()?;
    let task_id = stable_task_id(&resolved.context, &args.task);
    let context = task_context(&resolved.context, task_id)?;
    let encoded = encode_memory_context(&context)?;
    let (program, arguments) = args
        .command
        .split_first()
        .ok_or_else(|| MemoryError::InvalidRequest("session command is empty".into()))?;
    let mut command = ProcessCommand::new(program);
    command.args(arguments).env(MEMOREE_CONTEXT_ENV, encoded);
    if let Some(endpoint) = endpoint {
        command.env(
            ENDPOINT_ENV,
            resolve_endpoint(Some(endpoint), paths)?.display(),
        );
    }
    if no_autostart {
        command.env(NO_AUTOSTART_ENV, "true");
    }
    let status = command.status()?;
    Ok(status.code().unwrap_or(1))
}

fn stable_task_id(context: &AmbientContext, task: &str) -> String {
    let input = format!("{}:{}:{task}", context.workspace_id, context.project_id);
    let hash = blake3::hash(input.as_bytes()).to_hex().to_string();
    format!("tsk_{}", &hash[..20])
}

fn resolve_endpoint(value: Option<&str>, paths: &AppPaths) -> Result<Endpoint> {
    match value {
        Some(value) => Endpoint::parse(value),
        None => Ok(Endpoint::Unix(paths.socket_path.clone())),
    }
}

fn read_artifact(
    path: &str,
    media_type: Option<String>,
) -> Result<(ArtifactContent, String, String)> {
    let (bytes, source) = if path == "-" {
        let mut bytes = Vec::new();
        io::stdin()
            .take((MAX_ARTIFACT_BYTES + 1) as u64)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_ARTIFACT_BYTES {
            return Err(MemoryError::ContentTooLarge);
        }
        (bytes, "stdin".to_owned())
    } else {
        let path = fs::canonicalize(path)?;
        if fs::metadata(&path)?.len() > MAX_ARTIFACT_BYTES as u64 {
            return Err(MemoryError::ContentTooLarge);
        }
        let bytes = fs::read(&path)?;
        if bytes.len() > MAX_ARTIFACT_BYTES {
            return Err(MemoryError::ContentTooLarge);
        }
        (bytes, path.display().to_string())
    };
    let guessed = media_type.unwrap_or_else(|| {
        if path == "-" {
            "text/plain; charset=utf-8".to_owned()
        } else {
            mime_guess::from_path(path)
                .first_raw()
                .unwrap_or("application/octet-stream")
                .to_owned()
        }
    });
    let content = match String::from_utf8(bytes) {
        Ok(text) if json_string_encoded_len(&text) <= MAX_ENCODED_CONTENT_BYTES => {
            ArtifactContent::Text(text)
        }
        Ok(text) => ArtifactContent::Base64(BASE64.encode(text.into_bytes())),
        Err(error) => ArtifactContent::Base64(BASE64.encode(error.into_bytes())),
    };
    Ok((content, guessed, source))
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

fn parse_evidence(values: &[String]) -> Result<Vec<EvidenceLocator>> {
    values
        .iter()
        .map(|value| {
            let (artifact_id, revision_and_span) = value.split_once('@').ok_or_else(|| {
                MemoryError::InvalidRequest(format!(
                    "evidence must be ARTIFACT_ID@REVISION_ID[#START-END]: {value}"
                ))
            })?;
            if artifact_id.is_empty() || revision_and_span.is_empty() {
                return Err(MemoryError::InvalidRequest(format!(
                    "evidence contains an empty artifact or revision id: {value}"
                )));
            }
            let (revision_id, span) = match revision_and_span.split_once('#') {
                Some((revision_id, span)) if !revision_id.is_empty() && !span.is_empty() => {
                    (revision_id, Some(span))
                }
                Some(_) => {
                    return Err(MemoryError::InvalidRequest(format!(
                        "evidence contains an empty revision id or byte span: {value}"
                    )));
                }
                None => (revision_and_span, None),
            };
            let (start_byte, end_byte) = match span {
                Some(span) => {
                    let (start, end) = span.split_once('-').ok_or_else(|| {
                        MemoryError::InvalidRequest(format!(
                            "evidence byte span must be START-END: {value}"
                        ))
                    })?;
                    let start = start.parse::<u64>().map_err(|_| {
                        MemoryError::InvalidRequest(format!(
                            "evidence start byte is not an unsigned integer: {value}"
                        ))
                    })?;
                    let end = end.parse::<u64>().map_err(|_| {
                        MemoryError::InvalidRequest(format!(
                            "evidence end byte is not an unsigned integer: {value}"
                        ))
                    })?;
                    if start >= end {
                        return Err(MemoryError::InvalidRequest(format!(
                            "evidence byte span must satisfy START < END: {value}"
                        )));
                    }
                    (Some(start), Some(end))
                }
                None => (None, None),
            };
            Ok(EvidenceLocator {
                artifact_id: artifact_id.to_owned(),
                revision_id: revision_id.to_owned(),
                start_byte,
                end_byte,
            })
        })
        .collect()
}

fn parse_entity_ref(value: &str) -> Result<(EntityType, String)> {
    let (kind, id) = value.split_once(':').ok_or_else(|| {
        MemoryError::InvalidRequest(format!(
            "entity reference must be artifact:ID or claim:ID: {value}"
        ))
    })?;
    if id.trim().is_empty() {
        return Err(MemoryError::InvalidRequest("entity id is empty".into()));
    }
    let kind = match kind {
        "artifact" => EntityType::Artifact,
        "claim" => EntityType::Claim,
        _ => {
            return Err(MemoryError::InvalidRequest(format!(
                "unknown entity type {kind}"
            )));
        }
    };
    Ok((kind, id.to_owned()))
}

fn materialize_artifact(response: &mut Response, path: &Path, force: bool) -> Result<()> {
    let result = response
        .result
        .as_mut()
        .ok_or_else(|| MemoryError::Integrity("artifact response has no result".into()))?;
    let content = result
        .get("content")
        .cloned()
        .ok_or_else(|| MemoryError::Integrity("artifact response has no content".into()))?;
    let content: ArtifactContent = serde_json::from_value(content)?;
    let bytes = match content {
        ArtifactContent::Text(text) => text.into_bytes(),
        ArtifactContent::Base64(encoded) => BASE64
            .decode(encoded)
            .map_err(|error| MemoryError::Integrity(format!("invalid base64 content: {error}")))?,
    };
    if path.exists() && !force {
        return Err(MemoryError::InvalidRequest(format!(
            "output already exists (pass --force to replace it): {}",
            path.display()
        )));
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    temporary.write_all(&bytes)?;
    temporary.as_file_mut().sync_all()?;
    if force {
        temporary
            .persist(path)
            .map_err(|error| MemoryError::Io(error.error))?;
    } else {
        temporary
            .persist_noclobber(path)
            .map_err(|error| MemoryError::Io(error.error))?;
    }
    if let Some(object) = result.as_object_mut() {
        object.remove("content");
        object.insert(
            "materialized_path".into(),
            Value::String(path.display().to_string()),
        );
    }
    Ok(())
}

fn response_exit_code(response: &Response) -> i32 {
    let Some(error) = &response.error else {
        return 0;
    };
    use memoree::protocol::ErrorCode;
    match error.code {
        ErrorCode::InvalidRequest
        | ErrorCode::ConfigError
        | ErrorCode::ContentTooLarge
        | ErrorCode::UnsupportedVersion => 2,
        ErrorCode::NotFound => 3,
        ErrorCode::RevisionConflict | ErrorCode::IdempotencyConflict => 4,
        ErrorCode::NoAmbientContext => 5,
        ErrorCode::IndexNotReady => 6,
        ErrorCode::ScopeViolation => 7,
        _ => 1,
    }
}

fn print_json(value: &impl Serialize, pretty: bool) {
    let rendered = if pretty {
        serde_json::to_string_pretty(value)
    } else {
        serde_json::to_string(value)
    };
    match rendered {
        Ok(rendered) => println!("{rendered}"),
        Err(error) => println!(
            "{{\"v\":1,\"ok\":false,\"error\":{{\"code\":\"INTERNAL_ERROR\",\"message\":{}}}}}",
            serde_json::to_string(&error.to_string())
                .unwrap_or_else(|_| "\"serialization failed\"".into())
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evidence_parser_preserves_exact_byte_spans() {
        let evidence = parse_evidence(&["art_1@arev_2#12-34".into()]).unwrap();
        assert_eq!(evidence[0].artifact_id, "art_1");
        assert_eq!(evidence[0].revision_id, "arev_2");
        assert_eq!(evidence[0].start_byte, Some(12));
        assert_eq!(evidence[0].end_byte, Some(34));
    }

    #[test]
    fn evidence_parser_rejects_empty_or_reversed_spans() {
        assert!(parse_evidence(&["art_1@arev_2#".into()]).is_err());
        assert!(parse_evidence(&["art_1@arev_2#34-12".into()]).is_err());
        assert!(parse_evidence(&["@arev_2".into()]).is_err());
    }

    #[test]
    fn claim_history_cli_builds_a_typed_paginated_read_request() {
        let cli = Cli::try_parse_from([
            "memoree",
            "claim",
            "history",
            "clm_1",
            "--limit",
            "7",
            "--before-revision-number",
            "4",
        ])
        .unwrap();
        let prepared = prepare_command(cli.command).unwrap();
        assert!(matches!(prepared.request.op, Operation::ClaimHistory));
        assert!(prepared.request.idempotency_key.is_none());
        let input: ClaimHistoryInput = serde_json::from_value(prepared.request.input).unwrap();
        assert_eq!(input.claim_id, "clm_1");
        assert_eq!(input.limit, 7);
        assert_eq!(input.before_revision_number, Some(4));
    }

    #[test]
    fn conflict_list_cli_builds_a_scoped_paginated_read_request() {
        let cli = Cli::try_parse_from([
            "memoree",
            "conflict",
            "list",
            "--include-stale",
            "--horizon",
            "workspace",
            "--reason",
            "review shared contradictions",
            "--limit",
            "7",
            "--before-case-sequence",
            "42",
        ])
        .unwrap();
        let prepared = prepare_command(cli.command).unwrap();
        assert!(matches!(prepared.request.op, Operation::ConflictList));
        assert!(prepared.request.idempotency_key.is_none());
        let input: ConflictListInput = serde_json::from_value(prepared.request.input).unwrap();
        assert!(matches!(input.horizon, Horizon::Workspace));
        assert_eq!(
            input.reason.as_deref(),
            Some("review shared contradictions")
        );
        assert!(input.include_stale);
        assert_eq!(input.limit, 7);
        assert_eq!(input.before_case_sequence, Some(42));
    }

    #[test]
    fn recall_cli_builds_a_claim_first_bounded_read_request() {
        let cli = Cli::try_parse_from([
            "memoree",
            "recall",
            "storage",
            "decision",
            "--max-claims",
            "4",
            "--max-artifact-refs",
            "2",
            "--max-excerpt-bytes",
            "240",
        ])
        .unwrap();
        let prepared = prepare_command(cli.command).unwrap();
        assert!(matches!(prepared.request.op, Operation::MemoryRecall));
        assert!(prepared.request.idempotency_key.is_none());
        let input: RecallInput = serde_json::from_value(prepared.request.input).unwrap();
        assert_eq!(input.query, "storage decision");
        assert!(matches!(input.horizon, Horizon::Ambient));
        assert_eq!(input.max_claims, 4);
        assert_eq!(input.max_artifact_refs, 2);
        assert_eq!(input.max_excerpt_bytes, 240);
    }

    #[test]
    fn citation_get_cli_builds_a_context_free_bounded_read_request() {
        let cli = Cli::try_parse_from([
            "memoree",
            "citation",
            "get",
            "memoree://artifact/art_1@arev_2#12-34",
            "--max-bytes",
            "1024",
        ])
        .unwrap();
        let prepared = prepare_command(cli.command).unwrap();
        assert!(matches!(prepared.request.op, Operation::CitationGet));
        assert!(prepared.request.context.is_none());
        let input: CitationGetInput = serde_json::from_value(prepared.request.input).unwrap();
        assert_eq!(input.citation, "memoree://artifact/art_1@arev_2#12-34");
        assert_eq!(input.max_bytes, 1024);
    }

    #[test]
    fn probe_cli_uses_the_single_depth_eight_default() {
        let cli = Cli::try_parse_from(["memoree", "probe", "saved", "workspace"]).unwrap();
        let prepared = prepare_command(cli.command).unwrap();
        assert!(matches!(prepared.request.op, Operation::MemoryProbe));
        let input: ProbeInput = serde_json::from_value(prepared.request.input).unwrap();
        assert_eq!(input.query, "saved workspace");
        assert_eq!(input.max_leads, 8);
    }

    #[test]
    fn claim_assert_cli_preserves_explicit_validity_window() {
        let cli = Cli::try_parse_from([
            "memoree",
            "claim",
            "assert",
            "observation",
            "Checkout terms are draft.",
            "--valid-from",
            "2026-07-17T00:00:00Z",
            "--valid-until",
            "2026-08-01T00:00:00Z",
        ])
        .unwrap();
        let prepared = prepare_command(cli.command).unwrap();
        let input: ClaimAssertInput = serde_json::from_value(prepared.request.input).unwrap();
        assert_eq!(
            input.valid_from.unwrap().to_rfc3339(),
            "2026-07-17T00:00:00+00:00"
        );
        assert_eq!(
            input.valid_until.unwrap().to_rfc3339(),
            "2026-08-01T00:00:00+00:00"
        );
    }

    #[test]
    fn remember_quality_exposes_summary_and_observation_risks() {
        let claims = vec![ValidatedClaim {
            claim_type: ClaimType::Observation,
            statement: "Checkout terms are draft.".to_owned(),
            evidence: Vec::new(),
        }];
        let quality = remember_quality("inline", false, &claims);
        assert!(quality.requires_review);
        assert_eq!(quality.source_capture, "inline");
        assert!(
            quality
                .findings
                .iter()
                .any(|finding| finding.code == "REMEMBER_SELF_ATTESTED_SOURCE")
        );
        assert!(quality.findings.iter().any(|finding| {
            finding.code == "REMEMBER_MUTABLE_OBSERVATION" && finding.claim_indexes == vec![1]
        }));
    }

    #[test]
    fn non_loopback_serve_opt_in_is_explicitly_dangerous() {
        let cli = Cli::try_parse_from([
            "memoree",
            "serve",
            "--listen",
            "tcp://0.0.0.0:17878",
            "--dangerously-allow-non-loopback-tcp",
        ])
        .unwrap();
        let Commands::Serve(args) = cli.command else {
            panic!("expected serve command");
        };
        assert!(args.dangerously_allow_non_loopback_tcp);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn daemon_readiness_fails_fast_when_the_child_exits() {
        let temporary = tempfile::tempdir().unwrap();
        let endpoint = Endpoint::Unix(temporary.path().join("never-created.sock"));
        let mut child = ProcessCommand::new(env::current_exe().unwrap())
            .arg("--definitely-not-a-test-harness-option")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let started = Instant::now();

        let error = wait_for_daemon(&endpoint, &mut child).await.unwrap_err();

        assert!(error.to_string().contains("exited during startup"));
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    #[cfg(unix)]
    #[test]
    fn existing_platform_layout_is_tightened_but_unrelated_directories_are_refused() {
        use std::os::unix::fs::PermissionsExt as _;

        let temporary = tempfile::tempdir().unwrap();
        let application = temporary.path().join("application");
        fs::create_dir_all(application.join("run")).unwrap();
        fs::set_permissions(&application, fs::Permissions::from_mode(0o755)).unwrap();

        create_private_directory(&application).unwrap();
        assert_eq!(
            fs::metadata(&application).unwrap().permissions().mode() & 0o777,
            0o700
        );

        let unrelated = temporary.path().join("unrelated");
        fs::create_dir(&unrelated).unwrap();
        fs::write(unrelated.join("user-file"), b"keep private policy strict").unwrap();
        fs::set_permissions(&unrelated, fs::Permissions::from_mode(0o755)).unwrap();
        let error = create_private_directory(&unrelated).unwrap_err();
        assert!(matches!(error, MemoryError::Config(_)));
        assert_eq!(
            fs::metadata(&unrelated).unwrap().permissions().mode() & 0o777,
            0o755
        );
    }
}
