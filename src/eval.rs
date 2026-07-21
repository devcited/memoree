//! Deterministic retrieval regression harness.
//!
//! The harness always creates a fresh temporary store from a versioned JSONL
//! corpus. It never reads or writes the operator's Memoree data directory.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    time::Instant,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;

use crate::{
    error::{MemoryError, Result},
    protocol::{
        AmbientContext, ArtifactContent, ArtifactGetInput, ArtifactPutInput, CitationGetInput,
        CitationGetResult, ClaimAssertInput, ClaimType, ConflictSummary, ContextBuildInput,
        ContextSource, EntityType, EvidenceLocator, Horizon, MAX_CITATION_FETCH_BYTES, Operation,
        ProbeInput, ProbeLead, ProbeResult, RecallInput, RecallPresence, RecallResult,
        RecencyBiasInput, RelationPutInput, RelationType, Request, RetrieveInput, RetrieveResult,
        SearchInput, SearchResult,
    },
    service::MemoryService,
    store::{ArtifactRecord, ClaimRecord, MutationResult, RelationRecord, Store},
};

const EVAL_SCHEMA_VERSION: u32 = 2;
const MIN_EVAL_SCHEMA_VERSION: u32 = 1;
const CANDIDATE_POOL_PRIMARY_K: usize = 16;
const CANDIDATE_POOL_DIAGNOSTIC_K: usize = 32;
/// Evaluation wire budgets are conservatively rounded per response. This
/// absorbs volatile timestamp-serialization precision without ever
/// understating the bytes a caller would receive.
const EVAL_WIRE_BUDGET_BLOCK_BYTES: usize = 64;

const EVAL_SLICES: &[&str] = &[
    "correctness",
    "exact_identifier",
    "lexical",
    "paraphrase",
    "vague_intent",
    "typo_abbreviation",
    "noisy_distractors",
    "long_document",
    "lifecycle_currentness",
    "conflict",
    "multi_hop",
    "ambient_scope",
    "explicit_broadening",
    "honest_none",
];

const EVAL_TAGS: &[&str] = &[
    "single_memory",
    "multi_memory",
    "natural_query",
    "operator_query",
    "hard_negative",
    "temporal",
    "scoped",
    "long_context",
];

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvalContext {
    workspace_id: String,
    project_id: String,
    #[serde(default)]
    task_id: Option<String>,
    #[serde(default)]
    component: Option<String>,
}

impl EvalContext {
    fn ambient(&self) -> AmbientContext {
        AmbientContext {
            workspace_id: self.workspace_id.clone(),
            project_id: self.project_id.clone(),
            task_id: self.task_id.clone(),
            component: self.component.clone(),
            pins: vec![],
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct SeedRecord {
    schema_version: u32,
    label: String,
    context: EvalContext,
    #[serde(flatten)]
    item: SeedItem,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "entity", rename_all = "snake_case", deny_unknown_fields)]
enum SeedItem {
    Artifact {
        kind: String,
        title: String,
        #[serde(default = "default_media_type")]
        media_type: String,
        content: ArtifactContent,
        #[serde(default)]
        content_repeat: Option<SeedContentRepeat>,
    },
    Claim {
        claim_type: ClaimType,
        statement: String,
        #[serde(default)]
        evidence: Vec<SeedEvidence>,
        #[serde(default)]
        valid_from: Option<DateTime<Utc>>,
        #[serde(default)]
        valid_until: Option<DateTime<Utc>>,
    },
    Relation {
        source_label: String,
        relation: RelationType,
        target_label: String,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SeedContentRepeat {
    unit: String,
    count: usize,
    #[serde(default)]
    suffix: String,
}

fn default_media_type() -> String {
    "text/plain; charset=utf-8".into()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SeedEvidence {
    artifact_label: String,
    #[serde(default)]
    quote: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CaseGate {
    #[default]
    Hard,
    Report,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvalCase {
    schema_version: u32,
    case_id: String,
    #[serde(default = "default_eval_slice")]
    slice: String,
    #[serde(default)]
    tags: Vec<String>,
    context: EvalContext,
    query: String,
    #[serde(default)]
    horizon: Horizon,
    #[serde(default)]
    reason: Option<String>,
    #[serde(default = "default_max_claims")]
    max_claims: usize,
    #[serde(default = "default_max_artifact_refs")]
    max_artifact_refs: usize,
    #[serde(default = "default_max_excerpt_bytes")]
    max_excerpt_bytes: usize,
    #[serde(default = "default_context_bytes")]
    max_context_bytes: usize,
    expected_presence: RecallPresence,
    #[serde(default)]
    relevant_claims: Vec<String>,
    #[serde(default)]
    relevant_artifacts: Vec<String>,
    #[serde(default)]
    helpful_claims: Vec<String>,
    #[serde(default)]
    helpful_artifacts: Vec<String>,
    #[serde(default)]
    forbidden: Vec<String>,
    #[serde(default)]
    expected_conflicts: Vec<[String; 2]>,
    /// Require every directly relevant returned artifact to carry an exact
    /// immutable byte span rather than a revision-only citation.
    #[serde(default)]
    require_artifact_spans: bool,
    #[serde(default)]
    gate: CaseGate,
    /// Whether useful direct evidence exists inside the allowed horizon. V1
    /// cases derive this from the direct gold labels.
    #[serde(default)]
    answerable: Option<bool>,
    #[serde(default)]
    provenance: Option<EvalCaseProvenance>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeRecoverySuite {
    schema_version: u32,
    evaluator: String,
    evaluated_at: String,
    lead_depth: usize,
    max_fetches: usize,
    cases: Vec<ProbeRecoveryCase>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProbeRecoveryVerdict {
    Supported,
    Abstain,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProbeRecoveryCase {
    case_id: String,
    probe_query: String,
    selected_sources: Vec<String>,
    verdict: ProbeRecoveryVerdict,
    refined_query: Option<String>,
}

impl EvalCase {
    fn is_answerable(&self) -> bool {
        self.answerable
            .unwrap_or(!self.relevant_claims.is_empty() || !self.relevant_artifacts.is_empty())
    }
}

fn default_eval_slice() -> String {
    "correctness".into()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvalCaseProvenance {
    source: EvalCaseSource,
    author: String,
    labeled_at: DateTime<Utc>,
    #[serde(default)]
    second_label: Option<String>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum EvalCaseSource {
    Synthetic,
    AuthoredRealistic,
    AnonymizedFailure,
}

fn default_max_claims() -> usize {
    5
}

fn default_max_artifact_refs() -> usize {
    3
}

fn default_max_excerpt_bytes() -> usize {
    320
}

fn default_context_bytes() -> usize {
    4096
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct EvalBaseline {
    schema_version: u32,
    corpus_version: String,
    #[serde(default)]
    macro_claim_recall: Option<f64>,
    #[serde(default)]
    macro_claim_precision: Option<f64>,
    #[serde(default)]
    macro_artifact_recall: Option<f64>,
    #[serde(default)]
    macro_artifact_precision: Option<f64>,
    #[serde(default)]
    max_false_answer_rate: Option<f64>,
    #[serde(default)]
    max_false_abstain_rate: Option<f64>,
    #[serde(default)]
    max_forbidden_returns: Option<usize>,
    #[serde(default = "default_epsilon")]
    epsilon: f64,
}

fn default_epsilon() -> f64 {
    0.02
}

#[derive(Debug, Clone)]
struct SeededEntity {
    entity_type: EntityType,
    entity_id: String,
    revision_id: String,
    context: AmbientContext,
    text: Option<String>,
}

/// Bounded controls for one isolated retrieval evaluation run.
#[derive(Debug, Clone, Serialize)]
pub struct EvalOptions {
    pub recovery_only: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub case_id: Option<String>,
    pub case_timeout_ms: u64,
    pub suite_timeout_ms: u64,
    pub jobs: usize,
    pub prewarm_models: bool,
}

impl Default for EvalOptions {
    fn default() -> Self {
        Self {
            recovery_only: false,
            case_id: None,
            case_timeout_ms: 60_000,
            suite_timeout_ms: 600_000,
            jobs: 1,
            prewarm_models: false,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct EvalStageTiming {
    pub stage: String,
    pub duration_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct EvalCaseTiming {
    pub case_id: String,
    pub total_ms: f64,
    pub stages: Vec<EvalStageTiming>,
}

/// Privacy-safe timings: identifiers, stage names, counts, and durations only.
#[derive(Debug, Clone, Serialize)]
pub struct EvalTimings {
    pub total_ms: f64,
    pub setup: Vec<EvalStageTiming>,
    pub cases: Vec<EvalCaseTiming>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RetrievalEvalReport {
    pub schema_version: u32,
    pub corpus_version: String,
    pub isolated_temporary_store: bool,
    pub cases: Vec<RetrievalCaseReport>,
    pub aggregate: RetrievalAggregate,
    pub baseline: BaselineComparison,
    pub hard_failures: Vec<String>,
    pub requested_case_count: usize,
    pub completed_case_count: usize,
    pub complete: bool,
    pub timed_out: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timed_out_case: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timed_out_stage: Option<String>,
    pub options: EvalOptions,
    pub timings: EvalTimings,
    pub passed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RetrievalCaseReport {
    pub case_id: String,
    pub original_query: String,
    pub probe_query: String,
    pub slice: String,
    pub tags: Vec<String>,
    pub gate: String,
    pub answerable: bool,
    pub abstained: bool,
    pub presence: RecallPresence,
    pub returned_claims: Vec<String>,
    pub returned_artifacts: Vec<String>,
    pub suggested_candidate_claims: Vec<String>,
    pub suggested_candidate_artifacts: Vec<String>,
    pub candidate_suggestion_claim_recall: Option<f64>,
    pub candidate_suggestion_artifact_recall: Option<f64>,
    /// Authority-filtered, broad candidates before answerability filtering or
    /// a future cross-encoder decision, in deterministic fusion order.
    pub candidate_pool_claims: Vec<String>,
    pub candidate_pool_artifacts: Vec<String>,
    pub candidate_pool_claim_recall_at_16: Option<f64>,
    pub candidate_pool_claim_recall_at_32: Option<f64>,
    pub candidate_pool_artifact_recall_at_16: Option<f64>,
    pub candidate_pool_artifact_recall_at_32: Option<f64>,
    /// Dense similarities for all ranked search candidates, keyed by stable
    /// corpus label. Empty when semantic retrieval is disabled.
    pub semantic_scores: BTreeMap<String, f64>,
    pub probe_sources_at_5: Vec<String>,
    pub probe_sources_at_8: Vec<String>,
    pub probe_leads_at_5: Vec<ProbeLeadDiagnostic>,
    pub probe_leads_at_8: Vec<ProbeLeadDiagnostic>,
    pub probe_bait_sources_at_5: Vec<String>,
    pub probe_bait_sources_at_8: Vec<String>,
    pub probe_source_recall_at_5: Option<f64>,
    pub probe_source_recall_at_8: Option<f64>,
    pub probe_result_bytes_at_5: usize,
    pub probe_result_bytes_at_8: usize,
    pub recall_candidate_hint_bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub probe_recovery_verdict: Option<String>,
    pub probe_selected_sources: Vec<String>,
    pub probe_fetched_reference_count: usize,
    pub probe_fetched_content_bytes: usize,
    pub probe_fetched_lead_bytes: Vec<usize>,
    pub probe_fetch_response_bytes: usize,
    pub probe_full_artifact_response_bytes: usize,
    pub probe_refined_result_bytes: usize,
    pub probe_pipeline_bytes: usize,
    pub probe_artifact_get_baseline_bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub probe_large_artifact_pipeline_ratio: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub probe_citation_get_exact: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub probe_recovery_path_complete: Option<bool>,
    pub probe_recovery_path_failures: Vec<String>,
    pub probe_refined_returned_claims: Vec<String>,
    pub probe_refined_returned_artifacts: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub probe_recovery_succeeded: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub probe_recovery_false_answer: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub probe_recovery_false_abstain: Option<bool>,
    pub retrieve_presence: RecallPresence,
    pub retrieve_qualified_evidence_artifacts: Vec<String>,
    pub retrieve_recovery_artifacts: Vec<String>,
    pub retrieve_recovery_bait_artifacts: Vec<String>,
    pub retrieve_recovery_forbidden_artifacts: Vec<String>,
    pub retrieve_packet_bytes: usize,
    pub retrieve_latency_ms: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retrieve_recovery_succeeded: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retrieve_recovery_false_answer: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retrieve_recovery_false_abstain: Option<bool>,
    pub claim_recall: Option<f64>,
    pub claim_precision: f64,
    pub claim_ndcg: Option<f64>,
    pub claim_mrr: Option<f64>,
    pub artifact_recall: Option<f64>,
    pub artifact_precision: f64,
    pub artifact_ndcg: Option<f64>,
    pub artifact_mrr: Option<f64>,
    pub forbidden_returned: Vec<String>,
    pub context_used_bytes: usize,
    pub context_max_bytes: usize,
    pub failures: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProbeLeadDiagnostic {
    pub source_label: String,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_byte: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_byte: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RetrievalAggregate {
    pub case_count: usize,
    pub macro_claim_recall: f64,
    pub macro_claim_precision: f64,
    pub macro_claim_ndcg: f64,
    pub macro_artifact_recall: f64,
    pub macro_artifact_precision: f64,
    pub macro_artifact_ndcg: f64,
    pub candidate_pool_claim_recall_at_16: f64,
    pub candidate_pool_claim_recall_at_32: f64,
    pub candidate_pool_artifact_recall_at_16: f64,
    pub candidate_pool_artifact_recall_at_32: f64,
    pub candidate_suggestion_claim_recall: f64,
    pub candidate_suggestion_artifact_recall: f64,
    pub probe_source_recall_at_5: f64,
    pub probe_source_recall_at_8: f64,
    pub probe_bait_lead_rate_at_5: f64,
    pub probe_bait_lead_rate_at_8: f64,
    pub mean_probe_result_bytes_at_5: f64,
    pub max_probe_result_bytes_at_5: usize,
    pub mean_probe_result_bytes_at_8: f64,
    pub max_probe_result_bytes_at_8: usize,
    pub mean_recall_candidate_hint_bytes: f64,
    pub max_recall_candidate_hint_bytes: usize,
    pub probe_fetch_content_bytes_per_lead_p95: usize,
    pub mean_probe_pipeline_bytes: f64,
    pub max_probe_pipeline_bytes: usize,
    pub max_large_artifact_pipeline_ratio: f64,
    pub citation_get_exactness_violations: usize,
    pub probe_bait_fetches: usize,
    pub probe_recovery_case_count: usize,
    pub probe_recovery_success_rate: f64,
    pub probe_recovery_false_answer_rate: f64,
    pub probe_recovery_false_abstain_rate: f64,
    pub retrieve_recovery_case_count: usize,
    pub retrieve_recovery_success_rate: f64,
    pub retrieve_recovery_false_answer_rate: f64,
    pub retrieve_recovery_false_abstain_rate: f64,
    pub retrieve_bait_fetches: usize,
    pub retrieve_forbidden_fetches: usize,
    pub mean_retrieve_packet_bytes: f64,
    pub max_retrieve_packet_bytes: usize,
    pub retrieve_latency_p95_ms: f64,
    pub false_answer_rate: f64,
    pub false_abstain_rate: f64,
    pub forbidden_returns: usize,
    pub slices: BTreeMap<String, RetrievalSliceAggregate>,
    pub budget_violations: usize,
    pub citation_parity_violations: usize,
    pub scope_violations: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct RetrievalSliceAggregate {
    pub case_count: usize,
    pub answerable_count: usize,
    pub unanswerable_count: usize,
    pub macro_claim_recall: f64,
    pub macro_claim_precision: f64,
    pub macro_claim_ndcg: f64,
    pub macro_artifact_recall: f64,
    pub macro_artifact_precision: f64,
    pub macro_artifact_ndcg: f64,
    pub candidate_pool_claim_recall_at_16: f64,
    pub candidate_pool_claim_recall_at_32: f64,
    pub candidate_pool_artifact_recall_at_16: f64,
    pub candidate_pool_artifact_recall_at_32: f64,
    pub candidate_suggestion_claim_recall: f64,
    pub candidate_suggestion_artifact_recall: f64,
    pub probe_source_recall_at_5: f64,
    pub probe_source_recall_at_8: f64,
    pub probe_bait_lead_rate_at_5: f64,
    pub probe_bait_lead_rate_at_8: f64,
    pub mean_probe_result_bytes_at_5: f64,
    pub max_probe_result_bytes_at_5: usize,
    pub mean_probe_result_bytes_at_8: f64,
    pub max_probe_result_bytes_at_8: usize,
    pub mean_recall_candidate_hint_bytes: f64,
    pub max_recall_candidate_hint_bytes: usize,
    pub probe_fetch_content_bytes_per_lead_p95: usize,
    pub mean_probe_pipeline_bytes: f64,
    pub max_probe_pipeline_bytes: usize,
    pub max_large_artifact_pipeline_ratio: f64,
    pub probe_bait_fetches: usize,
    pub probe_recovery_case_count: usize,
    pub probe_recovery_success_rate: f64,
    pub probe_recovery_false_answer_rate: f64,
    pub probe_recovery_false_abstain_rate: f64,
    pub false_answer_rate: f64,
    pub false_abstain_rate: f64,
    pub forbidden_returns: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct BaselineComparison {
    pub epsilon: f64,
    pub checked_metrics: Vec<String>,
    pub regressions: Vec<String>,
    pub passed: bool,
}

pub async fn run_retrieval_eval(corpus_dir: &Path) -> Result<RetrievalEvalReport> {
    run_retrieval_eval_with_models(corpus_dir, None, None).await
}

/// Run the corpus with an optional already installed local semantic model.
/// The model directory is copied and verified inside the isolated evaluator;
/// evaluation never mutates the caller's memory store.
pub async fn run_retrieval_eval_with_semantic_model(
    corpus_dir: &Path,
    semantic_model_directory: Option<&Path>,
) -> Result<RetrievalEvalReport> {
    run_retrieval_eval_with_models(corpus_dir, semantic_model_directory, None).await
}

/// Run with independently pinned local candidate-generation and ordering
/// models. Neither path performs downloads or enables reranker qualification.
pub async fn run_retrieval_eval_with_models(
    corpus_dir: &Path,
    semantic_model_directory: Option<&Path>,
    reranker_model_directory: Option<&Path>,
) -> Result<RetrievalEvalReport> {
    run_retrieval_eval_with_options(
        corpus_dir,
        semantic_model_directory,
        reranker_model_directory,
        EvalOptions::default(),
    )
    .await
}

/// Run a bounded, selectable evaluation. Model paths are local-only and the
/// temporary store is always isolated from the operator's Memoree home.
pub async fn run_retrieval_eval_with_options(
    corpus_dir: &Path,
    semantic_model_directory: Option<&Path>,
    reranker_model_directory: Option<&Path>,
    options: EvalOptions,
) -> Result<RetrievalEvalReport> {
    if options.jobs != 1 {
        return Err(MemoryError::InvalidRequest(
            "memoree-eval currently requires --jobs 1 for deterministic model execution".into(),
        ));
    }
    if options.case_timeout_ms == 0 || options.suite_timeout_ms == 0 {
        return Err(MemoryError::InvalidRequest(
            "evaluation timeout values must be greater than zero".into(),
        ));
    }
    let suite_started = Instant::now();
    let corpus_started = Instant::now();
    let corpus_version = corpus_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| MemoryError::InvalidRequest("corpus directory needs a UTF-8 name".into()))?
        .to_owned();
    let seeds: Vec<SeedRecord> = read_jsonl(&corpus_dir.join("seed.jsonl"))?;
    let all_cases: Vec<EvalCase> = read_jsonl(&corpus_dir.join("cases.jsonl"))?;
    let baseline: EvalBaseline = read_json(&corpus_dir.join("baseline.json"))?;
    validate_corpus(&corpus_version, &seeds, &all_cases, &baseline)?;
    let recovery_path = corpus_dir.join("probe-recovery.json");
    let recovery_suite = recovery_path
        .exists()
        .then(|| read_json::<ProbeRecoverySuite>(&recovery_path))
        .transpose()?;
    let mut recovery_by_case = BTreeMap::new();
    if let Some(suite) = &recovery_suite {
        if suite.schema_version != 2
            || suite.lead_depth != 8
            || suite.max_fetches == 0
            || suite.max_fetches > 3
            || suite.evaluator.trim().is_empty()
            || suite.evaluated_at.trim().is_empty()
        {
            return Err(MemoryError::InvalidRequest(
                "probe-recovery.json has an unsupported or incomplete contract".into(),
            ));
        }
        for recovery in &suite.cases {
            if recovery.selected_sources.len() > suite.max_fetches
                || recovery.probe_query.trim().is_empty()
                || recovery_by_case
                    .insert(recovery.case_id.clone(), recovery.clone())
                    .is_some()
            {
                return Err(MemoryError::InvalidRequest(format!(
                    "invalid or duplicate probe recovery case {}",
                    recovery.case_id
                )));
            }
        }
    }

    if let Some(case_id) = options.case_id.as_deref()
        && !all_cases.iter().any(|case| case.case_id == case_id)
    {
        return Err(MemoryError::InvalidRequest(format!(
            "evaluation case {case_id} does not exist in {corpus_version}"
        )));
    }
    let cases = all_cases
        .iter()
        .filter(|case| {
            options
                .case_id
                .as_deref()
                .is_none_or(|case_id| case.case_id == case_id)
                && (!options.recovery_only || recovery_by_case.contains_key(&case.case_id))
        })
        .cloned()
        .collect::<Vec<_>>();
    if cases.is_empty() {
        return Err(MemoryError::InvalidRequest(
            "evaluation selection contains no cases".into(),
        ));
    }
    let requested_case_count = cases.len();
    let full_suite_selected = requested_case_count == all_cases.len();
    let full_recovery_suite_selected = options.recovery_only
        && options.case_id.is_none()
        && requested_case_count == recovery_by_case.len();
    let mut setup_timings = vec![stage_timing("corpus_loading", corpus_started)];

    let temporary = tempfile::tempdir()?;
    let service = MemoryService::new(Store::open(temporary.path())?);
    let seed_started = Instant::now();
    let entities = seed_store(&service, &seeds).await?;
    setup_timings.push(stage_timing("seed_loading", seed_started));
    if let Some(model_directory) = semantic_model_directory {
        let semantic_started = Instant::now();
        let semantic_report = service
            .store()
            .semantic_enable_from_directory(model_directory)?;
        setup_timings.push(EvalStageTiming {
            stage: "semantic_model_loading".into(),
            duration_ms: semantic_report.model_load_latency_ms,
        });
        setup_timings.push(EvalStageTiming {
            stage: "embedding_generation".into(),
            duration_ms: semantic_report.embedding_generation_ms,
        });
        setup_timings.push(EvalStageTiming {
            stage: "semantic_projection_io".into(),
            duration_ms: (elapsed_ms(semantic_started)
                - semantic_report.model_load_latency_ms
                - semantic_report.embedding_generation_ms)
                .max(0.0),
        });
    }
    if let Some(model_directory) = reranker_model_directory {
        let reranker_started = Instant::now();
        service
            .store()
            .reranker_enable_from_directory(model_directory)?;
        setup_timings.push(stage_timing("reranker_loading", reranker_started));
    }
    let labels_by_id: BTreeMap<String, String> = entities
        .iter()
        .map(|(label, entity)| (entity.entity_id.clone(), label.clone()))
        .collect();

    if options.prewarm_models {
        let prewarm_started = Instant::now();
        let prewarm_case = &cases[0];
        let _ = service.store().search(
            &prewarm_case.context.ambient(),
            &SearchInput {
                query: prewarm_case.query.clone(),
                horizon: prewarm_case.horizon,
                reason: prewarm_case.reason.clone(),
                limit: crate::protocol::MAX_SEARCH_ITEMS,
                include_historical: false,
                min_commit_seq: None,
                recency: RecencyBiasInput::default(),
            },
        )?;
        setup_timings.push(stage_timing("model_prewarm", prewarm_started));
    }

    let mut case_reports = Vec::new();
    let mut case_timings = Vec::new();
    let mut timed_out = false;
    let mut timed_out_case = None;
    let mut timed_out_stage = None;
    let mut hard_failures = Vec::new();
    let mut budget_violations = 0usize;
    let mut citation_parity_violations = 0usize;
    let mut citation_get_exactness_violations = 0usize;
    let mut scope_violations = 0usize;

    for case in &cases {
        if elapsed_ms(suite_started) >= options.suite_timeout_ms as f64 {
            timed_out = true;
            timed_out_case = Some(case.case_id.clone());
            timed_out_stage = Some("suite_budget".into());
            break;
        }
        let case_started = Instant::now();
        let mut stages = Vec::new();
        let ambient = case.context.ambient();
        let recovery = recovery_by_case.get(&case.case_id);
        let probe_query = recovery
            .map(|recovery| recovery.probe_query.as_str())
            .unwrap_or(case.query.as_str());
        let recall_started = Instant::now();
        let recall: RecallResult = read_operation(
            &service,
            Operation::MemoryRecall,
            RecallInput {
                query: case.query.clone(),
                horizon: case.horizon,
                reason: case.reason.clone(),
                max_claims: case.max_claims,
                max_artifact_refs: case.max_artifact_refs,
                max_excerpt_bytes: case.max_excerpt_bytes,
                max_candidate_claims: 3,
                max_candidate_artifact_refs: 3,
                min_commit_seq: None,
                recency: RecencyBiasInput::default(),
            },
            &ambient,
        )
        .await?;
        push_stage_duration(&mut stages, "recall", recall_started);
        let probe_started = Instant::now();
        let probe_at_8: ProbeResult = read_operation(
            &service,
            Operation::MemoryProbe,
            ProbeInput {
                query: probe_query.to_owned(),
                original_query: (probe_query != case.query).then(|| case.query.clone()),
                horizon: case.horizon,
                reason: case.reason.clone(),
                max_leads: 8,
                min_commit_seq: None,
                recency: RecencyBiasInput::default(),
            },
            &ambient,
        )
        .await?;
        let probe_at_5: ProbeResult = read_operation(
            &service,
            Operation::MemoryProbe,
            ProbeInput {
                query: probe_query.to_owned(),
                original_query: (probe_query != case.query).then(|| case.query.clone()),
                horizon: case.horizon,
                reason: case.reason.clone(),
                max_leads: 5,
                min_commit_seq: None,
                recency: RecencyBiasInput::default(),
            },
            &ambient,
        )
        .await?;
        push_stage_duration(&mut stages, "probe", probe_started);
        let supporting_retrieval_started = Instant::now();
        let search: SearchResult = read_operation(
            &service,
            Operation::Search,
            SearchInput {
                query: case.query.clone(),
                horizon: case.horizon,
                reason: case.reason.clone(),
                limit: crate::protocol::MAX_SEARCH_ITEMS,
                include_historical: false,
                min_commit_seq: None,
                recency: RecencyBiasInput::default(),
            },
            &ambient,
        )
        .await?;
        push_stage_duration(&mut stages, "recall", supporting_retrieval_started);
        let candidate_pool = service.store().search(
            &ambient,
            &SearchInput {
                query: case.query.clone(),
                horizon: case.horizon,
                reason: case.reason.clone(),
                limit: CANDIDATE_POOL_DIAGNOSTIC_K,
                include_historical: false,
                min_commit_seq: None,
                // Candidate membership must describe retrieval itself, before
                // the independently bounded presentation-time recency policy.
                recency: RecencyBiasInput { enabled: false },
            },
        )?;
        let bundle: crate::protocol::ContextBundle = read_operation(
            &service,
            Operation::ContextBuild,
            ContextBuildInput {
                search: SearchInput {
                    query: case.query.clone(),
                    horizon: case.horizon,
                    reason: case.reason.clone(),
                    limit: case.max_claims + case.max_artifact_refs,
                    include_historical: false,
                    min_commit_seq: None,
                    recency: RecencyBiasInput::default(),
                },
                max_bytes: case.max_context_bytes,
            },
            &ambient,
        )
        .await?;
        let retrieve_started = Instant::now();
        let retrieve: RetrieveResult = read_operation(
            &service,
            Operation::MemoryRetrieve,
            RetrieveInput {
                query: case.query.clone(),
                reformulation: recovery.map(|recovery| recovery.probe_query.clone()),
                horizon: case.horizon,
                reason: case.reason.clone(),
                max_claims: case.max_claims,
                max_artifact_refs: case.max_artifact_refs,
                max_excerpt_bytes: case.max_excerpt_bytes,
                max_recovery_leads: 8,
                max_recovery_bytes: 12 * 1024,
                min_commit_seq: None,
                recency: RecencyBiasInput::default(),
                profile: false,
            },
            &ambient,
        )
        .await?;
        let retrieve_latency_ms = elapsed_ms(retrieve_started);
        push_stage_ms(&mut stages, "one_call_retrieve", retrieve_latency_ms);

        let mut failures = Vec::new();
        if recall.presence != case.expected_presence {
            failures.push(format!(
                "presence was {:?}, expected {:?}",
                recall.presence, case.expected_presence
            ));
        }
        if recall.searched_horizons != vec![case.horizon] {
            failures.push(format!(
                "searched_horizons was {:?}, expected only {:?}",
                recall.searched_horizons, case.horizon
            ));
        }

        let returned_claims = ranked_labels_for_ids(
            recall.claims.iter().map(|claim| claim.claim_id.as_str()),
            &labels_by_id,
            &mut failures,
        );
        let returned_artifacts = ranked_labels_for_ids(
            recall
                .artifact_refs
                .iter()
                .map(|artifact| artifact.artifact_id.as_str()),
            &labels_by_id,
            &mut failures,
        );
        let suggested_candidate_claims = ranked_labels_for_ids(
            recall
                .candidate_claims
                .iter()
                .map(|claim| claim.claim_id.as_str()),
            &labels_by_id,
            &mut failures,
        );
        let suggested_candidate_artifacts = ranked_labels_for_ids(
            recall
                .candidate_artifact_refs
                .iter()
                .map(|artifact| artifact.artifact_id.as_str()),
            &labels_by_id,
            &mut failures,
        );
        let retrieve_returned_claims = ranked_labels_for_ids(
            retrieve.claims.iter().map(|claim| claim.claim_id.as_str()),
            &labels_by_id,
            &mut failures,
        );
        let retrieve_returned_artifacts = ranked_labels_for_ids(
            retrieve
                .artifact_refs
                .iter()
                .map(|artifact| artifact.artifact_id.as_str()),
            &labels_by_id,
            &mut failures,
        );
        let mut retrieve_qualified_evidence_artifacts = ranked_labels_for_ids(
            retrieve
                .claims
                .iter()
                .flat_map(|claim| claim.evidence.iter())
                .map(|evidence| evidence.artifact_id.as_str()),
            &labels_by_id,
            &mut failures,
        );
        retrieve_qualified_evidence_artifacts.sort();
        retrieve_qualified_evidence_artifacts.dedup();
        let mut retrieve_recovery_artifacts = retrieve
            .recovery
            .iter()
            .flat_map(|recovery| recovery.evidence.iter())
            .filter_map(|evidence| artifact_citation_parts(&evidence.citation))
            .filter_map(|(artifact_id, _)| labels_by_id.get(artifact_id).cloned())
            .collect::<Vec<_>>();
        retrieve_recovery_artifacts.sort();
        retrieve_recovery_artifacts.dedup();
        let retrieve_recovery_bait_artifacts = retrieve_recovery_artifacts
            .iter()
            .filter(|label| label.starts_with("embedding_bait_"))
            .cloned()
            .collect::<Vec<_>>();
        let retrieve_recovery_forbidden_artifacts = retrieve_recovery_artifacts
            .iter()
            .filter(|label| case.forbidden.contains(*label))
            .cloned()
            .collect::<Vec<_>>();
        let candidate_pool_claims = ranked_labels_for_ids(
            candidate_pool
                .hits
                .iter()
                .filter(|hit| matches!(hit.entity_type, EntityType::Claim))
                .map(|hit| hit.entity_id.as_str()),
            &labels_by_id,
            &mut failures,
        );
        let candidate_pool_artifacts = ranked_labels_for_ids(
            candidate_pool
                .hits
                .iter()
                .filter(|hit| matches!(hit.entity_type, EntityType::Artifact))
                .map(|hit| hit.entity_id.as_str()),
            &labels_by_id,
            &mut failures,
        );
        let candidate_pool_claims_at_16 = ranked_labels_for_ids(
            candidate_pool
                .hits
                .iter()
                .take(CANDIDATE_POOL_PRIMARY_K)
                .filter(|hit| matches!(hit.entity_type, EntityType::Claim))
                .map(|hit| hit.entity_id.as_str()),
            &labels_by_id,
            &mut failures,
        );
        let candidate_pool_artifacts_at_16 = ranked_labels_for_ids(
            candidate_pool
                .hits
                .iter()
                .take(CANDIDATE_POOL_PRIMARY_K)
                .filter(|hit| matches!(hit.entity_type, EntityType::Artifact))
                .map(|hit| hit.entity_id.as_str()),
            &labels_by_id,
            &mut failures,
        );
        let probe_sources_at_5 =
            ranked_probe_source_labels(&probe_at_5, &labels_by_id, &mut failures);
        let probe_sources_at_8 =
            ranked_probe_source_labels(&probe_at_8, &labels_by_id, &mut failures);
        let probe_leads_at_5 = probe_lead_diagnostics(&probe_at_5, &labels_by_id, &mut failures);
        let probe_leads_at_8 = probe_lead_diagnostics(&probe_at_8, &labels_by_id, &mut failures);
        let probe_bait_sources_at_5 = probe_sources_at_5
            .iter()
            .filter(|label| label.starts_with("embedding_bait_"))
            .cloned()
            .collect::<Vec<_>>();
        let probe_bait_sources_at_8 = probe_sources_at_8
            .iter()
            .filter(|label| label.starts_with("embedding_bait_"))
            .cloned()
            .collect::<Vec<_>>();
        let mut probe_refined_returned_claims = Vec::new();
        let mut probe_refined_returned_artifacts = Vec::new();
        let mut probe_fetched_content_bytes = 0usize;
        let mut probe_fetched_reference_count = 0usize;
        let mut probe_fetched_lead_bytes = Vec::new();
        let mut probe_fetch_response_bytes = 0usize;
        let mut probe_full_artifact_response_bytes = 0usize;
        let mut probe_refined_result_bytes = 0usize;
        let mut probe_citation_get_exact = recovery.map(|_| true);
        let mut probe_recovery_path_complete = recovery.map(|recovery| {
            !matches!(recovery.verdict, ProbeRecoveryVerdict::Supported)
                || !recovery.selected_sources.is_empty()
        });
        let mut probe_recovery_path_failures = Vec::new();
        let mut selected_has_large_artifact = false;
        if let Some(recovery) = recovery {
            let citation_started = Instant::now();
            if matches!(recovery.verdict, ProbeRecoveryVerdict::Supported)
                && recovery.selected_sources.is_empty()
            {
                probe_recovery_path_failures.push("no_selected_source".into());
            }
            for source in &recovery.selected_sources {
                let entity = entities.get(source).ok_or_else(|| {
                    MemoryError::InvalidRequest(format!(
                        "probe recovery {} selected unknown source {source}",
                        case.case_id
                    ))
                })?;
                if !matches!(entity.entity_type, EntityType::Artifact) {
                    return Err(MemoryError::InvalidRequest(format!(
                        "probe recovery {} selected non-artifact {source}",
                        case.case_id
                    )));
                }
                if !probe_sources_at_8.contains(source) {
                    probe_recovery_path_complete = Some(false);
                    probe_citation_get_exact = Some(false);
                    probe_recovery_path_failures.push(format!("selected_source_absent:{source}"));
                    failures.push(format!(
                        "probe recovery selected source {source} was absent from depth-8 leads"
                    ));
                    continue;
                }
                let Some(lead) = probe_lead_for_source(&probe_at_8, source, &labels_by_id) else {
                    probe_recovery_path_complete = Some(false);
                    probe_citation_get_exact = Some(false);
                    probe_recovery_path_failures
                        .push(format!("selected_source_unresolved:{source}"));
                    failures.push(format!(
                        "probe recovery selected source {source} had no resolvable depth-8 lead"
                    ));
                    continue;
                };
                let full = service.store().artifact_get(&ArtifactGetInput {
                    artifact_id: entity.entity_id.clone(),
                    revision_id: Some(entity.revision_id.clone()),
                    include_content: true,
                })?;
                probe_full_artifact_response_bytes += conservative_serialized_bytes(&full)?;
                selected_has_large_artifact |= full.size_bytes > 32 * 1024;
                let mut fetched_lead_bytes = 0usize;
                let mut successful_exact_fetches = 0usize;
                for locator in &lead.sources {
                    probe_fetched_reference_count += 1;
                    let Some((locator_artifact_id, Some(_))) =
                        artifact_citation_parts(&locator.citation)
                    else {
                        citation_get_exactness_violations += 1;
                        probe_citation_get_exact = Some(false);
                        probe_recovery_path_complete = Some(false);
                        probe_recovery_path_failures.push(format!("non_ranged_citation:{source}"));
                        failures.push(format!(
                            "probe recovery source {source} had malformed or non-ranged citation {}",
                            locator.citation
                        ));
                        continue;
                    };
                    let locator_entity = labels_by_id
                        .get(locator_artifact_id)
                        .and_then(|label| entities.get(label))
                        .unwrap_or(entity);
                    match unscoped_read_operation::<_, CitationGetResult>(
                        &service,
                        Operation::CitationGet,
                        CitationGetInput {
                            citation: locator.citation.clone(),
                            max_bytes: MAX_CITATION_FETCH_BYTES,
                        },
                    )
                    .await
                    {
                        Ok(fetched) => {
                            probe_fetched_content_bytes += fetched.byte_count;
                            fetched_lead_bytes += fetched.byte_count;
                            probe_fetch_response_bytes += conservative_serialized_bytes(&fetched)?;
                            if !citation_get_matches_entity(&fetched, locator_entity) {
                                citation_get_exactness_violations += 1;
                                probe_citation_get_exact = Some(false);
                                probe_recovery_path_complete = Some(false);
                                probe_recovery_path_failures
                                    .push(format!("citation_mismatch:{source}"));
                                failures.push(format!(
                                    "citation.get bytes for selected source {source} did not match its immutable artifact slice"
                                ));
                            } else {
                                successful_exact_fetches += 1;
                            }
                        }
                        Err(error) => {
                            citation_get_exactness_violations += 1;
                            probe_citation_get_exact = Some(false);
                            probe_recovery_path_complete = Some(false);
                            probe_recovery_path_failures
                                .push(format!("citation_fetch_failed:{source}"));
                            failures.push(format!(
                                "citation.get failed for selected source {source}: {error}"
                            ));
                        }
                    }
                }
                if successful_exact_fetches == 0 {
                    probe_recovery_path_complete = Some(false);
                    probe_recovery_path_failures.push(format!("no_exact_fetch:{source}"));
                }
                probe_fetched_lead_bytes.push(fetched_lead_bytes);
            }
            push_stage_duration(&mut stages, "citation_fetching", citation_started);
            let refined_started = Instant::now();
            match (recovery.verdict, recovery.refined_query.as_deref()) {
                (ProbeRecoveryVerdict::Supported, Some(refined_query)) => {
                    let refined: RecallResult = read_operation(
                        &service,
                        Operation::MemoryRecall,
                        RecallInput {
                            query: refined_query.to_owned(),
                            horizon: case.horizon,
                            reason: case.reason.clone(),
                            max_claims: case.max_claims,
                            max_artifact_refs: case.max_artifact_refs,
                            max_excerpt_bytes: case.max_excerpt_bytes,
                            max_candidate_claims: 0,
                            max_candidate_artifact_refs: 0,
                            min_commit_seq: None,
                            recency: RecencyBiasInput::default(),
                        },
                        &ambient,
                    )
                    .await?;
                    probe_refined_result_bytes = conservative_serialized_bytes(&refined)?;
                    probe_refined_returned_claims = ranked_labels_for_ids(
                        refined.claims.iter().map(|claim| claim.claim_id.as_str()),
                        &labels_by_id,
                        &mut failures,
                    );
                    probe_refined_returned_artifacts = ranked_labels_for_ids(
                        refined
                            .artifact_refs
                            .iter()
                            .map(|artifact| artifact.artifact_id.as_str()),
                        &labels_by_id,
                        &mut failures,
                    );
                }
                (ProbeRecoveryVerdict::Abstain, None) => {}
                _ => {
                    return Err(MemoryError::InvalidRequest(format!(
                        "probe recovery {} must pair supported with a refined query and abstain with null",
                        case.case_id
                    )));
                }
            }
            push_stage_duration(&mut stages, "refined_recall", refined_started);
        }
        check_expected_labels(
            "claim",
            &case.relevant_claims,
            &returned_claims,
            &mut failures,
        );
        check_expected_labels(
            "artifact",
            &case.relevant_artifacts,
            &returned_artifacts,
            &mut failures,
        );
        let mut forbidden_returned = case
            .forbidden
            .iter()
            .filter(|forbidden| {
                returned_claims.contains(forbidden) || returned_artifacts.contains(forbidden)
            })
            .cloned()
            .collect::<Vec<_>>();
        forbidden_returned.extend(
            returned_artifacts
                .iter()
                .filter(|label| label.starts_with("embedding_bait_"))
                .cloned(),
        );
        forbidden_returned.sort();
        forbidden_returned.dedup();
        for forbidden in &forbidden_returned {
            failures.push(format!("forbidden label {forbidden} was returned"));
        }
        check_conflicts(case, &recall.conflicts, &entities, &mut failures)?;

        let search_citations: BTreeSet<&str> = search
            .hits
            .iter()
            .map(|hit| hit.citation.as_str())
            .collect();
        for citation in recall
            .claims
            .iter()
            .map(|claim| claim.citation.as_str())
            .chain(
                recall
                    .artifact_refs
                    .iter()
                    .map(|artifact| artifact.citation.as_str()),
            )
        {
            if !search_citations.contains(citation) {
                citation_parity_violations += 1;
                failures.push(format!("recall citation {citation} was absent from search"));
            }
        }
        validate_evidence_refs(&recall, &entities, &mut failures)?;
        validate_artifact_refs(&recall, &entities, &mut failures)?;
        validate_candidate_refs(&recall, &entities, &mut failures)?;
        if case.require_artifact_spans {
            for label in &case.relevant_artifacts {
                let Some(entity) = entities.get(label) else {
                    continue;
                };
                if let Some(reference) = recall
                    .artifact_refs
                    .iter()
                    .find(|reference| reference.artifact_id == entity.entity_id)
                    && !reference.citation.contains('#')
                {
                    failures.push(format!(
                        "relevant artifact {label} did not return an exact byte span"
                    ));
                }
            }
        }

        if bundle.used_bytes > bundle.max_bytes || bundle.max_bytes != case.max_context_bytes {
            budget_violations += 1;
            failures.push(format!(
                "context used {}/{} bytes for requested budget {}",
                bundle.used_bytes, bundle.max_bytes, case.max_context_bytes
            ));
        }
        validate_bundle_evidence_rendering(&bundle, &mut failures);

        for label in returned_claims
            .iter()
            .chain(returned_artifacts.iter())
            .chain(suggested_candidate_claims.iter())
            .chain(suggested_candidate_artifacts.iter())
            .chain(probe_sources_at_8.iter())
            .chain(retrieve_returned_claims.iter())
            .chain(retrieve_returned_artifacts.iter())
            .chain(retrieve_qualified_evidence_artifacts.iter())
            .chain(retrieve_recovery_artifacts.iter())
        {
            let entity = entities.get(label).ok_or_else(|| {
                MemoryError::Integrity(format!("returned label {label} was not seeded"))
            })?;
            if !visible_at_horizon(&ambient, &entity.context, case.horizon) {
                scope_violations += 1;
                failures.push(format!(
                    "label {label} escaped {:?} retrieval scope",
                    case.horizon
                ));
            }
        }

        let answerable = case.is_answerable();
        let abstained = recall.presence == RecallPresence::None;
        let claim_recall = recall_ratio(&case.relevant_claims, &returned_claims);
        let artifact_recall = recall_ratio(&case.relevant_artifacts, &returned_artifacts);
        let semantic_scores = search
            .hits
            .iter()
            .filter_map(|hit| {
                hit.ranking.semantic_similarity.and_then(|similarity| {
                    labels_by_id
                        .get(&hit.entity_id)
                        .map(|label| (label.clone(), similarity))
                })
            })
            .collect::<BTreeMap<_, _>>();
        let probe_recovery_grounded = recovery.map(|recovery| {
            matches!(recovery.verdict, ProbeRecoveryVerdict::Supported)
                && recovery
                    .selected_sources
                    .iter()
                    .any(|source| case.relevant_artifacts.contains(source))
        });
        let probe_recovery_succeeded = recovery.map(|recovery| {
            if answerable {
                probe_recovery_grounded.unwrap_or(false)
                    && probe_recovery_path_complete.unwrap_or(false)
                    && probe_citation_get_exact == Some(true)
                    && case
                        .relevant_claims
                        .iter()
                        .all(|label| probe_refined_returned_claims.contains(label))
                    && case
                        .relevant_artifacts
                        .iter()
                        .all(|label| probe_refined_returned_artifacts.contains(label))
            } else {
                matches!(recovery.verdict, ProbeRecoveryVerdict::Abstain)
            }
        });
        let retrieve_recovery_succeeded = recovery.map(|_| {
            if !retrieve_recovery_forbidden_artifacts.is_empty() {
                false
            } else if answerable {
                if !case.relevant_artifacts.is_empty() {
                    case.relevant_artifacts.iter().all(|label| {
                        retrieve_returned_artifacts.contains(label)
                            || retrieve_qualified_evidence_artifacts.contains(label)
                            || retrieve_recovery_artifacts.contains(label)
                    })
                } else {
                    case.relevant_claims
                        .iter()
                        .all(|label| retrieve_returned_claims.contains(label))
                }
            } else {
                matches!(retrieve.presence, RecallPresence::None)
            }
        });
        let retrieve_recovery_false_answer = recovery.map(|_| {
            !retrieve_recovery_forbidden_artifacts.is_empty()
                || (!answerable && !matches!(retrieve.presence, RecallPresence::None))
        });
        let retrieve_recovery_false_abstain =
            retrieve_recovery_succeeded.map(|succeeded| answerable && !succeeded);
        let retrieve_packet_bytes = conservative_serialized_bytes(&retrieve)?;
        let probe_result_bytes_at_5 = conservative_serialized_bytes(&probe_at_5)?;
        let shipped_probe_result_bytes = conservative_serialized_bytes(&probe_at_8)?;
        let probe_pipeline_bytes = recovery
            .map(|_| {
                shipped_probe_result_bytes + probe_fetch_response_bytes + probe_refined_result_bytes
            })
            .unwrap_or(0);
        if probe_fetched_reference_count > 9 {
            hard_failures.push(format!(
                "probe recovery {} fetched {} references; maximum is 9",
                case.case_id, probe_fetched_reference_count
            ));
        }
        if probe_fetched_content_bytes > 12 * 1024 {
            hard_failures.push(format!(
                "probe recovery {} fetched {} source bytes; maximum is 12288",
                case.case_id, probe_fetched_content_bytes
            ));
        }
        if probe_pipeline_bytes > 12 * 1024 {
            hard_failures.push(format!(
                "probe recovery {} used {} serialized pipeline bytes; maximum is 12288",
                case.case_id, probe_pipeline_bytes
            ));
        }
        let probe_artifact_get_baseline_bytes = recovery
            .map(|_| {
                shipped_probe_result_bytes
                    + probe_full_artifact_response_bytes
                    + probe_refined_result_bytes
            })
            .unwrap_or(0);
        let probe_large_artifact_pipeline_ratio = (recovery.is_some()
            && selected_has_large_artifact
            && probe_artifact_get_baseline_bytes > 0)
            .then_some(probe_pipeline_bytes as f64 / probe_artifact_get_baseline_bytes as f64);
        let report = RetrievalCaseReport {
            case_id: case.case_id.clone(),
            original_query: case.query.clone(),
            probe_query: probe_query.to_owned(),
            slice: case.slice.clone(),
            tags: case.tags.clone(),
            gate: match case.gate {
                CaseGate::Hard => "hard",
                CaseGate::Report => "report",
            }
            .into(),
            answerable,
            abstained,
            presence: recall.presence,
            returned_claims: returned_claims.clone(),
            returned_artifacts: returned_artifacts.clone(),
            candidate_suggestion_claim_recall: recall_ratio(
                &case.relevant_claims,
                &suggested_candidate_claims,
            ),
            candidate_suggestion_artifact_recall: recall_ratio(
                &case.relevant_artifacts,
                &suggested_candidate_artifacts,
            ),
            suggested_candidate_claims,
            suggested_candidate_artifacts,
            candidate_pool_claim_recall_at_16: recall_ratio(
                &case.relevant_claims,
                &candidate_pool_claims_at_16,
            ),
            candidate_pool_claim_recall_at_32: recall_ratio(
                &case.relevant_claims,
                &candidate_pool_claims,
            ),
            candidate_pool_artifact_recall_at_16: recall_ratio(
                &case.relevant_artifacts,
                &candidate_pool_artifacts_at_16,
            ),
            candidate_pool_artifact_recall_at_32: recall_ratio(
                &case.relevant_artifacts,
                &candidate_pool_artifacts,
            ),
            candidate_pool_claims,
            candidate_pool_artifacts,
            semantic_scores,
            probe_leads_at_5,
            probe_leads_at_8,
            probe_source_recall_at_5: recall_ratio(&case.relevant_artifacts, &probe_sources_at_5),
            probe_source_recall_at_8: recall_ratio(&case.relevant_artifacts, &probe_sources_at_8),
            probe_result_bytes_at_5,
            probe_result_bytes_at_8: shipped_probe_result_bytes,
            recall_candidate_hint_bytes: recall
                .candidates_hint
                .as_deref()
                .map(str::len)
                .unwrap_or(0),
            probe_recovery_verdict: recovery.map(|recovery| match recovery.verdict {
                ProbeRecoveryVerdict::Supported => "supported".into(),
                ProbeRecoveryVerdict::Abstain => "abstain".into(),
            }),
            probe_selected_sources: recovery
                .map(|recovery| recovery.selected_sources.clone())
                .unwrap_or_default(),
            probe_fetched_reference_count,
            probe_fetched_content_bytes,
            probe_fetched_lead_bytes,
            probe_fetch_response_bytes,
            probe_full_artifact_response_bytes,
            probe_refined_result_bytes,
            probe_pipeline_bytes,
            probe_artifact_get_baseline_bytes,
            probe_large_artifact_pipeline_ratio,
            probe_citation_get_exact,
            probe_recovery_path_complete,
            probe_recovery_path_failures,
            probe_recovery_succeeded,
            probe_recovery_false_answer: recovery
                .map(|recovery| matches!(recovery.verdict, ProbeRecoveryVerdict::Supported))
                .zip(probe_recovery_grounded)
                .map(|(supported, grounded)| supported && !grounded),
            probe_recovery_false_abstain: probe_recovery_succeeded
                .map(|succeeded| answerable && !succeeded),
            retrieve_presence: retrieve.presence,
            retrieve_qualified_evidence_artifacts,
            retrieve_recovery_artifacts,
            retrieve_recovery_bait_artifacts,
            retrieve_recovery_forbidden_artifacts,
            retrieve_packet_bytes,
            retrieve_latency_ms,
            retrieve_recovery_succeeded,
            retrieve_recovery_false_answer,
            retrieve_recovery_false_abstain,
            probe_refined_returned_claims,
            probe_refined_returned_artifacts,
            probe_sources_at_5,
            probe_sources_at_8,
            probe_bait_sources_at_5,
            probe_bait_sources_at_8,
            claim_recall,
            claim_precision: precision(
                &case.relevant_claims,
                &case.helpful_claims,
                &returned_claims,
            ),
            claim_ndcg: ndcg(
                &case.relevant_claims,
                &case.helpful_claims,
                &returned_claims,
            ),
            claim_mrr: reciprocal_rank(&case.relevant_claims, &returned_claims),
            artifact_recall,
            artifact_precision: precision(
                &case.relevant_artifacts,
                &case.helpful_artifacts,
                &returned_artifacts,
            ),
            artifact_ndcg: ndcg(
                &case.relevant_artifacts,
                &case.helpful_artifacts,
                &returned_artifacts,
            ),
            artifact_mrr: reciprocal_rank(&case.relevant_artifacts, &returned_artifacts),
            forbidden_returned,
            context_used_bytes: bundle.used_bytes,
            context_max_bytes: bundle.max_bytes,
            failures: failures.clone(),
        };
        let reranker_inference_ms = recall.reranker_claims.inference_latency_ms.unwrap_or(0.0)
            + recall
                .reranker_artifacts
                .inference_latency_ms
                .unwrap_or(0.0)
            + search.reranker.inference_latency_ms.unwrap_or(0.0);
        if reranker_inference_ms > 0.0 {
            push_stage_ms(&mut stages, "reranker_inference", reranker_inference_ms);
        }
        let serialization_started = Instant::now();
        let _ = serde_json::to_vec(&report)?;
        push_stage_duration(&mut stages, "serialization", serialization_started);
        if matches!(case.gate, CaseGate::Hard) {
            hard_failures.extend(
                failures
                    .iter()
                    .map(|failure| format!("{}: {failure}", case.case_id)),
            );
        }
        case_reports.push(report);
        let total_ms = elapsed_ms(case_started);
        case_timings.push(EvalCaseTiming {
            case_id: case.case_id.clone(),
            total_ms,
            stages: stages.clone(),
        });
        if total_ms >= options.case_timeout_ms as f64
            || elapsed_ms(suite_started) >= options.suite_timeout_ms as f64
        {
            timed_out = true;
            timed_out_case = Some(case.case_id.clone());
            timed_out_stage = stages
                .iter()
                .max_by(|left, right| left.duration_ms.total_cmp(&right.duration_ms))
                .map(|timing| timing.stage.clone())
                .or_else(|| Some("case_total".into()));
            hard_failures.push(format!(
                "evaluation timed out after case {} (case {:.1} ms, suite {:.1} ms)",
                case.case_id,
                total_ms,
                elapsed_ms(suite_started)
            ));
            break;
        }
    }

    let aggregate = aggregate_reports(
        &case_reports,
        budget_violations,
        citation_parity_violations,
        citation_get_exactness_violations,
        scope_violations,
    );
    if semantic_model_directory.is_some()
        && (full_suite_selected || full_recovery_suite_selected)
        && !timed_out
    {
        if let Some(paraphrase) = aggregate.slices.get("paraphrase") {
            if paraphrase.probe_source_recall_at_5 + f64::EPSILON < 0.80 {
                hard_failures.push(format!(
                    "paraphrase probe source recall@5 {:.3} is below 0.80",
                    paraphrase.probe_source_recall_at_5
                ));
            }
            if paraphrase.probe_source_recall_at_8 + f64::EPSILON < 0.90 {
                hard_failures.push(format!(
                    "paraphrase probe source recall@8 {:.3} is below 0.90",
                    paraphrase.probe_source_recall_at_8
                ));
            }
            if paraphrase.probe_recovery_case_count > 0
                && (paraphrase.probe_recovery_success_rate + f64::EPSILON < 0.90
                    || paraphrase.probe_recovery_false_answer_rate > f64::EPSILON
                    || paraphrase.probe_recovery_false_abstain_rate > 0.10 + f64::EPSILON)
            {
                hard_failures.push(format!(
                    "paraphrase probe recovery gates failed: success {:.3}, false-answer {:.3}, false-abstain {:.3}",
                    paraphrase.probe_recovery_success_rate,
                    paraphrase.probe_recovery_false_answer_rate,
                    paraphrase.probe_recovery_false_abstain_rate
                ));
            }
        }
        if aggregate.max_probe_result_bytes_at_5 > 2048
            || aggregate.max_probe_result_bytes_at_8 > 3072
        {
            hard_failures.push(format!(
                "probe byte gates exceeded: max@5 {}, max@8 {}",
                aggregate.max_probe_result_bytes_at_5, aggregate.max_probe_result_bytes_at_8
            ));
        }
        if aggregate.probe_fetch_content_bytes_per_lead_p95 > MAX_CITATION_FETCH_BYTES {
            hard_failures.push(format!(
                "probe exact-fetch p95 {} exceeds the {} byte hard limit",
                aggregate.probe_fetch_content_bytes_per_lead_p95, MAX_CITATION_FETCH_BYTES
            ));
        }
        if aggregate.max_large_artifact_pipeline_ratio > 0.25 + f64::EPSILON {
            hard_failures.push(format!(
                "probe pipeline consumed {:.3} of the artifact.get baseline for a selected artifact over 32 KiB (maximum 0.250)",
                aggregate.max_large_artifact_pipeline_ratio
            ));
        }
        if aggregate.citation_get_exactness_violations > 0 {
            hard_failures.push(format!(
                "citation.get had {} exactness violations",
                aggregate.citation_get_exactness_violations
            ));
        }
        if aggregate.probe_bait_fetches > 0 {
            hard_failures.push(format!(
                "probe recovery fetched {} semantic bait sources",
                aggregate.probe_bait_fetches
            ));
        }
        if aggregate.retrieve_recovery_case_count > 0
            && (aggregate.retrieve_recovery_success_rate + f64::EPSILON < 0.90
                || aggregate.retrieve_recovery_false_answer_rate > f64::EPSILON
                || aggregate.retrieve_recovery_false_abstain_rate > 0.10 + f64::EPSILON)
        {
            hard_failures.push(format!(
                "one-call recovery gates failed: success {:.3}, false-answer {:.3}, false-abstain {:.3}",
                aggregate.retrieve_recovery_success_rate,
                aggregate.retrieve_recovery_false_answer_rate,
                aggregate.retrieve_recovery_false_abstain_rate
            ));
        }
        if aggregate.retrieve_bait_fetches > 0 {
            hard_failures.push(format!(
                "one-call recovery fetched {} semantic bait sources",
                aggregate.retrieve_bait_fetches
            ));
        }
        if aggregate.retrieve_forbidden_fetches > 0 {
            hard_failures.push(format!(
                "one-call recovery fetched {} case-forbidden sources",
                aggregate.retrieve_forbidden_fetches
            ));
        }
        if aggregate.max_retrieve_packet_bytes > 12 * 1024 {
            hard_failures.push(format!(
                "one-call packet used {} serialized bytes; maximum is 12288",
                aggregate.max_retrieve_packet_bytes
            ));
        }
        if aggregate.retrieve_latency_p95_ms > 2_000.0 {
            hard_failures.push(format!(
                "one-call recovery p95 {:.1} ms exceeds 2000 ms",
                aggregate.retrieve_latency_p95_ms
            ));
        }
    }
    let report_schema_version = baseline.schema_version;
    if timed_out
        && !hard_failures
            .iter()
            .any(|failure| failure.contains("timed out"))
    {
        hard_failures.push(format!(
            "evaluation timed out before completing {} selected cases",
            requested_case_count
        ));
    }
    let complete = !timed_out && case_reports.len() == requested_case_count;
    let baseline = if full_suite_selected && complete {
        compare_baseline(&aggregate, &baseline)
    } else {
        BaselineComparison {
            epsilon: baseline.epsilon,
            checked_metrics: vec![],
            regressions: vec![],
            passed: true,
        }
    };
    let passed = complete && hard_failures.is_empty() && baseline.passed;
    let total_ms = elapsed_ms(suite_started);
    Ok(RetrievalEvalReport {
        schema_version: report_schema_version,
        corpus_version,
        isolated_temporary_store: true,
        cases: case_reports,
        aggregate,
        baseline,
        hard_failures,
        requested_case_count,
        completed_case_count: case_timings.len(),
        complete,
        timed_out,
        timed_out_case,
        timed_out_stage,
        options,
        timings: EvalTimings {
            total_ms,
            setup: setup_timings,
            cases: case_timings,
        },
        passed,
    })
}

fn elapsed_ms(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1_000.0
}

fn stage_timing(stage: &str, started: Instant) -> EvalStageTiming {
    EvalStageTiming {
        stage: stage.into(),
        duration_ms: elapsed_ms(started),
    }
}

fn push_stage_duration(stages: &mut Vec<EvalStageTiming>, stage: &str, started: Instant) {
    push_stage_ms(stages, stage, elapsed_ms(started));
}

fn push_stage_ms(stages: &mut Vec<EvalStageTiming>, stage: &str, duration_ms: f64) {
    if let Some(existing) = stages.iter_mut().find(|timing| timing.stage == stage) {
        existing.duration_ms += duration_ms;
    } else {
        stages.push(EvalStageTiming {
            stage: stage.into(),
            duration_ms,
        });
    }
}

fn validate_corpus(
    corpus_version: &str,
    seeds: &[SeedRecord],
    cases: &[EvalCase],
    baseline: &EvalBaseline,
) -> Result<()> {
    if seeds.is_empty() || cases.is_empty() {
        return Err(MemoryError::InvalidRequest(
            "retrieval corpus needs at least one seed and one case".into(),
        ));
    }
    let schema_version = baseline.schema_version;
    if !(MIN_EVAL_SCHEMA_VERSION..=EVAL_SCHEMA_VERSION).contains(&schema_version)
        || baseline.corpus_version != corpus_version
        || seeds
            .iter()
            .any(|seed| seed.schema_version != schema_version)
        || cases
            .iter()
            .any(|case| case.schema_version != schema_version)
    {
        return Err(MemoryError::InvalidRequest(format!(
            "eval schema/corpus version mismatch for {corpus_version}"
        )));
    }
    let mut labels = BTreeSet::new();
    for seed in seeds {
        if !labels.insert(seed.label.clone()) {
            return Err(MemoryError::InvalidRequest(format!(
                "duplicate seed label {}",
                seed.label
            )));
        }
    }
    let mut case_ids = BTreeSet::new();
    for case in cases {
        if !case_ids.insert(case.case_id.clone()) {
            return Err(MemoryError::InvalidRequest(format!(
                "duplicate eval case id {}",
                case.case_id
            )));
        }
        if case.horizon != Horizon::Ambient
            && case
                .reason
                .as_deref()
                .is_none_or(|reason| reason.trim().is_empty())
        {
            return Err(MemoryError::InvalidRequest(format!(
                "case {} broadens without a reason",
                case.case_id
            )));
        }
        if !EVAL_SLICES.contains(&case.slice.as_str()) {
            return Err(MemoryError::InvalidRequest(format!(
                "case {} uses unknown slice {}",
                case.case_id, case.slice
            )));
        }
        for tag in &case.tags {
            if !EVAL_TAGS.contains(&tag.as_str()) {
                return Err(MemoryError::InvalidRequest(format!(
                    "case {} uses unknown tag {tag}",
                    case.case_id
                )));
            }
        }
        if schema_version >= 2 && case.provenance.is_none() {
            return Err(MemoryError::InvalidRequest(format!(
                "schema v2 case {} requires provenance",
                case.case_id
            )));
        }
        if let Some(provenance) = &case.provenance {
            if provenance.author.trim().is_empty() {
                return Err(MemoryError::InvalidRequest(format!(
                    "case {} has an empty provenance author",
                    case.case_id
                )));
            }
            let _ = (
                provenance.source,
                provenance.labeled_at,
                provenance.second_label.as_deref(),
            );
        }
        let has_direct = !case.relevant_claims.is_empty() || !case.relevant_artifacts.is_empty();
        if case
            .answerable
            .is_some_and(|answerable| answerable != has_direct)
        {
            return Err(MemoryError::InvalidRequest(format!(
                "case {} answerable disagrees with its direct gold labels",
                case.case_id
            )));
        }
        for label in case
            .relevant_claims
            .iter()
            .chain(case.relevant_artifacts.iter())
            .chain(case.helpful_claims.iter())
            .chain(case.helpful_artifacts.iter())
            .chain(case.forbidden.iter())
            .chain(case.expected_conflicts.iter().flatten())
        {
            if !labels.contains(label) {
                return Err(MemoryError::InvalidRequest(format!(
                    "case {} references unknown label {label}",
                    case.case_id
                )));
            }
        }
    }
    Ok(())
}

async fn seed_store(
    service: &MemoryService,
    seeds: &[SeedRecord],
) -> Result<BTreeMap<String, SeededEntity>> {
    let mut entities = BTreeMap::new();
    for (index, seed) in seeds.iter().enumerate() {
        if entities.contains_key(&seed.label) {
            return Err(MemoryError::InvalidRequest(format!(
                "duplicate seed label {}",
                seed.label
            )));
        }
        let ambient = seed.context.ambient();
        match &seed.item {
            SeedItem::Artifact {
                kind,
                title,
                media_type,
                content,
                content_repeat,
            } => {
                let content = materialize_seed_content(content, content_repeat.as_ref())?;
                let mutation: MutationResult<ArtifactRecord> = mutation_operation(
                    service,
                    Operation::ArtifactPut,
                    ArtifactPutInput {
                        kind: kind.clone(),
                        title: title.clone(),
                        media_type: media_type.clone(),
                        content: content.clone(),
                        provenance: BTreeMap::from([(
                            "eval_label".into(),
                            serde_json::Value::String(seed.label.clone()),
                        )]),
                        actor: Some("memoree-eval".into()),
                    },
                    &ambient,
                    &format!("eval:{index}:{}", seed.label),
                )
                .await?;
                let text = match &content {
                    ArtifactContent::Text(text) => Some(text.clone()),
                    ArtifactContent::Base64(_) => None,
                };
                entities.insert(
                    seed.label.clone(),
                    SeededEntity {
                        entity_type: EntityType::Artifact,
                        entity_id: mutation.value.artifact_id,
                        revision_id: mutation.value.revision_id,
                        context: ambient,
                        text,
                    },
                );
            }
            SeedItem::Claim {
                claim_type,
                statement,
                evidence,
                valid_from,
                valid_until,
            } => {
                let evidence = evidence
                    .iter()
                    .map(|item| seed_evidence(item, &entities))
                    .collect::<Result<Vec<_>>>()?;
                let mutation: MutationResult<ClaimRecord> = mutation_operation(
                    service,
                    Operation::ClaimAssert,
                    ClaimAssertInput {
                        claim_type: *claim_type,
                        statement: statement.clone(),
                        confidence: None,
                        evidence,
                        valid_from: *valid_from,
                        valid_until: *valid_until,
                        actor: Some("memoree-eval".into()),
                    },
                    &ambient,
                    &format!("eval:{index}:{}", seed.label),
                )
                .await?;
                entities.insert(
                    seed.label.clone(),
                    SeededEntity {
                        entity_type: EntityType::Claim,
                        entity_id: mutation.value.claim_id,
                        revision_id: mutation.value.revision_id,
                        context: ambient,
                        text: Some(statement.clone()),
                    },
                );
            }
            SeedItem::Relation {
                source_label,
                relation,
                target_label,
            } => {
                let source = entities.get(source_label).ok_or_else(|| {
                    MemoryError::InvalidRequest(format!(
                        "relation {} references unseeded source {source_label}",
                        seed.label
                    ))
                })?;
                let target = entities.get(target_label).ok_or_else(|| {
                    MemoryError::InvalidRequest(format!(
                        "relation {} references unseeded target {target_label}",
                        seed.label
                    ))
                })?;
                let _: MutationResult<RelationRecord> = mutation_operation(
                    service,
                    Operation::RelationPut,
                    RelationPutInput {
                        source_type: source.entity_type,
                        source_id: source.entity_id.clone(),
                        relation: *relation,
                        target_type: target.entity_type,
                        target_id: target.entity_id.clone(),
                        metadata: BTreeMap::from([(
                            "eval_label".into(),
                            serde_json::Value::String(seed.label.clone()),
                        )]),
                    },
                    &ambient,
                    &format!("eval:{index}:{}", seed.label),
                )
                .await?;
            }
        }
    }
    Ok(entities)
}

fn materialize_seed_content(
    content: &ArtifactContent,
    repeat: Option<&SeedContentRepeat>,
) -> Result<ArtifactContent> {
    let Some(repeat) = repeat else {
        return Ok(content.clone());
    };
    let ArtifactContent::Text(prefix) = content else {
        return Err(MemoryError::InvalidRequest(
            "eval content_repeat requires text artifact content".into(),
        ));
    };
    let repeated_bytes = repeat
        .unit
        .len()
        .checked_mul(repeat.count)
        .and_then(|bytes| bytes.checked_add(prefix.len()))
        .and_then(|bytes| bytes.checked_add(repeat.suffix.len()))
        .ok_or(MemoryError::ContentTooLarge)?;
    if repeated_bytes > crate::protocol::MAX_ARTIFACT_BYTES {
        return Err(MemoryError::ContentTooLarge);
    }
    let mut expanded = String::with_capacity(repeated_bytes);
    expanded.push_str(prefix);
    for _ in 0..repeat.count {
        expanded.push_str(&repeat.unit);
    }
    expanded.push_str(&repeat.suffix);
    Ok(ArtifactContent::Text(expanded))
}

fn seed_evidence(
    seed: &SeedEvidence,
    entities: &BTreeMap<String, SeededEntity>,
) -> Result<EvidenceLocator> {
    let artifact = entities.get(&seed.artifact_label).ok_or_else(|| {
        MemoryError::InvalidRequest(format!(
            "evidence references unseeded artifact {}",
            seed.artifact_label
        ))
    })?;
    if !matches!(artifact.entity_type, EntityType::Artifact) {
        return Err(MemoryError::InvalidRequest(format!(
            "evidence label {} is not an artifact",
            seed.artifact_label
        )));
    }
    let (start_byte, end_byte) = match &seed.quote {
        Some(quote) => {
            let text = artifact.text.as_deref().ok_or_else(|| {
                MemoryError::InvalidRequest(format!(
                    "quoted evidence {} is not text",
                    seed.artifact_label
                ))
            })?;
            let matches: Vec<usize> = text.match_indices(quote).map(|(index, _)| index).collect();
            if matches.len() != 1 {
                return Err(MemoryError::InvalidRequest(format!(
                    "evidence quote for {} matched {} times",
                    seed.artifact_label,
                    matches.len()
                )));
            }
            (
                Some(matches[0] as u64),
                Some((matches[0] + quote.len()) as u64),
            )
        }
        None => (None, None),
    };
    Ok(EvidenceLocator {
        artifact_id: artifact.entity_id.clone(),
        revision_id: artifact.revision_id.clone(),
        start_byte,
        end_byte,
    })
}

async fn mutation_operation<I: Serialize, T: DeserializeOwned>(
    service: &MemoryService,
    operation: Operation,
    input: I,
    context: &AmbientContext,
    idempotency_key: &str,
) -> Result<T> {
    let mut request = Request::new(operation, input)?;
    request.context = Some(context.clone());
    request.context_source = ContextSource::Explicit;
    request.idempotency_key = Some(idempotency_key.into());
    response_result(service.handle(request).await)
}

async fn read_operation<I: Serialize, T: DeserializeOwned>(
    service: &MemoryService,
    operation: Operation,
    input: I,
    context: &AmbientContext,
) -> Result<T> {
    let mut request = Request::new(operation, input)?;
    request.context = Some(context.clone());
    request.context_source = ContextSource::Explicit;
    response_result(service.handle(request).await)
}

async fn unscoped_read_operation<I: Serialize, T: DeserializeOwned>(
    service: &MemoryService,
    operation: Operation,
    input: I,
) -> Result<T> {
    let request = Request::new(operation, input)?;
    response_result(service.handle(request).await)
}

fn response_result<T: DeserializeOwned>(response: crate::protocol::Response) -> Result<T> {
    if !response.ok {
        let error = response
            .error
            .map(|error| format!("{:?}: {}", error.code, error.message))
            .unwrap_or_else(|| "unknown protocol error".into());
        return Err(MemoryError::InvalidRequest(format!(
            "eval protocol request failed: {error}"
        )));
    }
    serde_json::from_value(
        response
            .result
            .ok_or_else(|| MemoryError::Integrity("eval response had no result".into()))?,
    )
    .map_err(Into::into)
}

fn ranked_labels_for_ids<'a>(
    ids: impl Iterator<Item = &'a str>,
    labels_by_id: &BTreeMap<String, String>,
    failures: &mut Vec<String>,
) -> Vec<String> {
    let mut labels = Vec::new();
    let mut seen = BTreeSet::new();
    for id in ids {
        match labels_by_id.get(id) {
            Some(label) => {
                if seen.insert(label.clone()) {
                    labels.push(label.clone());
                }
            }
            None => failures.push(format!("returned unseeded entity {id}")),
        }
    }
    labels
}

fn ranked_probe_source_labels(
    probe: &ProbeResult,
    labels_by_id: &BTreeMap<String, String>,
    failures: &mut Vec<String>,
) -> Vec<String> {
    let mut sources = Vec::new();
    let mut seen = BTreeSet::new();
    for lead in &probe.leads {
        let Some(source) = lead.sources.first() else {
            failures.push("probe lead had no source locator".into());
            continue;
        };
        let Some(artifact_id) = source
            .citation
            .strip_prefix("memoree://artifact/")
            .and_then(|tail| tail.split_once('@').map(|(artifact_id, _)| artifact_id))
        else {
            failures.push(format!(
                "probe lead had a non-artifact source citation {}",
                source.citation
            ));
            continue;
        };
        if let Some(label) = labels_by_id.get(artifact_id) {
            if seen.insert(label.clone()) {
                sources.push(label.clone());
            }
        } else {
            failures.push(format!("probe returned unseeded artifact {artifact_id}"));
        }
    }
    sources
}

fn artifact_citation_parts(citation: &str) -> Option<(&str, Option<(usize, usize)>)> {
    let tail = citation.strip_prefix("memoree://artifact/")?;
    let (artifact_id, revision_and_span) = tail.split_once('@')?;
    let span = match revision_and_span.split_once('#') {
        Some((_, span)) => {
            let (start, end) = span.split_once('-')?;
            Some((start.parse().ok()?, end.parse().ok()?))
        }
        None => None,
    };
    Some((artifact_id, span))
}

fn probe_lead_diagnostics(
    probe: &ProbeResult,
    labels_by_id: &BTreeMap<String, String>,
    failures: &mut Vec<String>,
) -> Vec<ProbeLeadDiagnostic> {
    probe
        .leads
        .iter()
        .filter_map(|lead| {
            let Some(source) = lead.sources.first() else {
                failures.push("probe lead had no source locator".into());
                return None;
            };
            let Some((artifact_id, span)) = artifact_citation_parts(&source.citation) else {
                failures.push(format!(
                    "probe lead had an invalid artifact citation {}",
                    source.citation
                ));
                return None;
            };
            let Some(source_label) = labels_by_id.get(artifact_id) else {
                failures.push(format!("probe returned unseeded artifact {artifact_id}"));
                return None;
            };
            let (start_byte, end_byte) = span
                .map(|(start, end)| (Some(start), Some(end)))
                .unwrap_or((None, None));
            Some(ProbeLeadDiagnostic {
                source_label: source_label.clone(),
                title: lead.title.clone(),
                start_byte,
                end_byte,
            })
        })
        .collect()
}

fn probe_lead_for_source<'a>(
    probe: &'a ProbeResult,
    source_label: &str,
    labels_by_id: &BTreeMap<String, String>,
) -> Option<&'a ProbeLead> {
    let source_matches = |source: &crate::protocol::ProbeSourceLocator| {
        artifact_citation_parts(&source.citation)
            .and_then(|(artifact_id, _)| labels_by_id.get(artifact_id))
            .is_some_and(|label| label == source_label)
    };
    probe
        .leads
        .iter()
        .find(|lead| {
            lead.sources.iter().any(|source| {
                source_matches(source)
                    && artifact_citation_parts(&source.citation)
                        .is_some_and(|(_, span)| span.is_some())
            })
        })
        .or_else(|| {
            probe
                .leads
                .iter()
                .find(|lead| lead.sources.iter().any(source_matches))
        })
}

fn citation_get_matches_entity(result: &CitationGetResult, entity: &SeededEntity) -> bool {
    if !result.content_is_untrusted || result.byte_count != result.content.len() {
        return false;
    }
    let Some((artifact_id, Some((start, end)))) = artifact_citation_parts(&result.citation) else {
        return false;
    };
    if artifact_id != entity.entity_id || end.saturating_sub(start) != result.byte_count {
        return false;
    }
    entity.text.as_deref().is_some_and(|text| {
        start < end
            && end <= text.len()
            && text.is_char_boundary(start)
            && text.is_char_boundary(end)
            && text[start..end] == result.content
    })
}

fn check_expected_labels(
    kind: &str,
    expected: &[String],
    returned: &[String],
    failures: &mut Vec<String>,
) {
    for label in expected {
        if !returned.contains(label) {
            failures.push(format!("relevant {kind} {label} was not returned"));
        }
    }
}

fn check_conflicts(
    case: &EvalCase,
    conflicts: &[ConflictSummary],
    entities: &BTreeMap<String, SeededEntity>,
    failures: &mut Vec<String>,
) -> Result<()> {
    for pair in &case.expected_conflicts {
        let left = entities.get(&pair[0]).ok_or_else(|| {
            MemoryError::InvalidRequest(format!("unknown conflict label {}", pair[0]))
        })?;
        let right = entities.get(&pair[1]).ok_or_else(|| {
            MemoryError::InvalidRequest(format!("unknown conflict label {}", pair[1]))
        })?;
        if !conflicts.iter().any(|conflict| {
            (conflict.left_id == left.entity_id && conflict.right_id == right.entity_id)
                || (conflict.left_id == right.entity_id && conflict.right_id == left.entity_id)
        }) {
            failures.push(format!(
                "expected conflict {} <-> {} was not surfaced",
                pair[0], pair[1]
            ));
        }
    }
    Ok(())
}

fn validate_evidence_refs(
    recall: &RecallResult,
    entities: &BTreeMap<String, SeededEntity>,
    failures: &mut Vec<String>,
) -> Result<()> {
    let artifacts: BTreeSet<(&str, &str)> = entities
        .values()
        .filter(|entity| matches!(entity.entity_type, EntityType::Artifact))
        .map(|entity| (entity.entity_id.as_str(), entity.revision_id.as_str()))
        .collect();
    for claim in &recall.claims {
        for evidence in &claim.evidence {
            if !artifacts.contains(&(evidence.artifact_id.as_str(), evidence.revision_id.as_str()))
            {
                failures.push(format!(
                    "claim {} returned unresolved evidence {}",
                    claim.claim_id, evidence.citation
                ));
            }
            if let (Some(start), Some(end), Some(excerpt)) =
                (evidence.start_byte, evidence.end_byte, &evidence.excerpt)
            {
                let artifact = entities
                    .values()
                    .find(|entity| entity.entity_id == evidence.artifact_id)
                    .ok_or_else(|| {
                        MemoryError::Integrity(format!(
                            "evidence artifact {} was not seeded",
                            evidence.artifact_id
                        ))
                    })?;
                let text = artifact.text.as_deref().ok_or_else(|| {
                    MemoryError::Integrity(format!(
                        "evidence artifact {} has no text",
                        evidence.artifact_id
                    ))
                })?;
                let bytes = text.as_bytes();
                if end as usize > bytes.len() || start >= end {
                    failures.push(format!("evidence {} is out of bounds", evidence.citation));
                } else {
                    let exact = String::from_utf8_lossy(&bytes[start as usize..end as usize]);
                    if !evidence.excerpt_truncated && exact != excerpt.as_str() {
                        failures.push(format!(
                            "evidence {} excerpt did not round-trip",
                            evidence.citation
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

fn validate_artifact_refs(
    recall: &RecallResult,
    entities: &BTreeMap<String, SeededEntity>,
    failures: &mut Vec<String>,
) -> Result<()> {
    for reference in &recall.artifact_refs {
        let artifact = entities
            .values()
            .find(|entity| entity.entity_id == reference.artifact_id)
            .ok_or_else(|| {
                MemoryError::Integrity(format!(
                    "artifact reference {} was not seeded",
                    reference.artifact_id
                ))
            })?;
        let base = format!(
            "memoree://artifact/{}@{}",
            artifact.entity_id, artifact.revision_id
        );
        let Some(suffix) = reference.citation.strip_prefix(&base) else {
            failures.push(format!(
                "artifact reference {} has a citation for a different revision",
                reference.artifact_id
            ));
            continue;
        };
        if suffix.is_empty() {
            // Revision-only citations are valid for title matches, binary
            // artifacts, and conservative boundary fallbacks.
            continue;
        }
        let Some(span) = suffix.strip_prefix('#') else {
            failures.push(format!(
                "artifact reference {} has an invalid citation suffix {suffix}",
                reference.artifact_id
            ));
            continue;
        };
        let Some((start, end)) = span.split_once('-') else {
            failures.push(format!(
                "artifact reference {} has an invalid byte span {span}",
                reference.artifact_id
            ));
            continue;
        };
        let Ok(start) = start.parse::<usize>() else {
            failures.push(format!(
                "artifact citation {} has invalid start",
                reference.citation
            ));
            continue;
        };
        let Ok(end) = end.parse::<usize>() else {
            failures.push(format!(
                "artifact citation {} has invalid end",
                reference.citation
            ));
            continue;
        };
        let Some(text) = artifact.text.as_deref() else {
            failures.push(format!(
                "binary artifact {} unexpectedly returned a byte span",
                reference.artifact_id
            ));
            continue;
        };
        if start >= end
            || end > text.len()
            || !text.is_char_boundary(start)
            || !text.is_char_boundary(end)
        {
            failures.push(format!(
                "artifact citation {} is out of bounds",
                reference.citation
            ));
            continue;
        }
        let exact = &text[start..end];
        if !exact.starts_with(&reference.excerpt)
            || (!reference.excerpt_truncated && exact != reference.excerpt)
        {
            failures.push(format!(
                "artifact citation {} excerpt did not round-trip",
                reference.citation
            ));
        }
    }
    Ok(())
}

fn validate_candidate_refs(
    recall: &RecallResult,
    entities: &BTreeMap<String, SeededEntity>,
    failures: &mut Vec<String>,
) -> Result<()> {
    for candidate in &recall.candidate_claims {
        let claim = entities
            .values()
            .find(|entity| entity.entity_id == candidate.claim_id)
            .ok_or_else(|| {
                MemoryError::Integrity(format!(
                    "candidate claim {} was not seeded",
                    candidate.claim_id
                ))
            })?;
        let expected = format!("memoree://claim/{}@{}", claim.entity_id, claim.revision_id);
        if !matches!(claim.entity_type, EntityType::Claim)
            || candidate.retrieval_tier != "unqualified_candidate"
            || candidate.revision_id != claim.revision_id
            || candidate.citation != expected
        {
            failures.push(format!(
                "candidate claim {} has invalid tier or citation",
                candidate.claim_id
            ));
        }
    }
    for candidate in &recall.candidate_artifact_refs {
        let artifact = entities
            .values()
            .find(|entity| entity.entity_id == candidate.artifact_id)
            .ok_or_else(|| {
                MemoryError::Integrity(format!(
                    "candidate artifact {} was not seeded",
                    candidate.artifact_id
                ))
            })?;
        let base = format!(
            "memoree://artifact/{}@{}",
            artifact.entity_id, artifact.revision_id
        );
        if !matches!(artifact.entity_type, EntityType::Artifact)
            || candidate.retrieval_tier != "unqualified_candidate"
            || candidate.revision_id != artifact.revision_id
            || !candidate.citation.starts_with(&base)
        {
            failures.push(format!(
                "candidate artifact {} has invalid tier or citation",
                candidate.artifact_id
            ));
            continue;
        }
        let Some(suffix) = candidate.citation.strip_prefix(&base) else {
            continue;
        };
        if suffix.is_empty() {
            continue;
        }
        let Some((start, end)) = suffix
            .strip_prefix('#')
            .and_then(|span| span.split_once('-'))
        else {
            failures.push(format!(
                "candidate artifact {} has an invalid byte span",
                candidate.artifact_id
            ));
            continue;
        };
        let (Ok(start), Ok(end)) = (start.parse::<usize>(), end.parse::<usize>()) else {
            failures.push(format!(
                "candidate artifact {} has non-numeric byte bounds",
                candidate.artifact_id
            ));
            continue;
        };
        let Some(text) = artifact.text.as_deref() else {
            failures.push(format!(
                "binary candidate artifact {} returned a byte span",
                candidate.artifact_id
            ));
            continue;
        };
        if start >= end
            || end > text.len()
            || !text.is_char_boundary(start)
            || !text.is_char_boundary(end)
            || !text[start..end].starts_with(&candidate.excerpt)
        {
            failures.push(format!(
                "candidate artifact citation {} did not round-trip",
                candidate.citation
            ));
        }
    }
    Ok(())
}

fn validate_bundle_evidence_rendering(
    bundle: &crate::protocol::ContextBundle,
    failures: &mut Vec<String>,
) {
    for item in bundle
        .manifest
        .iter()
        .filter(|item| matches!(item.entity_type, EntityType::Claim))
    {
        let Some(evidence) = item
            .provenance
            .get("evidence")
            .and_then(|value| value.as_array())
        else {
            continue;
        };
        for locator in evidence {
            let Some(artifact_id) = locator.get("artifact_id").and_then(|value| value.as_str())
            else {
                continue;
            };
            let Some(revision_id) = locator.get("revision_id").and_then(|value| value.as_str())
            else {
                continue;
            };
            let citation = match (
                locator.get("start_byte").and_then(|value| value.as_u64()),
                locator.get("end_byte").and_then(|value| value.as_u64()),
            ) {
                (Some(start), Some(end)) => {
                    format!("memoree://artifact/{artifact_id}@{revision_id}#{start}-{end}")
                }
                _ => format!("memoree://artifact/{artifact_id}@{revision_id}"),
            };
            if !bundle.rendered_markdown.contains(&citation) {
                failures.push(format!(
                    "context claim {} omitted rendered evidence {citation}",
                    item.citation
                ));
            }
        }
    }
}

fn visible_at_horizon(ambient: &AmbientContext, owner: &AmbientContext, horizon: Horizon) -> bool {
    match horizon {
        Horizon::Ambient => {
            ambient.workspace_id == owner.workspace_id
                && ambient.project_id == owner.project_id
                && (ambient.task_id.is_none()
                    || owner.task_id.is_none()
                    || ambient.task_id == owner.task_id)
        }
        Horizon::Workspace => ambient.workspace_id == owner.workspace_id,
        Horizon::Personal => true,
    }
}

fn recall_ratio(expected: &[String], returned: &[String]) -> Option<f64> {
    if expected.is_empty() {
        return None;
    }
    let relevant = expected
        .iter()
        .filter(|label| returned.contains(label))
        .count();
    Some(relevant as f64 / expected.len() as f64)
}

fn precision(direct: &[String], helpful: &[String], returned: &[String]) -> f64 {
    if returned.is_empty() {
        return if direct.is_empty() { 1.0 } else { 0.0 };
    }
    let relevant: BTreeSet<&str> = direct.iter().chain(helpful).map(String::as_str).collect();
    returned
        .iter()
        .filter(|label| relevant.contains(label.as_str()))
        .count() as f64
        / returned.len() as f64
}

fn reciprocal_rank(direct: &[String], returned: &[String]) -> Option<f64> {
    if direct.len() != 1 {
        return None;
    }
    returned
        .iter()
        .position(|label| label == &direct[0])
        .map(|index| 1.0 / (index + 1) as f64)
        .or(Some(0.0))
}

fn ndcg(direct: &[String], helpful: &[String], returned: &[String]) -> Option<f64> {
    if direct.is_empty() && helpful.is_empty() {
        return None;
    }
    let direct: BTreeSet<&str> = direct.iter().map(String::as_str).collect();
    let helpful: BTreeSet<&str> = helpful.iter().map(String::as_str).collect();
    let gain = |label: &str| {
        if direct.contains(label) {
            2.0
        } else if helpful.contains(label) {
            1.0
        } else {
            0.0
        }
    };
    let dcg = returned
        .iter()
        .enumerate()
        .map(|(index, label)| gain(label) / ((index + 2) as f64).log2())
        .sum::<f64>();
    let mut ideal = vec![2.0; direct.len()];
    ideal.extend(vec![1.0; helpful.len()]);
    ideal.truncate(returned.len().max(1));
    let idcg = ideal
        .iter()
        .enumerate()
        .map(|(index, gain)| gain / ((index + 2) as f64).log2())
        .sum::<f64>();
    Some(if idcg == 0.0 { 0.0 } else { dcg / idcg })
}

fn mean(values: impl Iterator<Item = f64>) -> f64 {
    let values: Vec<f64> = values.collect();
    if values.is_empty() {
        1.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn rate(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn percentile_nearest_rank(mut values: Vec<usize>, percentile: f64) -> usize {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    let rank = (percentile.clamp(0.0, 1.0) * values.len() as f64).ceil() as usize;
    values[rank.saturating_sub(1).min(values.len() - 1)]
}

fn percentile_nearest_rank_f64(mut values: Vec<f64>, percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(f64::total_cmp);
    let rank = (percentile.clamp(0.0, 1.0) * values.len() as f64).ceil() as usize;
    values[rank.saturating_sub(1).min(values.len() - 1)]
}

fn slice_aggregate(reports: &[&RetrievalCaseReport]) -> RetrievalSliceAggregate {
    let answerable_count = reports.iter().filter(|case| case.answerable).count();
    let unanswerable_count = reports.len() - answerable_count;
    let recovery_case_count = reports
        .iter()
        .filter(|case| case.probe_recovery_succeeded.is_some())
        .count();
    let recovery_answerable_count = reports
        .iter()
        .filter(|case| case.answerable && case.probe_recovery_succeeded.is_some())
        .count();
    RetrievalSliceAggregate {
        case_count: reports.len(),
        answerable_count,
        unanswerable_count,
        macro_claim_recall: mean(reports.iter().filter_map(|case| case.claim_recall)),
        macro_claim_precision: mean(reports.iter().map(|case| case.claim_precision)),
        macro_claim_ndcg: mean(reports.iter().filter_map(|case| case.claim_ndcg)),
        macro_artifact_recall: mean(reports.iter().filter_map(|case| case.artifact_recall)),
        macro_artifact_precision: mean(reports.iter().map(|case| case.artifact_precision)),
        macro_artifact_ndcg: mean(reports.iter().filter_map(|case| case.artifact_ndcg)),
        candidate_pool_claim_recall_at_16: mean(
            reports
                .iter()
                .filter_map(|case| case.candidate_pool_claim_recall_at_16),
        ),
        candidate_pool_claim_recall_at_32: mean(
            reports
                .iter()
                .filter_map(|case| case.candidate_pool_claim_recall_at_32),
        ),
        candidate_pool_artifact_recall_at_16: mean(
            reports
                .iter()
                .filter_map(|case| case.candidate_pool_artifact_recall_at_16),
        ),
        candidate_pool_artifact_recall_at_32: mean(
            reports
                .iter()
                .filter_map(|case| case.candidate_pool_artifact_recall_at_32),
        ),
        candidate_suggestion_claim_recall: mean(
            reports
                .iter()
                .filter_map(|case| case.candidate_suggestion_claim_recall),
        ),
        candidate_suggestion_artifact_recall: mean(
            reports
                .iter()
                .filter_map(|case| case.candidate_suggestion_artifact_recall),
        ),
        probe_source_recall_at_5: mean(
            reports
                .iter()
                .filter_map(|case| case.probe_source_recall_at_5),
        ),
        probe_source_recall_at_8: mean(
            reports
                .iter()
                .filter_map(|case| case.probe_source_recall_at_8),
        ),
        probe_bait_lead_rate_at_5: mean(reports.iter().map(|case| {
            rate(
                case.probe_bait_sources_at_5.len(),
                case.probe_sources_at_5.len(),
            )
        })),
        probe_bait_lead_rate_at_8: mean(reports.iter().map(|case| {
            rate(
                case.probe_bait_sources_at_8.len(),
                case.probe_sources_at_8.len(),
            )
        })),
        mean_probe_result_bytes_at_5: mean(
            reports
                .iter()
                .map(|case| case.probe_result_bytes_at_5 as f64),
        ),
        max_probe_result_bytes_at_5: reports
            .iter()
            .map(|case| case.probe_result_bytes_at_5)
            .max()
            .unwrap_or(0),
        mean_probe_result_bytes_at_8: mean(
            reports
                .iter()
                .map(|case| case.probe_result_bytes_at_8 as f64),
        ),
        max_probe_result_bytes_at_8: reports
            .iter()
            .map(|case| case.probe_result_bytes_at_8)
            .max()
            .unwrap_or(0),
        mean_recall_candidate_hint_bytes: mean(
            reports
                .iter()
                .map(|case| case.recall_candidate_hint_bytes as f64),
        ),
        max_recall_candidate_hint_bytes: reports
            .iter()
            .map(|case| case.recall_candidate_hint_bytes)
            .max()
            .unwrap_or(0),
        probe_fetch_content_bytes_per_lead_p95: percentile_nearest_rank(
            reports
                .iter()
                .flat_map(|case| case.probe_fetched_lead_bytes.iter().copied())
                .collect(),
            0.95,
        ),
        mean_probe_pipeline_bytes: mean(
            reports
                .iter()
                .filter(|case| case.probe_recovery_succeeded.is_some())
                .map(|case| case.probe_pipeline_bytes as f64),
        ),
        max_probe_pipeline_bytes: reports
            .iter()
            .map(|case| case.probe_pipeline_bytes)
            .max()
            .unwrap_or(0),
        max_large_artifact_pipeline_ratio: reports
            .iter()
            .filter_map(|case| case.probe_large_artifact_pipeline_ratio)
            .fold(0.0, f64::max),
        probe_bait_fetches: reports
            .iter()
            .flat_map(|case| case.probe_selected_sources.iter())
            .filter(|source| source.starts_with("embedding_bait_"))
            .count(),
        probe_recovery_case_count: recovery_case_count,
        probe_recovery_success_rate: rate(
            reports
                .iter()
                .filter(|case| case.probe_recovery_succeeded == Some(true))
                .count(),
            recovery_case_count,
        ),
        probe_recovery_false_answer_rate: rate(
            reports
                .iter()
                .filter(|case| case.probe_recovery_false_answer == Some(true))
                .count(),
            recovery_case_count,
        ),
        probe_recovery_false_abstain_rate: rate(
            reports
                .iter()
                .filter(|case| case.probe_recovery_false_abstain == Some(true))
                .count(),
            recovery_answerable_count,
        ),
        false_answer_rate: rate(
            reports
                .iter()
                .filter(|case| !case.answerable && !case.abstained)
                .count(),
            unanswerable_count,
        ),
        false_abstain_rate: rate(
            reports
                .iter()
                .filter(|case| case.answerable && case.abstained)
                .count(),
            answerable_count,
        ),
        forbidden_returns: reports
            .iter()
            .map(|case| case.forbidden_returned.len())
            .sum(),
    }
}

fn aggregate_reports(
    reports: &[RetrievalCaseReport],
    budget_violations: usize,
    citation_parity_violations: usize,
    citation_get_exactness_violations: usize,
    scope_violations: usize,
) -> RetrievalAggregate {
    let answerable_count = reports.iter().filter(|case| case.answerable).count();
    let unanswerable_count = reports.len() - answerable_count;
    let mut grouped: BTreeMap<String, Vec<&RetrievalCaseReport>> = BTreeMap::new();
    for report in reports {
        grouped
            .entry(report.slice.clone())
            .or_default()
            .push(report);
    }
    let recovery_case_count = reports
        .iter()
        .filter(|case| case.probe_recovery_succeeded.is_some())
        .count();
    let recovery_answerable_count = reports
        .iter()
        .filter(|case| case.answerable && case.probe_recovery_succeeded.is_some())
        .count();
    let retrieve_recovery_case_count = reports
        .iter()
        .filter(|case| case.retrieve_recovery_succeeded.is_some())
        .count();
    let retrieve_recovery_answerable_count = reports
        .iter()
        .filter(|case| case.answerable && case.retrieve_recovery_succeeded.is_some())
        .count();
    RetrievalAggregate {
        case_count: reports.len(),
        macro_claim_recall: mean(reports.iter().filter_map(|case| case.claim_recall)),
        macro_claim_precision: mean(reports.iter().map(|case| case.claim_precision)),
        macro_claim_ndcg: mean(reports.iter().filter_map(|case| case.claim_ndcg)),
        macro_artifact_recall: mean(reports.iter().filter_map(|case| case.artifact_recall)),
        macro_artifact_precision: mean(reports.iter().map(|case| case.artifact_precision)),
        macro_artifact_ndcg: mean(reports.iter().filter_map(|case| case.artifact_ndcg)),
        candidate_pool_claim_recall_at_16: mean(
            reports
                .iter()
                .filter_map(|case| case.candidate_pool_claim_recall_at_16),
        ),
        candidate_pool_claim_recall_at_32: mean(
            reports
                .iter()
                .filter_map(|case| case.candidate_pool_claim_recall_at_32),
        ),
        candidate_pool_artifact_recall_at_16: mean(
            reports
                .iter()
                .filter_map(|case| case.candidate_pool_artifact_recall_at_16),
        ),
        candidate_pool_artifact_recall_at_32: mean(
            reports
                .iter()
                .filter_map(|case| case.candidate_pool_artifact_recall_at_32),
        ),
        candidate_suggestion_claim_recall: mean(
            reports
                .iter()
                .filter_map(|case| case.candidate_suggestion_claim_recall),
        ),
        candidate_suggestion_artifact_recall: mean(
            reports
                .iter()
                .filter_map(|case| case.candidate_suggestion_artifact_recall),
        ),
        probe_source_recall_at_5: mean(
            reports
                .iter()
                .filter_map(|case| case.probe_source_recall_at_5),
        ),
        probe_source_recall_at_8: mean(
            reports
                .iter()
                .filter_map(|case| case.probe_source_recall_at_8),
        ),
        probe_bait_lead_rate_at_5: mean(reports.iter().map(|case| {
            rate(
                case.probe_bait_sources_at_5.len(),
                case.probe_sources_at_5.len(),
            )
        })),
        probe_bait_lead_rate_at_8: mean(reports.iter().map(|case| {
            rate(
                case.probe_bait_sources_at_8.len(),
                case.probe_sources_at_8.len(),
            )
        })),
        mean_probe_result_bytes_at_5: mean(
            reports
                .iter()
                .map(|case| case.probe_result_bytes_at_5 as f64),
        ),
        max_probe_result_bytes_at_5: reports
            .iter()
            .map(|case| case.probe_result_bytes_at_5)
            .max()
            .unwrap_or(0),
        mean_probe_result_bytes_at_8: mean(
            reports
                .iter()
                .map(|case| case.probe_result_bytes_at_8 as f64),
        ),
        max_probe_result_bytes_at_8: reports
            .iter()
            .map(|case| case.probe_result_bytes_at_8)
            .max()
            .unwrap_or(0),
        mean_recall_candidate_hint_bytes: mean(
            reports
                .iter()
                .map(|case| case.recall_candidate_hint_bytes as f64),
        ),
        max_recall_candidate_hint_bytes: reports
            .iter()
            .map(|case| case.recall_candidate_hint_bytes)
            .max()
            .unwrap_or(0),
        probe_fetch_content_bytes_per_lead_p95: percentile_nearest_rank(
            reports
                .iter()
                .flat_map(|case| case.probe_fetched_lead_bytes.iter().copied())
                .collect(),
            0.95,
        ),
        mean_probe_pipeline_bytes: mean(
            reports
                .iter()
                .filter(|case| case.probe_recovery_succeeded.is_some())
                .map(|case| case.probe_pipeline_bytes as f64),
        ),
        max_probe_pipeline_bytes: reports
            .iter()
            .map(|case| case.probe_pipeline_bytes)
            .max()
            .unwrap_or(0),
        max_large_artifact_pipeline_ratio: reports
            .iter()
            .filter_map(|case| case.probe_large_artifact_pipeline_ratio)
            .fold(0.0, f64::max),
        citation_get_exactness_violations,
        probe_bait_fetches: reports
            .iter()
            .flat_map(|case| case.probe_selected_sources.iter())
            .filter(|source| source.starts_with("embedding_bait_"))
            .count(),
        probe_recovery_case_count: recovery_case_count,
        probe_recovery_success_rate: rate(
            reports
                .iter()
                .filter(|case| case.probe_recovery_succeeded == Some(true))
                .count(),
            recovery_case_count,
        ),
        probe_recovery_false_answer_rate: rate(
            reports
                .iter()
                .filter(|case| case.probe_recovery_false_answer == Some(true))
                .count(),
            recovery_case_count,
        ),
        probe_recovery_false_abstain_rate: rate(
            reports
                .iter()
                .filter(|case| case.probe_recovery_false_abstain == Some(true))
                .count(),
            recovery_answerable_count,
        ),
        retrieve_recovery_case_count,
        retrieve_recovery_success_rate: rate(
            reports
                .iter()
                .filter(|case| case.retrieve_recovery_succeeded == Some(true))
                .count(),
            retrieve_recovery_case_count,
        ),
        retrieve_recovery_false_answer_rate: rate(
            reports
                .iter()
                .filter(|case| case.retrieve_recovery_false_answer == Some(true))
                .count(),
            retrieve_recovery_case_count,
        ),
        retrieve_recovery_false_abstain_rate: rate(
            reports
                .iter()
                .filter(|case| case.retrieve_recovery_false_abstain == Some(true))
                .count(),
            retrieve_recovery_answerable_count,
        ),
        retrieve_bait_fetches: reports
            .iter()
            .map(|case| case.retrieve_recovery_bait_artifacts.len())
            .sum(),
        retrieve_forbidden_fetches: reports
            .iter()
            .map(|case| case.retrieve_recovery_forbidden_artifacts.len())
            .sum(),
        mean_retrieve_packet_bytes: mean(
            reports.iter().map(|case| case.retrieve_packet_bytes as f64),
        ),
        max_retrieve_packet_bytes: reports
            .iter()
            .map(|case| case.retrieve_packet_bytes)
            .max()
            .unwrap_or(0),
        retrieve_latency_p95_ms: percentile_nearest_rank_f64(
            reports
                .iter()
                .map(|case| case.retrieve_latency_ms)
                .collect(),
            0.95,
        ),
        false_answer_rate: rate(
            reports
                .iter()
                .filter(|case| !case.answerable && !case.abstained)
                .count(),
            unanswerable_count,
        ),
        false_abstain_rate: rate(
            reports
                .iter()
                .filter(|case| case.answerable && case.abstained)
                .count(),
            answerable_count,
        ),
        forbidden_returns: reports
            .iter()
            .map(|case| case.forbidden_returned.len())
            .sum(),
        slices: grouped
            .into_iter()
            .map(|(slice, reports)| (slice, slice_aggregate(&reports)))
            .collect(),
        budget_violations,
        citation_parity_violations,
        scope_violations,
    }
}

fn compare_baseline(aggregate: &RetrievalAggregate, baseline: &EvalBaseline) -> BaselineComparison {
    let mut regressions = Vec::new();
    let mut checked_metrics = Vec::new();
    for (name, current, expected) in [
        (
            "macro_claim_recall",
            aggregate.macro_claim_recall,
            baseline.macro_claim_recall,
        ),
        (
            "macro_claim_precision",
            aggregate.macro_claim_precision,
            baseline.macro_claim_precision,
        ),
        (
            "macro_artifact_recall",
            aggregate.macro_artifact_recall,
            baseline.macro_artifact_recall,
        ),
        (
            "macro_artifact_precision",
            aggregate.macro_artifact_precision,
            baseline.macro_artifact_precision,
        ),
    ] {
        let Some(expected) = expected else {
            continue;
        };
        checked_metrics.push(name.into());
        if current + baseline.epsilon < expected {
            regressions.push(format!(
                "{name} regressed from {expected:.6} to {current:.6} (epsilon {:.6})",
                baseline.epsilon
            ));
        }
    }
    for (name, current, maximum) in [
        (
            "false_answer_rate",
            aggregate.false_answer_rate,
            baseline.max_false_answer_rate,
        ),
        (
            "false_abstain_rate",
            aggregate.false_abstain_rate,
            baseline.max_false_abstain_rate,
        ),
    ] {
        let Some(maximum) = maximum else {
            continue;
        };
        checked_metrics.push(name.into());
        if current > maximum + baseline.epsilon {
            regressions.push(format!(
                "{name} exceeded maximum {maximum:.6} at {current:.6} (epsilon {:.6})",
                baseline.epsilon
            ));
        }
    }
    if let Some(maximum) = baseline.max_forbidden_returns {
        checked_metrics.push("forbidden_returns".into());
        if aggregate.forbidden_returns > maximum {
            regressions.push(format!(
                "forbidden_returns exceeded maximum {maximum} at {}",
                aggregate.forbidden_returns
            ));
        }
    }
    BaselineComparison {
        epsilon: baseline.epsilon,
        checked_metrics,
        passed: regressions.is_empty(),
        regressions,
    }
}

fn read_jsonl<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>> {
    let source = fs::read_to_string(path)?;
    source
        .lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(index, line)| {
            serde_json::from_str(line).map_err(|error| {
                MemoryError::InvalidRequest(format!(
                    "{}:{} is invalid JSONL: {error}",
                    path.display(),
                    index + 1
                ))
            })
        })
        .collect()
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    serde_json::from_slice(&fs::read(path)?).map_err(|error| {
        MemoryError::InvalidRequest(format!("{} is invalid JSON: {error}", path.display()))
    })
}

fn conservative_serialized_bytes<T: Serialize>(value: &T) -> Result<usize> {
    let mut value = serde_json::to_value(value)?;
    canonicalize_wire_timestamps(&mut value);
    let actual = serde_json::to_vec(&value)?.len();
    Ok(actual.div_ceil(EVAL_WIRE_BUDGET_BLOCK_BYTES) * EVAL_WIRE_BUDGET_BLOCK_BYTES)
}

fn canonicalize_wire_timestamps(value: &mut Value) {
    const MAX_UTC_RFC3339: &str = "9999-12-31T23:59:59.999999999Z";
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                let timestamp_field =
                    key.ends_with("_at") || matches!(key.as_str(), "valid_from" | "valid_until");
                if timestamp_field
                    && let Some(serialized) = value.as_str()
                    && DateTime::parse_from_rfc3339(serialized).is_ok()
                {
                    *value = Value::String(MAX_UTC_RFC3339.into());
                } else {
                    canonicalize_wire_timestamps(value);
                }
            }
        }
        Value::Array(values) => {
            for value in values {
                canonicalize_wire_timestamps(value);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn committed_v1_corpus_passes_in_an_isolated_store() {
        let corpus = Path::new(env!("CARGO_MANIFEST_DIR")).join("eval/corpus/v1");
        let first = run_retrieval_eval(&corpus).await.unwrap();
        let second = run_retrieval_eval(&corpus).await.unwrap();
        assert!(first.passed, "{:?}", first.hard_failures);
        assert!(first.isolated_temporary_store);
        assert_eq!(
            deterministic_eval_value(&first),
            deterministic_eval_value(&second)
        );
    }

    #[tokio::test]
    async fn committed_v2_corpus_reports_ranked_realistic_slices_deterministically() {
        let corpus = Path::new(env!("CARGO_MANIFEST_DIR")).join("eval/corpus/v2");
        let first = run_retrieval_eval(&corpus).await.unwrap();
        let second = run_retrieval_eval(&corpus).await.unwrap();
        assert!(first.passed, "{:?}", first.hard_failures);
        assert_eq!(first.schema_version, 2);
        assert_eq!(first.aggregate.case_count, 69);
        assert_eq!(first.aggregate.forbidden_returns, 0);
        assert_eq!(first.aggregate.false_answer_rate, 0.0);
        assert!(first.aggregate.false_abstain_rate > 0.0);
        assert_eq!(first.aggregate.slices["exact_identifier"].case_count, 10);
        assert_eq!(first.aggregate.slices["paraphrase"].case_count, 11);
        assert_eq!(first.aggregate.slices["typo_abbreviation"].case_count, 10);
        assert_eq!(first.aggregate.slices["long_document"].case_count, 4);
        assert_eq!(
            first.aggregate.slices["exact_identifier"].macro_claim_recall,
            1.0
        );
        assert!(first.aggregate.slices["exact_identifier"].macro_claim_precision >= 0.9);
        assert_eq!(first.aggregate.slices["lexical"].macro_claim_recall, 1.0);
        assert!(first.aggregate.slices["lexical"].macro_claim_precision >= 0.7);
        assert!(first.aggregate.slices["typo_abbreviation"].macro_claim_recall >= 0.8);
        assert_eq!(
            first.aggregate.slices["long_document"].macro_artifact_ndcg,
            1.0
        );
        assert_eq!(first.aggregate.slices["honest_none"].false_answer_rate, 0.0);
        let first_value = deterministic_eval_value(&first);
        let second_value = deterministic_eval_value(&second);
        let first_cases = first_value["cases"].clone();
        let second_cases = second_value["cases"].clone();
        if let Some((path, left, right)) =
            first_json_difference(&first_cases, &second_cases, "$.cases".into())
        {
            let index = path
                .strip_prefix("$.cases[")
                .and_then(|path| path.split_once(']'))
                .and_then(|(index, _)| index.parse::<usize>().ok())
                .unwrap_or(0);
            let first_case = &first.cases[index];
            let second_case = &second.cases[index];
            panic!(
                "case evaluation changed at {path}: {left} != {right}; bytes {:?} != {:?}",
                (
                    &first_case.case_id,
                    first_case.probe_result_bytes_at_8,
                    first_case.probe_fetch_response_bytes,
                    first_case.probe_full_artifact_response_bytes,
                    first_case.probe_refined_result_bytes,
                ),
                (
                    &second_case.case_id,
                    second_case.probe_result_bytes_at_8,
                    second_case.probe_fetch_response_bytes,
                    second_case.probe_full_artifact_response_bytes,
                    second_case.probe_refined_result_bytes,
                )
            );
        }
        if let Some((path, left, right)) =
            first_json_difference(&first_value, &second_value, "$".into())
        {
            panic!("evaluation changed at {path}: {left} != {right}");
        }
    }

    fn deterministic_eval_value(report: &RetrievalEvalReport) -> Value {
        let mut value = serde_json::to_value(report).unwrap();
        let object = value.as_object_mut().unwrap();
        object.remove("timings");
        if let Some(aggregate) = object.get_mut("aggregate").and_then(Value::as_object_mut) {
            aggregate.remove("retrieve_latency_p95_ms");
        }
        if let Some(cases) = object.get_mut("cases").and_then(Value::as_array_mut) {
            for case in cases {
                if let Some(case) = case.as_object_mut() {
                    case.remove("retrieve_latency_ms");
                }
            }
        }
        value
    }

    fn first_json_difference(
        left: &Value,
        right: &Value,
        path: String,
    ) -> Option<(String, Value, Value)> {
        match (left, right) {
            (Value::Object(left), Value::Object(right)) => {
                let keys = left.keys().chain(right.keys()).collect::<BTreeSet<_>>();
                for key in keys {
                    let next = format!("{path}.{key}");
                    match (left.get(key), right.get(key)) {
                        (Some(left), Some(right)) => {
                            if let Some(difference) = first_json_difference(left, right, next) {
                                return Some(difference);
                            }
                        }
                        (left, right) => {
                            return Some((
                                next,
                                left.cloned().unwrap_or(Value::Null),
                                right.cloned().unwrap_or(Value::Null),
                            ));
                        }
                    }
                }
                None
            }
            (Value::Array(left), Value::Array(right)) if left.len() == right.len() => left
                .iter()
                .zip(right)
                .enumerate()
                .find_map(|(index, (left, right))| {
                    first_json_difference(left, right, format!("{path}[{index}]"))
                }),
            _ if left == right => None,
            _ => Some((path, left.clone(), right.clone())),
        }
    }

    #[test]
    fn graded_rank_metrics_reward_direct_before_helpful() {
        let direct = vec!["direct".into()];
        let helpful = vec!["helpful".into()];
        let ideal = vec!["direct".into(), "helpful".into()];
        let reversed = vec!["helpful".into(), "direct".into()];
        assert_eq!(reciprocal_rank(&direct, &ideal), Some(1.0));
        assert_eq!(precision(&direct, &helpful, &reversed), 1.0);
        assert_eq!(ndcg(&direct, &helpful, &ideal), Some(1.0));
        assert!(ndcg(&direct, &helpful, &reversed) < ndcg(&direct, &helpful, &ideal));
    }

    #[test]
    fn wire_accounting_normalizes_timestamp_precision_without_touching_content() {
        let short = serde_json::json!({
            "created_at": "2026-07-21T16:09:46Z",
            "content": {"data": "2026-07-21T16:09:46Z"}
        });
        let precise = serde_json::json!({
            "created_at": "2026-07-21T16:09:46.123456789Z",
            "content": {"data": "2026-07-21T16:09:46Z"}
        });
        assert_eq!(
            conservative_serialized_bytes(&short).unwrap(),
            conservative_serialized_bytes(&precise).unwrap()
        );
        let mut normalized = short;
        canonicalize_wire_timestamps(&mut normalized);
        assert_eq!(normalized["content"]["data"], "2026-07-21T16:09:46Z");
    }

    #[test]
    fn baseline_comparison_rejects_a_metric_regression() {
        let aggregate = RetrievalAggregate {
            case_count: 1,
            macro_claim_recall: 0.75,
            macro_claim_precision: 1.0,
            macro_claim_ndcg: 1.0,
            macro_artifact_recall: 1.0,
            macro_artifact_precision: 1.0,
            macro_artifact_ndcg: 1.0,
            candidate_pool_claim_recall_at_16: 1.0,
            candidate_pool_claim_recall_at_32: 1.0,
            candidate_pool_artifact_recall_at_16: 1.0,
            candidate_pool_artifact_recall_at_32: 1.0,
            candidate_suggestion_claim_recall: 1.0,
            candidate_suggestion_artifact_recall: 1.0,
            probe_source_recall_at_5: 1.0,
            probe_source_recall_at_8: 1.0,
            probe_bait_lead_rate_at_5: 0.0,
            probe_bait_lead_rate_at_8: 0.0,
            mean_probe_result_bytes_at_5: 0.0,
            max_probe_result_bytes_at_5: 0,
            mean_probe_result_bytes_at_8: 0.0,
            max_probe_result_bytes_at_8: 0,
            mean_recall_candidate_hint_bytes: 0.0,
            max_recall_candidate_hint_bytes: 0,
            probe_fetch_content_bytes_per_lead_p95: 0,
            mean_probe_pipeline_bytes: 0.0,
            max_probe_pipeline_bytes: 0,
            max_large_artifact_pipeline_ratio: 0.0,
            citation_get_exactness_violations: 0,
            probe_bait_fetches: 0,
            probe_recovery_case_count: 0,
            probe_recovery_success_rate: 0.0,
            probe_recovery_false_answer_rate: 0.0,
            probe_recovery_false_abstain_rate: 0.0,
            retrieve_recovery_case_count: 0,
            retrieve_recovery_success_rate: 0.0,
            retrieve_recovery_false_answer_rate: 0.0,
            retrieve_recovery_false_abstain_rate: 0.0,
            retrieve_bait_fetches: 0,
            retrieve_forbidden_fetches: 0,
            mean_retrieve_packet_bytes: 0.0,
            max_retrieve_packet_bytes: 0,
            retrieve_latency_p95_ms: 0.0,
            false_answer_rate: 0.0,
            false_abstain_rate: 0.0,
            forbidden_returns: 0,
            slices: BTreeMap::new(),
            budget_violations: 0,
            citation_parity_violations: 0,
            scope_violations: 0,
        };
        let baseline = EvalBaseline {
            schema_version: 1,
            corpus_version: "v1".into(),
            macro_claim_recall: Some(1.0),
            macro_claim_precision: Some(1.0),
            macro_artifact_recall: Some(1.0),
            macro_artifact_precision: Some(1.0),
            max_false_answer_rate: None,
            max_false_abstain_rate: None,
            max_forbidden_returns: None,
            epsilon: 0.02,
        };
        let comparison = compare_baseline(&aggregate, &baseline);
        assert!(!comparison.passed);
        assert_eq!(comparison.regressions.len(), 1);
        assert!(comparison.regressions[0].contains("macro_claim_recall"));
    }
}
