//! Deterministic retrieval regression harness.
//!
//! The harness always creates a fresh temporary store from a versioned JSONL
//! corpus. It never reads or writes the operator's Memoree data directory.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    error::{MemoryError, Result},
    protocol::{
        AmbientContext, ArtifactContent, ArtifactPutInput, ClaimAssertInput, ClaimType,
        ConflictSummary, ContextBuildInput, ContextSource, EntityType, EvidenceLocator, Horizon,
        Operation, RecallInput, RecallPresence, RecallResult, RecencyBiasInput, RelationPutInput,
        RelationType, Request, SearchInput, SearchResult,
    },
    service::MemoryService,
    store::{ArtifactRecord, ClaimRecord, MutationResult, RelationRecord, Store},
};

const EVAL_SCHEMA_VERSION: u32 = 1;

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
    forbidden: Vec<String>,
    #[serde(default)]
    expected_conflicts: Vec<[String; 2]>,
    #[serde(default)]
    gate: CaseGate,
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
    macro_claim_recall: f64,
    macro_claim_precision: f64,
    macro_artifact_recall: f64,
    macro_artifact_precision: f64,
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

#[derive(Debug, Clone, Serialize)]
pub struct RetrievalEvalReport {
    pub schema_version: u32,
    pub corpus_version: String,
    pub isolated_temporary_store: bool,
    pub cases: Vec<RetrievalCaseReport>,
    pub aggregate: RetrievalAggregate,
    pub baseline: BaselineComparison,
    pub hard_failures: Vec<String>,
    pub passed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RetrievalCaseReport {
    pub case_id: String,
    pub gate: String,
    pub presence: RecallPresence,
    pub returned_claims: Vec<String>,
    pub returned_artifacts: Vec<String>,
    pub claim_recall: Option<f64>,
    pub claim_precision: f64,
    pub artifact_recall: Option<f64>,
    pub artifact_precision: f64,
    pub context_used_bytes: usize,
    pub context_max_bytes: usize,
    pub failures: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RetrievalAggregate {
    pub macro_claim_recall: f64,
    pub macro_claim_precision: f64,
    pub macro_artifact_recall: f64,
    pub macro_artifact_precision: f64,
    pub budget_violations: usize,
    pub citation_parity_violations: usize,
    pub scope_violations: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct BaselineComparison {
    pub epsilon: f64,
    pub regressions: Vec<String>,
    pub passed: bool,
}

pub async fn run_retrieval_eval(corpus_dir: &Path) -> Result<RetrievalEvalReport> {
    let corpus_version = corpus_dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| MemoryError::InvalidRequest("corpus directory needs a UTF-8 name".into()))?
        .to_owned();
    let seeds: Vec<SeedRecord> = read_jsonl(&corpus_dir.join("seed.jsonl"))?;
    let cases: Vec<EvalCase> = read_jsonl(&corpus_dir.join("cases.jsonl"))?;
    let baseline: EvalBaseline = read_json(&corpus_dir.join("baseline.json"))?;
    validate_corpus(&corpus_version, &seeds, &cases, &baseline)?;

    let temporary = tempfile::tempdir()?;
    let service = MemoryService::new(Store::open(temporary.path())?);
    let entities = seed_store(&service, &seeds).await?;
    let labels_by_id: BTreeMap<String, String> = entities
        .iter()
        .map(|(label, entity)| (entity.entity_id.clone(), label.clone()))
        .collect();

    let mut case_reports = Vec::new();
    let mut hard_failures = Vec::new();
    let mut budget_violations = 0usize;
    let mut citation_parity_violations = 0usize;
    let mut scope_violations = 0usize;

    for case in &cases {
        let ambient = case.context.ambient();
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
                min_commit_seq: None,
                recency: RecencyBiasInput::default(),
            },
            &ambient,
        )
        .await?;
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

        let returned_claims = labels_for_ids(
            recall.claims.iter().map(|claim| claim.claim_id.as_str()),
            &labels_by_id,
            &mut failures,
        );
        let returned_artifacts = labels_for_ids(
            recall
                .artifact_refs
                .iter()
                .map(|artifact| artifact.artifact_id.as_str()),
            &labels_by_id,
            &mut failures,
        );
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
        for forbidden in &case.forbidden {
            if returned_claims.contains(forbidden) || returned_artifacts.contains(forbidden) {
                failures.push(format!("forbidden label {forbidden} was returned"));
            }
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

        if bundle.used_bytes > bundle.max_bytes || bundle.max_bytes != case.max_context_bytes {
            budget_violations += 1;
            failures.push(format!(
                "context used {}/{} bytes for requested budget {}",
                bundle.used_bytes, bundle.max_bytes, case.max_context_bytes
            ));
        }
        validate_bundle_evidence_rendering(&bundle, &mut failures);

        for label in returned_claims.iter().chain(returned_artifacts.iter()) {
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

        let claim_recall = recall_ratio(&case.relevant_claims, &returned_claims);
        let artifact_recall = recall_ratio(&case.relevant_artifacts, &returned_artifacts);
        let report = RetrievalCaseReport {
            case_id: case.case_id.clone(),
            gate: match case.gate {
                CaseGate::Hard => "hard",
                CaseGate::Report => "report",
            }
            .into(),
            presence: recall.presence,
            returned_claims: returned_claims.iter().cloned().collect(),
            returned_artifacts: returned_artifacts.iter().cloned().collect(),
            claim_recall,
            claim_precision: precision(&case.relevant_claims, &returned_claims),
            artifact_recall,
            artifact_precision: precision(&case.relevant_artifacts, &returned_artifacts),
            context_used_bytes: bundle.used_bytes,
            context_max_bytes: bundle.max_bytes,
            failures: failures.clone(),
        };
        if matches!(case.gate, CaseGate::Hard) {
            hard_failures.extend(
                failures
                    .iter()
                    .map(|failure| format!("{}: {failure}", case.case_id)),
            );
        }
        case_reports.push(report);
    }

    let aggregate = RetrievalAggregate {
        macro_claim_recall: mean(case_reports.iter().filter_map(|case| case.claim_recall)),
        macro_claim_precision: mean(case_reports.iter().map(|case| case.claim_precision)),
        macro_artifact_recall: mean(case_reports.iter().filter_map(|case| case.artifact_recall)),
        macro_artifact_precision: mean(case_reports.iter().map(|case| case.artifact_precision)),
        budget_violations,
        citation_parity_violations,
        scope_violations,
    };
    let baseline = compare_baseline(&aggregate, &baseline);
    let passed = hard_failures.is_empty() && baseline.passed;
    Ok(RetrievalEvalReport {
        schema_version: EVAL_SCHEMA_VERSION,
        corpus_version,
        isolated_temporary_store: true,
        cases: case_reports,
        aggregate,
        baseline,
        hard_failures,
        passed,
    })
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
    if baseline.schema_version != EVAL_SCHEMA_VERSION
        || baseline.corpus_version != corpus_version
        || seeds
            .iter()
            .any(|seed| seed.schema_version != EVAL_SCHEMA_VERSION)
        || cases
            .iter()
            .any(|case| case.schema_version != EVAL_SCHEMA_VERSION)
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
        for label in case
            .relevant_claims
            .iter()
            .chain(case.relevant_artifacts.iter())
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
            } => {
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
                let text = match content {
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

fn labels_for_ids<'a>(
    ids: impl Iterator<Item = &'a str>,
    labels_by_id: &BTreeMap<String, String>,
    failures: &mut Vec<String>,
) -> BTreeSet<String> {
    let mut labels = BTreeSet::new();
    for id in ids {
        match labels_by_id.get(id) {
            Some(label) => {
                labels.insert(label.clone());
            }
            None => failures.push(format!("returned unseeded entity {id}")),
        }
    }
    labels
}

fn check_expected_labels(
    kind: &str,
    expected: &[String],
    returned: &BTreeSet<String>,
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

fn recall_ratio(expected: &[String], returned: &BTreeSet<String>) -> Option<f64> {
    if expected.is_empty() {
        return None;
    }
    let relevant = expected
        .iter()
        .filter(|label| returned.contains(label.as_str()))
        .count();
    Some(relevant as f64 / expected.len() as f64)
}

fn precision(expected: &[String], returned: &BTreeSet<String>) -> f64 {
    if returned.is_empty() {
        return if expected.is_empty() { 1.0 } else { 0.0 };
    }
    let expected: BTreeSet<&str> = expected.iter().map(String::as_str).collect();
    returned
        .iter()
        .filter(|label| expected.contains(label.as_str()))
        .count() as f64
        / returned.len() as f64
}

fn mean(values: impl Iterator<Item = f64>) -> f64 {
    let values: Vec<f64> = values.collect();
    if values.is_empty() {
        1.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn compare_baseline(aggregate: &RetrievalAggregate, baseline: &EvalBaseline) -> BaselineComparison {
    let mut regressions = Vec::new();
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
        if current + baseline.epsilon < expected {
            regressions.push(format!(
                "{name} regressed from {expected:.6} to {current:.6} (epsilon {:.6})",
                baseline.epsilon
            ));
        }
    }
    BaselineComparison {
        epsilon: baseline.epsilon,
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
            serde_json::to_value(&first).unwrap(),
            serde_json::to_value(&second).unwrap()
        );
    }

    #[test]
    fn baseline_comparison_rejects_a_metric_regression() {
        let aggregate = RetrievalAggregate {
            macro_claim_recall: 0.75,
            macro_claim_precision: 1.0,
            macro_artifact_recall: 1.0,
            macro_artifact_precision: 1.0,
            budget_violations: 0,
            citation_parity_violations: 0,
            scope_violations: 0,
        };
        let baseline = EvalBaseline {
            schema_version: 1,
            corpus_version: "v1".into(),
            macro_claim_recall: 1.0,
            macro_claim_precision: 1.0,
            macro_artifact_recall: 1.0,
            macro_artifact_precision: 1.0,
            epsilon: 0.02,
        };
        let comparison = compare_baseline(&aggregate, &baseline);
        assert!(!comparison.passed);
        assert_eq!(comparison.regressions.len(), 1);
        assert!(comparison.regressions[0].contains("macro_claim_recall"));
    }
}
