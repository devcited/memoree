//! Deterministic calibration and held-out evaluation for the local reranker.
//!
//! This evaluator consumes authored pair corpora directly. It does not touch
//! the operator's memory store and never downloads model files.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    time::Instant,
};

use serde::{Deserialize, Serialize};

use crate::{
    error::{MemoryError, Result},
    semantic::CandleReranker,
};

const RERANKER_PAIR_SCHEMA_VERSION: u32 = 1;
const CALIBRATION_METHOD: &str = "raw_logit_min_threshold_precision_0.80_recall_0.60_v1";
const MIN_PRECISION: f64 = 0.80;
const MIN_RECALL: f64 = 0.60;
const MAX_JACKKNIFE_THRESHOLD_SHIFT: f64 = 0.25;
const MIN_POWERED_POSITIVES_PER_SURFACE: usize = 60;
const MIN_POWERED_NEGATIVES_PER_SURFACE: usize = 120;
const SCORE_BINS: &[f64] = &[-10.0, -5.0, -2.0, 0.0, 2.0, 5.0, 10.0];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum PairSurface {
    Claim,
    Artifact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PairPolarity {
    Supports,
    Contradicts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PairTemporalStatus {
    Current,
    FuturePlan,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RerankerPair {
    schema_version: u32,
    pair_id: String,
    group: String,
    surface: PairSurface,
    query: String,
    passage: String,
    relevant: bool,
    negative_kind: Option<String>,
    #[serde(default)]
    ordering_grade: Option<u8>,
    #[serde(default)]
    polarity: Option<PairPolarity>,
    #[serde(default)]
    temporal_status: Option<PairTemporalStatus>,
    #[serde(default)]
    conflict_group_id: Option<String>,
    source: String,
}

#[derive(Debug, Clone)]
struct ScoredPair {
    pair: RerankerPair,
    logit: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RerankerPairEvalReport {
    pub schema_version: u32,
    pub model_id: String,
    pub model_revision: String,
    pub calibration_method: String,
    pub calibration_corpus_hash: String,
    pub heldout_corpus_hash: String,
    pub calibration_version: Option<String>,
    pub scoring_latency_ms: f64,
    pub calibration: BTreeMap<String, SurfaceCalibrationReport>,
    pub heldout: BTreeMap<String, SurfaceEvaluationReport>,
    pub calibration_conflict_completeness: Option<f64>,
    pub heldout_conflict_completeness: Option<f64>,
    pub statistically_powered: bool,
    pub passed: bool,
    pub failures: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SurfaceCalibrationReport {
    pub pair_count: usize,
    pub positive_count: usize,
    pub negative_count: usize,
    pub threshold: Option<f64>,
    pub precision: f64,
    pub recall: f64,
    pub false_positive_count: usize,
    pub false_negative_count: usize,
    pub macro_query_recall: f64,
    pub jackknife_threshold_min: Option<f64>,
    pub jackknife_threshold_max: Option<f64>,
    pub jackknife_max_shift: Option<f64>,
    pub threshold_stable: bool,
    pub negative_length_logit_correlation: Option<f64>,
    pub positive_logit_histogram: Vec<usize>,
    pub negative_logit_histogram: Vec<usize>,
    pub false_accepts_by_kind: BTreeMap<String, usize>,
    pub negative_kind_logit_summary: BTreeMap<String, LogitSummary>,
    pub lowest_positive_pairs: Vec<PairScoreDiagnostic>,
    pub highest_negative_pairs: Vec<PairScoreDiagnostic>,
    pub facet_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SurfaceEvaluationReport {
    pub pair_count: usize,
    pub positive_count: usize,
    pub negative_count: usize,
    pub threshold: f64,
    pub precision: f64,
    pub recall: f64,
    pub false_positive_count: usize,
    pub false_negative_count: usize,
    pub macro_query_recall: f64,
    pub positive_logit_histogram: Vec<usize>,
    pub negative_logit_histogram: Vec<usize>,
    pub false_accepts_by_kind: BTreeMap<String, usize>,
    pub negative_kind_logit_summary: BTreeMap<String, LogitSummary>,
    pub lowest_positive_pairs: Vec<PairScoreDiagnostic>,
    pub highest_negative_pairs: Vec<PairScoreDiagnostic>,
    pub facet_counts: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LogitSummary {
    pub count: usize,
    pub minimum: f64,
    pub mean: f64,
    pub maximum: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct PairScoreDiagnostic {
    pub pair_id: String,
    pub logit: f64,
}

#[derive(Debug, Clone, Copy)]
struct DecisionMetrics {
    precision: f64,
    recall: f64,
    false_positive_count: usize,
    false_negative_count: usize,
}

pub fn evaluate_reranker_pairs(
    model_directory: &Path,
    calibration_path: &Path,
    heldout_path: &Path,
    model_id: &str,
    model_revision: &str,
) -> Result<RerankerPairEvalReport> {
    if model_id.trim().is_empty() || model_revision.trim().is_empty() {
        return Err(MemoryError::InvalidRequest(
            "reranker model id and revision must be explicit".into(),
        ));
    }
    let (calibration_pairs, calibration_hash) = load_pair_corpus(calibration_path)?;
    let (heldout_pairs, heldout_hash) = load_pair_corpus(heldout_path)?;
    ensure_disjoint(&calibration_pairs, &heldout_pairs)?;

    let started = Instant::now();
    let mut model = CandleReranker::load(model_directory)?;
    let calibration_scores = score_pairs(&mut model, calibration_pairs)?;
    let heldout_scores = score_pairs(&mut model, heldout_pairs)?;
    let scoring_latency_ms = started.elapsed().as_secs_f64() * 1000.0;

    let mut calibration = BTreeMap::new();
    let mut heldout = BTreeMap::new();
    let mut thresholds = BTreeMap::new();
    let mut failures = Vec::new();
    let mut statistically_powered = true;
    for surface in [PairSurface::Claim, PairSurface::Artifact] {
        let name = surface_name(surface).to_owned();
        let surface_calibration = calibration_scores
            .iter()
            .filter(|pair| pair.pair.surface == surface)
            .cloned()
            .collect::<Vec<_>>();
        let report = calibrate_surface(&surface_calibration);
        if report.positive_count < MIN_POWERED_POSITIVES_PER_SURFACE
            || report.negative_count < MIN_POWERED_NEGATIVES_PER_SURFACE
        {
            statistically_powered = false;
            failures.push(format!(
                "{name} calibration has {}/{} positive/negative pairs; require at least {MIN_POWERED_POSITIVES_PER_SURFACE}/{MIN_POWERED_NEGATIVES_PER_SURFACE}",
                report.positive_count, report.negative_count
            ));
        }
        let Some(threshold) = report.threshold else {
            failures.push(format!(
                "{name} calibration has no raw-logit threshold satisfying precision >= {MIN_PRECISION:.2} and recall >= {MIN_RECALL:.2}"
            ));
            calibration.insert(name, report);
            continue;
        };
        if !report.threshold_stable {
            failures.push(format!(
                "{name} calibration threshold is unstable under leave-one-group-out jackknife"
            ));
        }
        thresholds.insert(surface, threshold);
        calibration.insert(name.clone(), report);

        let surface_heldout = heldout_scores
            .iter()
            .filter(|pair| pair.pair.surface == surface)
            .cloned()
            .collect::<Vec<_>>();
        let evaluation = evaluate_surface(&surface_heldout, threshold);
        if evaluation.positive_count < MIN_POWERED_POSITIVES_PER_SURFACE
            || evaluation.negative_count < MIN_POWERED_NEGATIVES_PER_SURFACE
        {
            statistically_powered = false;
            failures.push(format!(
                "{name} held-out set has {}/{} positive/negative pairs; require at least {MIN_POWERED_POSITIVES_PER_SURFACE}/{MIN_POWERED_NEGATIVES_PER_SURFACE}",
                evaluation.positive_count, evaluation.negative_count
            ));
        }
        if evaluation.precision + f64::EPSILON < MIN_PRECISION {
            failures.push(format!(
                "{name} held-out precision {:.3} is below {MIN_PRECISION:.2}",
                evaluation.precision
            ));
        }
        if evaluation.recall + f64::EPSILON < MIN_RECALL {
            failures.push(format!(
                "{name} held-out recall {:.3} is below {MIN_RECALL:.2}",
                evaluation.recall
            ));
        }
        heldout.insert(name, evaluation);
    }

    let calibration_version = (thresholds.len() == 2).then(|| {
        let material = format!(
            "{CALIBRATION_METHOD}\n{model_id}\n{model_revision}\n{calibration_hash}\nclaim={:.9}\nartifact={:.9}",
            thresholds[&PairSurface::Claim], thresholds[&PairSurface::Artifact]
        );
        format!("reranker_calibration_v1_{}", &blake3::hash(material.as_bytes()).to_hex()[..16])
    });
    let calibration_conflict_completeness = conflict_completeness(&calibration_scores, &thresholds);
    let heldout_conflict_completeness = conflict_completeness(&heldout_scores, &thresholds);
    if heldout_conflict_completeness.is_some_and(|score| score + f64::EPSILON < 0.90) {
        failures.push(format!(
            "held-out conflict completeness {:.3} is below 0.90",
            heldout_conflict_completeness.unwrap_or_default()
        ));
    }
    Ok(RerankerPairEvalReport {
        schema_version: RERANKER_PAIR_SCHEMA_VERSION,
        model_id: model_id.into(),
        model_revision: model_revision.into(),
        calibration_method: CALIBRATION_METHOD.into(),
        calibration_corpus_hash: calibration_hash,
        heldout_corpus_hash: heldout_hash,
        calibration_version,
        scoring_latency_ms,
        calibration,
        heldout,
        calibration_conflict_completeness,
        heldout_conflict_completeness,
        statistically_powered,
        passed: failures.is_empty(),
        failures,
    })
}

fn load_pair_corpus(path: &Path) -> Result<(Vec<RerankerPair>, String)> {
    let bytes = fs::read(path)?;
    let hash = blake3::hash(&bytes).to_hex().to_string();
    let text = std::str::from_utf8(&bytes).map_err(|error| {
        MemoryError::InvalidRequest(format!("{} is not UTF-8: {error}", path.display()))
    })?;
    let mut pairs = Vec::new();
    let mut ids = BTreeSet::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let pair: RerankerPair = serde_json::from_str(line).map_err(|error| {
            MemoryError::InvalidRequest(format!(
                "{} line {} is invalid: {error}",
                path.display(),
                index + 1
            ))
        })?;
        if pair.schema_version != RERANKER_PAIR_SCHEMA_VERSION
            || pair.pair_id.trim().is_empty()
            || pair.group.trim().is_empty()
            || pair.query.trim().is_empty()
            || pair.passage.trim().is_empty()
            || pair.source != "authored_realistic"
            || (pair.relevant && pair.negative_kind.is_some())
            || (!pair.relevant && pair.negative_kind.as_deref().is_none_or(str::is_empty))
            || pair.ordering_grade.is_some_and(|grade| grade > 2)
            || (pair.relevant && pair.ordering_grade == Some(0))
            || (!pair.relevant && pair.ordering_grade.is_some_and(|grade| grade != 0))
            || pair.conflict_group_id.as_deref().is_some_and(str::is_empty)
        {
            return Err(MemoryError::InvalidRequest(format!(
                "{} line {} violates the reranker pair contract",
                path.display(),
                index + 1
            )));
        }
        if !ids.insert(pair.pair_id.clone()) {
            return Err(MemoryError::InvalidRequest(format!(
                "{} contains duplicate pair id {}",
                path.display(),
                pair.pair_id
            )));
        }
        pairs.push(pair);
    }
    if pairs.is_empty() {
        return Err(MemoryError::InvalidRequest(format!(
            "{} contains no reranker pairs",
            path.display()
        )));
    }
    for surface in [PairSurface::Claim, PairSurface::Artifact] {
        let positives = pairs
            .iter()
            .filter(|pair| pair.surface == surface && pair.relevant)
            .count();
        let negatives = pairs
            .iter()
            .filter(|pair| pair.surface == surface && !pair.relevant)
            .count();
        if positives < 5 || negatives < 10 {
            return Err(MemoryError::InvalidRequest(format!(
                "{} needs at least 5 positives and 10 negatives for {}",
                path.display(),
                surface_name(surface)
            )));
        }
    }
    Ok((pairs, hash))
}

fn ensure_disjoint(calibration: &[RerankerPair], heldout: &[RerankerPair]) -> Result<()> {
    let calibration_ids = calibration
        .iter()
        .map(|pair| pair.pair_id.as_str())
        .collect::<BTreeSet<_>>();
    if let Some(overlap) = heldout
        .iter()
        .find(|pair| calibration_ids.contains(pair.pair_id.as_str()))
    {
        return Err(MemoryError::InvalidRequest(format!(
            "pair {} appears in calibration and held-out corpora",
            overlap.pair_id
        )));
    }
    Ok(())
}

fn score_pairs(model: &mut CandleReranker, pairs: Vec<RerankerPair>) -> Result<Vec<ScoredPair>> {
    let mut by_query = BTreeMap::<String, Vec<usize>>::new();
    for (index, pair) in pairs.iter().enumerate() {
        by_query.entry(pair.query.clone()).or_default().push(index);
    }
    let mut logits = vec![None; pairs.len()];
    for (query, indices) in by_query {
        let passages = indices
            .iter()
            .map(|index| pairs[*index].passage.as_str())
            .collect::<Vec<_>>();
        let scores = model.score(&query, &passages)?;
        if scores.len() != indices.len() {
            return Err(MemoryError::Integrity(
                "reranker returned the wrong pair score count".into(),
            ));
        }
        for (index, score) in indices.into_iter().zip(scores) {
            if !score.is_finite() {
                return Err(MemoryError::Integrity(
                    "reranker returned a non-finite logit".into(),
                ));
            }
            logits[index] = Some(f64::from(score));
        }
    }
    pairs
        .into_iter()
        .zip(logits)
        .map(|(pair, logit)| {
            Ok(ScoredPair {
                pair,
                logit: logit
                    .ok_or_else(|| MemoryError::Integrity("reranker pair was not scored".into()))?,
            })
        })
        .collect()
}

fn calibrate_surface(pairs: &[ScoredPair]) -> SurfaceCalibrationReport {
    let threshold = select_threshold(pairs);
    let metrics = threshold.map_or(
        DecisionMetrics {
            precision: 0.0,
            recall: 0.0,
            false_positive_count: pairs.iter().filter(|pair| !pair.pair.relevant).count(),
            false_negative_count: pairs.iter().filter(|pair| pair.pair.relevant).count(),
        },
        |threshold| decision_metrics(pairs, threshold),
    );
    let groups = pairs
        .iter()
        .map(|pair| pair.pair.group.clone())
        .collect::<BTreeSet<_>>();
    let jackknife = groups
        .iter()
        .filter_map(|left_out| {
            let fold = pairs
                .iter()
                .filter(|pair| &pair.pair.group != left_out)
                .cloned()
                .collect::<Vec<_>>();
            select_threshold(&fold)
        })
        .collect::<Vec<_>>();
    let jackknife_complete = jackknife.len() == groups.len() && !jackknife.is_empty();
    let jackknife_min = jackknife.iter().copied().min_by(f64::total_cmp);
    let jackknife_max = jackknife.iter().copied().max_by(f64::total_cmp);
    let max_shift = threshold.and_then(|selected| {
        jackknife
            .iter()
            .map(|fold| (fold - selected).abs())
            .max_by(f64::total_cmp)
    });
    let threshold_stable =
        jackknife_complete && max_shift.is_some_and(|shift| shift <= MAX_JACKKNIFE_THRESHOLD_SHIFT);
    SurfaceCalibrationReport {
        pair_count: pairs.len(),
        positive_count: pairs.iter().filter(|pair| pair.pair.relevant).count(),
        negative_count: pairs.iter().filter(|pair| !pair.pair.relevant).count(),
        threshold,
        precision: metrics.precision,
        recall: metrics.recall,
        false_positive_count: metrics.false_positive_count,
        false_negative_count: metrics.false_negative_count,
        macro_query_recall: macro_query_recall(pairs, threshold),
        jackknife_threshold_min: jackknife_min,
        jackknife_threshold_max: jackknife_max,
        jackknife_max_shift: max_shift,
        threshold_stable,
        negative_length_logit_correlation: negative_length_logit_correlation(pairs),
        positive_logit_histogram: score_histogram(pairs, true),
        negative_logit_histogram: score_histogram(pairs, false),
        false_accepts_by_kind: threshold
            .map(|threshold| false_accepts_by_kind(pairs, threshold))
            .unwrap_or_default(),
        negative_kind_logit_summary: negative_kind_logit_summary(pairs),
        lowest_positive_pairs: score_extremes(pairs, true, false),
        highest_negative_pairs: score_extremes(pairs, false, true),
        facet_counts: facet_counts(pairs),
    }
}

fn evaluate_surface(pairs: &[ScoredPair], threshold: f64) -> SurfaceEvaluationReport {
    let metrics = decision_metrics(pairs, threshold);
    SurfaceEvaluationReport {
        pair_count: pairs.len(),
        positive_count: pairs.iter().filter(|pair| pair.pair.relevant).count(),
        negative_count: pairs.iter().filter(|pair| !pair.pair.relevant).count(),
        threshold,
        precision: metrics.precision,
        recall: metrics.recall,
        false_positive_count: metrics.false_positive_count,
        false_negative_count: metrics.false_negative_count,
        macro_query_recall: macro_query_recall(pairs, Some(threshold)),
        positive_logit_histogram: score_histogram(pairs, true),
        negative_logit_histogram: score_histogram(pairs, false),
        false_accepts_by_kind: false_accepts_by_kind(pairs, threshold),
        negative_kind_logit_summary: negative_kind_logit_summary(pairs),
        lowest_positive_pairs: score_extremes(pairs, true, false),
        highest_negative_pairs: score_extremes(pairs, false, true),
        facet_counts: facet_counts(pairs),
    }
}

fn facet_counts(pairs: &[ScoredPair]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for pair in pairs {
        let polarity = match pair.pair.polarity {
            Some(PairPolarity::Supports) => Some("polarity_supports"),
            Some(PairPolarity::Contradicts) => Some("polarity_contradicts"),
            None => None,
        };
        let temporal = match pair.pair.temporal_status {
            Some(PairTemporalStatus::Current) => Some("temporal_current"),
            Some(PairTemporalStatus::FuturePlan) => Some("temporal_future_plan"),
            None => None,
        };
        for facet in polarity.into_iter().chain(temporal) {
            *counts.entry(facet.into()).or_default() += 1;
        }
        if let Some(grade) = pair.pair.ordering_grade {
            *counts.entry(format!("ordering_grade_{grade}")).or_default() += 1;
        }
    }
    counts
}

fn score_extremes(
    pairs: &[ScoredPair],
    relevant: bool,
    descending: bool,
) -> Vec<PairScoreDiagnostic> {
    let mut selected = pairs
        .iter()
        .filter(|pair| pair.pair.relevant == relevant)
        .collect::<Vec<_>>();
    selected.sort_by(|left, right| {
        let order = left.logit.total_cmp(&right.logit);
        (if descending { order.reverse() } else { order })
            .then_with(|| left.pair.pair_id.cmp(&right.pair.pair_id))
    });
    selected
        .into_iter()
        .take(5)
        .map(|pair| PairScoreDiagnostic {
            pair_id: pair.pair.pair_id.clone(),
            logit: pair.logit,
        })
        .collect()
}

fn macro_query_recall(pairs: &[ScoredPair], threshold: Option<f64>) -> f64 {
    let Some(threshold) = threshold else {
        return 0.0;
    };
    let mut groups = BTreeMap::<&str, (usize, usize)>::new();
    for pair in pairs.iter().filter(|pair| pair.pair.relevant) {
        let entry = groups.entry(pair.pair.group.as_str()).or_default();
        entry.1 += 1;
        entry.0 += usize::from(pair.logit >= threshold);
    }
    if groups.is_empty() {
        0.0
    } else {
        groups
            .values()
            .map(|(qualified, relevant)| ratio(*qualified, *relevant))
            .sum::<f64>()
            / groups.len() as f64
    }
}

fn conflict_completeness(
    pairs: &[ScoredPair],
    thresholds: &BTreeMap<PairSurface, f64>,
) -> Option<f64> {
    if thresholds.len() != 2 {
        return None;
    }
    let mut groups = BTreeMap::<&str, Vec<&ScoredPair>>::new();
    for pair in pairs
        .iter()
        .filter(|pair| pair.pair.relevant && pair.pair.conflict_group_id.is_some())
    {
        groups
            .entry(pair.pair.conflict_group_id.as_deref().unwrap_or_default())
            .or_default()
            .push(pair);
    }
    groups.retain(|_, members| members.len() >= 2);
    if groups.is_empty() {
        return None;
    }
    Some(ratio(
        groups
            .values()
            .filter(|members| {
                members
                    .iter()
                    .all(|pair| pair.logit >= thresholds[&pair.pair.surface])
            })
            .count(),
        groups.len(),
    ))
}

fn negative_kind_logit_summary(pairs: &[ScoredPair]) -> BTreeMap<String, LogitSummary> {
    let mut grouped = BTreeMap::<String, Vec<f64>>::new();
    for pair in pairs.iter().filter(|pair| !pair.pair.relevant) {
        grouped
            .entry(
                pair.pair
                    .negative_kind
                    .clone()
                    .unwrap_or_else(|| "unknown".into()),
            )
            .or_default()
            .push(pair.logit);
    }
    grouped
        .into_iter()
        .map(|(kind, values)| {
            let minimum = values.iter().copied().min_by(f64::total_cmp).unwrap_or(0.0);
            let maximum = values.iter().copied().max_by(f64::total_cmp).unwrap_or(0.0);
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            (
                kind,
                LogitSummary {
                    count: values.len(),
                    minimum,
                    mean,
                    maximum,
                },
            )
        })
        .collect()
}

fn select_threshold(pairs: &[ScoredPair]) -> Option<f64> {
    let mut thresholds = pairs.iter().map(|pair| pair.logit).collect::<Vec<_>>();
    thresholds.sort_by(f64::total_cmp);
    thresholds.dedup_by(|left, right| left.total_cmp(right).is_eq());
    thresholds.into_iter().find(|threshold| {
        let metrics = decision_metrics(pairs, *threshold);
        metrics.precision + f64::EPSILON >= MIN_PRECISION
            && metrics.recall + f64::EPSILON >= MIN_RECALL
    })
}

fn decision_metrics(pairs: &[ScoredPair], threshold: f64) -> DecisionMetrics {
    let mut true_positive = 0usize;
    let mut false_positive = 0usize;
    let mut false_negative = 0usize;
    for pair in pairs {
        let qualified = pair.logit >= threshold;
        match (pair.pair.relevant, qualified) {
            (true, true) => true_positive += 1,
            (false, true) => false_positive += 1,
            (true, false) => false_negative += 1,
            (false, false) => {}
        }
    }
    DecisionMetrics {
        precision: ratio(true_positive, true_positive + false_positive),
        recall: ratio(true_positive, true_positive + false_negative),
        false_positive_count: false_positive,
        false_negative_count: false_negative,
    }
}

fn false_accepts_by_kind(pairs: &[ScoredPair], threshold: f64) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for pair in pairs
        .iter()
        .filter(|pair| !pair.pair.relevant && pair.logit >= threshold)
    {
        *counts
            .entry(
                pair.pair
                    .negative_kind
                    .clone()
                    .unwrap_or_else(|| "unknown".into()),
            )
            .or_default() += 1;
    }
    counts
}

fn score_histogram(pairs: &[ScoredPair], relevant: bool) -> Vec<usize> {
    let mut counts = vec![0usize; SCORE_BINS.len() + 1];
    for pair in pairs.iter().filter(|pair| pair.pair.relevant == relevant) {
        let bin = SCORE_BINS.partition_point(|boundary| pair.logit >= *boundary);
        counts[bin] += 1;
    }
    counts
}

fn negative_length_logit_correlation(pairs: &[ScoredPair]) -> Option<f64> {
    let values = pairs
        .iter()
        .filter(|pair| !pair.pair.relevant)
        .map(|pair| (pair.pair.passage.len() as f64, pair.logit))
        .collect::<Vec<_>>();
    if values.len() < 3 {
        return None;
    }
    let mean_x = values.iter().map(|(x, _)| x).sum::<f64>() / values.len() as f64;
    let mean_y = values.iter().map(|(_, y)| y).sum::<f64>() / values.len() as f64;
    let covariance = values
        .iter()
        .map(|(x, y)| (x - mean_x) * (y - mean_y))
        .sum::<f64>();
    let x_variance = values
        .iter()
        .map(|(x, _)| (x - mean_x).powi(2))
        .sum::<f64>();
    let y_variance = values
        .iter()
        .map(|(_, y)| (y - mean_y).powi(2))
        .sum::<f64>();
    let denominator = (x_variance * y_variance).sqrt();
    (denominator > f64::EPSILON).then(|| covariance / denominator)
}

fn ratio(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn surface_name(surface: PairSurface) -> &'static str {
    match surface {
        PairSurface::Claim => "claim",
        PairSurface::Artifact => "artifact",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair(id: &str, group: &str, relevant: bool, logit: f64) -> ScoredPair {
        ScoredPair {
            pair: RerankerPair {
                schema_version: 1,
                pair_id: id.into(),
                group: group.into(),
                surface: PairSurface::Claim,
                query: "query".into(),
                passage: "passage".into(),
                relevant,
                negative_kind: (!relevant).then(|| "hard_negative".into()),
                ordering_grade: None,
                polarity: None,
                temporal_status: None,
                conflict_group_id: None,
                source: "authored_realistic".into(),
            },
            logit,
        }
    }

    #[test]
    fn raw_threshold_selection_is_smallest_feasible_boundary() {
        let pairs = vec![
            pair("p1", "one", true, 3.0),
            pair("p2", "two", true, 2.0),
            pair("p3", "three", true, 1.0),
            pair("n1", "one", false, 1.5),
            pair("n2", "two", false, -1.0),
        ];
        assert_eq!(select_threshold(&pairs), Some(2.0));
        let metrics = decision_metrics(&pairs, 2.0);
        assert_eq!(metrics.precision, 1.0);
        assert!((metrics.recall - 2.0 / 3.0).abs() < 1e-12);
    }

    #[test]
    fn histogram_and_false_accepts_do_not_expose_passages() {
        let pairs = vec![pair("p", "one", true, 4.0), pair("n", "two", false, 3.0)];
        assert_eq!(score_histogram(&pairs, true).iter().sum::<usize>(), 1);
        assert_eq!(false_accepts_by_kind(&pairs, 2.0)["hard_negative"], 1);
    }
}
