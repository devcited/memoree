//! Opt-in, project-local operational metrics and explicit paired experiments.
//!
//! This store is deliberately separate from authoritative memory and from the
//! disposable project index. Its schema has a closed numeric/categorical
//! allowlist: queries, retrieved content, citations, paths, prompts, free-text
//! labels, and raw error messages have no column in which they can be stored.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use chrono::Utc;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    context::{
        LocalProjectSettings, MARKER_FILE, Marker, MetricsConfig, find_marker,
        load_local_project_settings, local_project_settings_path, read_marker,
        update_local_project_settings,
    },
    error::{MemoryError, Result},
};

const METRICS_SCHEMA_VERSION: i64 = 2;
const METRICS_DIRECTORY: &str = "metrics";
const METRICS_DATABASE: &str = "metrics.sqlite3";
const MINUTE_SECONDS: i64 = 60;
const MAX_REPORT_DAYS: u32 = 365;

const EVENT_OPERATIONS: &[&str] = &[
    "memory.retrieve",
    "memory.recall",
    "memory.probe",
    "memory.search",
    "context.build",
    "feedback.record",
    "project.index",
    "project.map",
    "project.search",
    "project.get",
    "project.watch",
];
const EVENT_OUTCOMES: &[&str] = &[
    "qualified",
    "artifacts_only",
    "recovered",
    "abstained",
    "hits",
    "fallback",
    "empty",
    "ok",
    "error",
    "feedback_useful",
    "feedback_miss",
    "feedback_incorrect",
    "feedback_stale",
];
const ERROR_CATEGORIES: &[&str] = &[
    "none",
    "invalid_request",
    "not_found",
    "citation_error",
    "conflict",
    "index_not_ready",
    "scope_violation",
    "content_too_large",
    "integrity_error",
    "unsupported_version",
    "config_error",
    "transport_error",
    "reasoner_error",
    "internal_error",
    "timeout",
];
const RUNTIME_STATES: &[&str] = &[
    "unknown",
    "ready",
    "disabled",
    "unavailable",
    "stale",
    "degraded",
    "breaker_open",
];

#[derive(Debug, Clone)]
pub struct MetricsStore {
    root: PathBuf,
    marker: Marker,
    data_dir: PathBuf,
    directory: PathBuf,
    database_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricEvent {
    pub operation: String,
    pub outcome: String,
    pub error_category: String,
    pub duration_ms: f64,
    pub recall_ms: Option<f64>,
    pub probe_ms: Option<f64>,
    pub citation_fetch_ms: Option<f64>,
    pub model_load_ms: Option<f64>,
    pub inference_ms: Option<f64>,
    pub response_bytes: Option<u64>,
    pub qualified_claims: Option<u64>,
    pub qualified_artifacts: Option<u64>,
    pub recovery_references: Option<u64>,
    pub result_count: Option<u64>,
    pub indexed_files: Option<u64>,
    pub indexed_bytes: Option<u64>,
    pub changed_files: Option<u64>,
    pub changed_bytes: Option<u64>,
    pub project_edges: Option<u64>,
    pub project_edge_truncations: Option<u64>,
    pub project_mentions: Option<u64>,
    pub project_mentions_truncated: Option<bool>,
    pub stale: Option<bool>,
    pub reindex_attempted: Option<bool>,
    pub semantic_state: String,
    pub reranker_state: String,
    pub breaker_open: bool,
}

impl MetricEvent {
    pub fn new(operation: &str, outcome: &str, duration_ms: f64) -> Self {
        Self {
            operation: operation.into(),
            outcome: outcome.into(),
            error_category: "none".into(),
            duration_ms,
            recall_ms: None,
            probe_ms: None,
            citation_fetch_ms: None,
            model_load_ms: None,
            inference_ms: None,
            response_bytes: None,
            qualified_claims: None,
            qualified_artifacts: None,
            recovery_references: None,
            result_count: None,
            indexed_files: None,
            indexed_bytes: None,
            changed_files: None,
            changed_bytes: None,
            project_edges: None,
            project_edge_truncations: None,
            project_mentions: None,
            project_mentions_truncated: None,
            stale: None,
            reindex_attempted: None,
            semantic_state: "unknown".into(),
            reranker_state: "unknown".into(),
            breaker_open: false,
        }
    }

    pub fn error(operation: &str, duration_ms: f64, category: &str) -> Self {
        let mut event = Self::new(operation, "error", duration_ms);
        event.error_category = safe_error_category(category).into();
        event
    }

    fn validate(&self) -> Result<()> {
        if !EVENT_OPERATIONS.contains(&self.operation.as_str())
            || !EVENT_OUTCOMES.contains(&self.outcome.as_str())
            || !ERROR_CATEGORIES.contains(&self.error_category.as_str())
            || !RUNTIME_STATES.contains(&self.semantic_state.as_str())
            || !RUNTIME_STATES.contains(&self.reranker_state.as_str())
        {
            return Err(MemoryError::Integrity(
                "metrics event contains a value outside the closed allowlist".into(),
            ));
        }
        for value in [
            Some(self.duration_ms),
            self.recall_ms,
            self.probe_ms,
            self.citation_fetch_ms,
            self.model_load_ms,
            self.inference_ms,
        ]
        .into_iter()
        .flatten()
        {
            if !value.is_finite() || !(0.0..=86_400_000.0).contains(&value) {
                return Err(MemoryError::InvalidRequest(
                    "metrics durations must be finite and between zero and one day".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricsStatus {
    pub schema_version: i64,
    pub project_id: String,
    pub enabled: bool,
    pub retention_days: u32,
    pub max_database_bytes: u64,
    pub sample_rate: f64,
    pub database_exists: bool,
    pub database_bytes: u64,
    pub recorded_events: u64,
    pub experiments: u64,
    pub oldest_event_minute: Option<i64>,
    pub newest_event_minute: Option<i64>,
    pub query_recorded: bool,
    pub content_recorded: bool,
    pub citation_recorded: bool,
    pub path_recorded: bool,
    pub network_telemetry: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricDistribution {
    pub min: f64,
    pub p50: f64,
    pub p95: f64,
    pub max: f64,
    pub mean: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct OperationMetrics {
    pub operation: String,
    pub count: usize,
    pub duration_ms: MetricDistribution,
    pub outcomes: BTreeMap<String, usize>,
    pub error_count: usize,
    pub error_rate: f64,
    pub qualified_rate: f64,
    pub recovery_rate: f64,
    pub abstention_rate: f64,
    pub feedback_useful_rate: Option<f64>,
    pub response_bytes: MetricDistribution,
    pub recall_ms: MetricDistribution,
    pub probe_ms: MetricDistribution,
    pub citation_fetch_ms: MetricDistribution,
    pub model_load_ms: MetricDistribution,
    pub inference_ms: MetricDistribution,
    pub qualified_claims: MetricDistribution,
    pub qualified_artifacts: MetricDistribution,
    pub recovery_references: MetricDistribution,
    pub result_count: MetricDistribution,
    pub indexed_files: MetricDistribution,
    pub indexed_bytes: MetricDistribution,
    pub changed_files: MetricDistribution,
    pub changed_bytes: MetricDistribution,
    pub project_edges: MetricDistribution,
    pub project_edge_truncations: MetricDistribution,
    pub project_mentions: MetricDistribution,
    pub project_mentions_truncated_count: usize,
    pub breaker_open_count: usize,
    pub stale_count: usize,
    pub reindex_attempted_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricsReport {
    pub schema_version: i64,
    pub project_id: String,
    pub since_minutes: i64,
    pub generated_at_minute: i64,
    pub database_bytes: u64,
    pub max_database_bytes: u64,
    pub operations: Vec<OperationMetrics>,
    pub caveats: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricsDoctorReport {
    pub schema_version: i64,
    pub project_id: String,
    pub enabled: bool,
    pub database_exists: bool,
    pub integrity_ok: bool,
    pub pragmas_ok: bool,
    pub closed_schema_ok: bool,
    pub categorical_allowlists_ok: bool,
    pub private_permissions_ok: bool,
    pub outside_project_tree: bool,
    pub within_size_cap: bool,
    pub within_retention: bool,
    pub status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExperimentPrimaryMetric {
    Tokens,
    Elapsed,
}

impl ExperimentPrimaryMetric {
    fn as_str(self) -> &'static str {
        match self {
            Self::Tokens => "tokens",
            Self::Elapsed => "elapsed",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "tokens" => Ok(Self::Tokens),
            "elapsed" => Ok(Self::Elapsed),
            _ => Err(MemoryError::Integrity(
                "experiment primary metric is outside the allowlist".into(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExperimentArm {
    Memory,
    Baseline,
}

impl ExperimentArm {
    fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Baseline => "baseline",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ExperimentBeginReport {
    pub experiment_id: String,
    pub primary_metric: ExperimentPrimaryMetric,
    pub task_content_recorded: bool,
    pub next: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExperimentPairReport {
    pub experiment_id: String,
    pub pair_id: String,
    pub first_arm: ExperimentArm,
    pub second_arm: ExperimentArm,
    pub assignment_randomized: bool,
    pub task_content_recorded: bool,
}

#[derive(Debug, Clone)]
pub struct ExperimentObservationInput {
    pub pair_id: String,
    pub arm: ExperimentArm,
    pub tokens: u64,
    pub elapsed_ms: u64,
    pub tool_calls: u64,
    pub completed: bool,
    pub completeness: Option<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExperimentObservationReport {
    pub experiment_id: String,
    pub pair_id: String,
    pub arm: ExperimentArm,
    pub attempt: u8,
    pub pair_complete: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct PairedMetricSummary {
    pub median_delta_memory_minus_baseline: Option<f64>,
    pub median_percent_delta: Option<f64>,
    pub favors_memory: usize,
    pub favors_baseline: usize,
    pub ties: usize,
    pub sign_test_two_sided_p: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExperimentPairDelta {
    pub pair_id: String,
    pub memory_first: bool,
    pub tokens_delta: i64,
    pub elapsed_ms_delta: i64,
    pub tool_calls_delta: i64,
    pub memory_completed: bool,
    pub baseline_completed: bool,
    pub memory_completeness: Option<u8>,
    pub baseline_completeness: Option<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExperimentReport {
    pub schema_version: i64,
    pub experiment_id: String,
    pub primary_metric: ExperimentPrimaryMetric,
    pub complete_pairs: usize,
    pub unpaired_excluded: usize,
    pub memory_first_pairs: usize,
    pub baseline_first_pairs: usize,
    pub tokens: PairedMetricSummary,
    pub elapsed_ms: PairedMetricSummary,
    pub tool_calls: PairedMetricSummary,
    pub memory_completed: usize,
    pub baseline_completed: usize,
    pub memory_completeness_median: Option<f64>,
    pub baseline_completeness_median: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pairs: Option<Vec<ExperimentPairDelta>>,
    pub caveats: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricsExportReport {
    pub schema_version: i64,
    pub output: String,
    pub rows: usize,
    pub query_recorded: bool,
    pub content_recorded: bool,
    pub citation_recorded: bool,
    pub path_recorded: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "record_type", rename_all = "snake_case")]
enum ExportRow {
    Event(Box<StoredEvent>),
    Experiment(StoredExperiment),
    Pair(StoredPair),
    Observation(StoredObservation),
}

#[derive(Debug, Clone, Serialize)]
struct StoredEvent {
    ts_minute: i64,
    operation: String,
    outcome: String,
    error_category: String,
    duration_ms: f64,
    recall_ms: Option<f64>,
    probe_ms: Option<f64>,
    citation_fetch_ms: Option<f64>,
    model_load_ms: Option<f64>,
    inference_ms: Option<f64>,
    response_bytes: Option<u64>,
    qualified_claims: Option<u64>,
    qualified_artifacts: Option<u64>,
    recovery_references: Option<u64>,
    result_count: Option<u64>,
    indexed_files: Option<u64>,
    indexed_bytes: Option<u64>,
    changed_files: Option<u64>,
    changed_bytes: Option<u64>,
    project_edges: Option<u64>,
    project_edge_truncations: Option<u64>,
    project_mentions: Option<u64>,
    project_mentions_truncated: Option<bool>,
    stale: Option<bool>,
    reindex_attempted: Option<bool>,
    semantic_state: String,
    reranker_state: String,
    breaker_open: bool,
}

#[derive(Debug, Clone, Serialize)]
struct StoredExperiment {
    experiment_id: String,
    created_minute: i64,
    primary_metric: String,
}

#[derive(Debug, Clone, Serialize)]
struct StoredPair {
    pair_id: String,
    experiment_id: String,
    created_minute: i64,
    memory_first: bool,
}

#[derive(Debug, Clone, Serialize)]
struct StoredObservation {
    pair_id: String,
    ts_minute: i64,
    arm: String,
    attempt: u8,
    tokens: u64,
    elapsed_ms: u64,
    tool_calls: u64,
    completed: bool,
    completeness: Option<u8>,
}

impl MetricsStore {
    pub fn discover(cwd: &Path, data_dir: &Path) -> Result<Self> {
        let marker_path = find_marker(cwd)?.ok_or_else(|| {
            MemoryError::Config(format!(
                "no {MARKER_FILE} was found from {}; initialize project memory first",
                cwd.display()
            ))
        })?;
        let mut marker = read_marker(&marker_path)?;
        marker.metrics = load_local_project_settings(data_dir, &marker.project_id)?
            .map(|settings| settings.metrics)
            .unwrap_or_default();
        let root = marker_path
            .parent()
            .ok_or_else(|| MemoryError::Config("project marker has no parent directory".into()))?
            .to_path_buf();
        let project_key = blake3::hash(marker.project_id.as_bytes())
            .to_hex()
            .to_string();
        let directory = data_dir.join(METRICS_DIRECTORY).join(&project_key[..32]);
        let database_path = directory.join(METRICS_DATABASE);
        Ok(Self {
            root,
            marker,
            data_dir: data_dir.to_path_buf(),
            directory,
            database_path,
        })
    }

    pub fn discover_enabled(cwd: &Path, data_dir: &Path) -> Result<Option<Self>> {
        let store = Self::discover(cwd, data_dir)?;
        Ok(store.marker.metrics.enabled.then_some(store))
    }

    pub fn config(&self) -> &MetricsConfig {
        &self.marker.metrics
    }

    pub fn configure(
        &mut self,
        enabled: Option<bool>,
        retention_days: Option<u32>,
        max_database_bytes: Option<u64>,
        sample_rate: Option<f64>,
    ) -> Result<MetricsStatus> {
        let initial = LocalProjectSettings::from_marker(&self.marker);
        let settings = update_local_project_settings(
            &self.data_dir,
            &self.marker.project_id,
            &initial,
            |settings| {
                if let Some(enabled) = enabled {
                    settings.metrics.enabled = enabled;
                }
                if let Some(retention_days) = retention_days {
                    settings.metrics.retention_days = retention_days;
                }
                if let Some(max_database_bytes) = max_database_bytes {
                    settings.metrics.max_database_bytes = max_database_bytes;
                }
                if let Some(sample_rate) = sample_rate {
                    settings.metrics.sample_rate = sample_rate;
                }
                validate_metrics_config(&settings.metrics)
            },
        )?;
        self.marker.metrics = settings.metrics;
        if self.marker.metrics.enabled {
            let connection = self.open_database()?;
            connection.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES ('consent_at_minute', ?1)",
                [now_minute().to_string()],
            )?;
        }
        self.status()
    }

    pub fn status(&self) -> Result<MetricsStatus> {
        let database_exists = self.database_path.exists();
        let (events, experiments, oldest, newest) = if database_exists {
            let connection = self.open_database()?;
            (
                count(&connection, "events")?,
                count(&connection, "experiments")?,
                connection.query_row("SELECT MIN(ts_minute) FROM events", [], |row| row.get(0))?,
                connection.query_row("SELECT MAX(ts_minute) FROM events", [], |row| row.get(0))?,
            )
        } else {
            (0, 0, None, None)
        };
        Ok(MetricsStatus {
            schema_version: METRICS_SCHEMA_VERSION,
            project_id: self.marker.project_id.clone(),
            enabled: self.marker.metrics.enabled,
            retention_days: self.marker.metrics.retention_days,
            max_database_bytes: self.marker.metrics.max_database_bytes,
            sample_rate: self.marker.metrics.sample_rate,
            database_exists,
            database_bytes: file_bytes(&self.database_path),
            recorded_events: events,
            experiments,
            oldest_event_minute: oldest,
            newest_event_minute: newest,
            query_recorded: false,
            content_recorded: false,
            citation_recorded: false,
            path_recorded: false,
            network_telemetry: false,
        })
    }

    pub fn record_event(&self, event: &MetricEvent) -> Result<bool> {
        if !self.marker.metrics.enabled || !self.should_sample() {
            return Ok(false);
        }
        event.validate()?;
        let connection = self.open_database()?;
        connection.execute(
            "INSERT INTO events(
                ts_minute, operation, outcome, error_category, duration_ms,
                recall_ms, probe_ms, citation_fetch_ms, model_load_ms, inference_ms,
                response_bytes, qualified_claims, qualified_artifacts, recovery_references,
                result_count, indexed_files, indexed_bytes, changed_files, changed_bytes,
                project_edges, project_edge_truncations, project_mentions,
                project_mentions_truncated, stale, reindex_attempted,
                semantic_state, reranker_state, breaker_open
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24,
                ?25, ?26, ?27, ?28
             )",
            params![
                now_minute(),
                event.operation,
                event.outcome,
                event.error_category,
                event.duration_ms,
                event.recall_ms,
                event.probe_ms,
                event.citation_fetch_ms,
                event.model_load_ms,
                event.inference_ms,
                event.response_bytes.map(to_i64).transpose()?,
                event.qualified_claims.map(to_i64).transpose()?,
                event.qualified_artifacts.map(to_i64).transpose()?,
                event.recovery_references.map(to_i64).transpose()?,
                event.result_count.map(to_i64).transpose()?,
                event.indexed_files.map(to_i64).transpose()?,
                event.indexed_bytes.map(to_i64).transpose()?,
                event.changed_files.map(to_i64).transpose()?,
                event.changed_bytes.map(to_i64).transpose()?,
                event.project_edges.map(to_i64).transpose()?,
                event.project_edge_truncations.map(to_i64).transpose()?,
                event.project_mentions.map(to_i64).transpose()?,
                event.project_mentions_truncated.map(i64::from),
                event.stale.map(i64::from),
                event.reindex_attempted.map(i64::from),
                event.semantic_state,
                event.reranker_state,
                i64::from(event.breaker_open),
            ],
        )?;
        self.enforce_limits(&connection)?;
        Ok(true)
    }

    pub fn report(&self, days: u32) -> Result<MetricsReport> {
        validate_days(days)?;
        let since = now_minute() - i64::from(days) * 24 * 60;
        let connection = self.open_database()?;
        let events = load_events(&connection, since)?;
        let mut grouped = BTreeMap::<String, Vec<StoredEvent>>::new();
        for event in events {
            grouped
                .entry(event.operation.clone())
                .or_default()
                .push(event);
        }
        let operations = grouped
            .into_iter()
            .map(|(operation, events)| summarize_operation(operation, &events))
            .collect();
        Ok(MetricsReport {
            schema_version: METRICS_SCHEMA_VERSION,
            project_id: self.marker.project_id.clone(),
            since_minutes: since,
            generated_at_minute: now_minute(),
            database_bytes: file_bytes(&self.database_path),
            max_database_bytes: self.marker.metrics.max_database_bytes,
            operations,
            caveats: vec![
                "Operational traces measure latency, payload, and retrieval behavior; they do not observe the counterfactual task without Memoree and cannot prove token savings.".into(),
                "Sampling, cold starts, cache state, project state, and optional feedback can confound comparisons.".into(),
            ],
        })
    }

    pub fn doctor(&self) -> Result<MetricsDoctorReport> {
        if !self.database_path.exists() {
            return Ok(MetricsDoctorReport {
                schema_version: METRICS_SCHEMA_VERSION,
                project_id: self.marker.project_id.clone(),
                enabled: self.marker.metrics.enabled,
                database_exists: false,
                integrity_ok: true,
                pragmas_ok: true,
                closed_schema_ok: true,
                categorical_allowlists_ok: true,
                private_permissions_ok: true,
                outside_project_tree: !self.database_path.starts_with(&self.root),
                within_size_cap: true,
                within_retention: true,
                status: "ready_without_database".into(),
            });
        }
        let connection = self.open_database()?;
        let integrity: String =
            connection.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        let pragmas_ok = metrics_pragmas_ok(&connection)?;
        let closed_schema_ok = closed_schema_ok(&connection)?;
        let categorical_allowlists_ok = categorical_allowlists_ok(&connection)?;
        let oldest: Option<i64> =
            connection.query_row("SELECT MIN(ts_minute) FROM events", [], |row| row.get(0))?;
        let cutoff = now_minute() - i64::from(self.marker.metrics.retention_days) * 24 * 60;
        let within_retention = oldest.is_none_or(|oldest| oldest >= cutoff);
        let metrics_root = self.data_dir.join(METRICS_DIRECTORY);
        let local_settings = local_project_settings_path(&self.data_dir, &self.marker.project_id);
        let private_permissions_ok = [
            self.data_dir.as_path(),
            metrics_root.as_path(),
            self.directory.as_path(),
            self.database_path.as_path(),
            local_settings.parent().unwrap_or(self.data_dir.as_path()),
            local_settings.as_path(),
        ]
        .into_iter()
        .all(|path| private_permissions(path).unwrap_or(false));
        let outside_project_tree = !self.database_path.starts_with(&self.root);
        let within_size_cap =
            file_bytes(&self.database_path) <= self.marker.metrics.max_database_bytes;
        let ok = integrity == "ok"
            && pragmas_ok
            && closed_schema_ok
            && categorical_allowlists_ok
            && private_permissions_ok
            && outside_project_tree
            && within_size_cap
            && within_retention;
        Ok(MetricsDoctorReport {
            schema_version: METRICS_SCHEMA_VERSION,
            project_id: self.marker.project_id.clone(),
            enabled: self.marker.metrics.enabled,
            database_exists: true,
            integrity_ok: integrity == "ok",
            pragmas_ok,
            closed_schema_ok,
            categorical_allowlists_ok,
            private_permissions_ok,
            outside_project_tree,
            within_size_cap,
            within_retention,
            status: if ok { "ok" } else { "degraded" }.into(),
        })
    }

    pub fn export_jsonl(&self, output: &Path, days: u32) -> Result<MetricsExportReport> {
        validate_days(days)?;
        let output_parent = output.parent().ok_or_else(|| {
            MemoryError::InvalidRequest("metrics export path has no parent directory".into())
        })?;
        let canonical_parent = fs::canonicalize(output_parent)?;
        let canonical_root = fs::canonicalize(&self.root)?;
        if canonical_parent.starts_with(canonical_root) {
            return Err(MemoryError::InvalidRequest(
                "metrics export must be written outside the project tree to avoid accidental version control"
                    .into(),
            ));
        }
        let since = now_minute() - i64::from(days) * 24 * 60;
        let connection = self.open_database()?;
        if !categorical_allowlists_ok(&connection)? {
            return Err(MemoryError::Integrity(
                "metrics export refused because categorical privacy allowlists failed".into(),
            ));
        }
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(output)?;
        #[cfg(unix)]
        fs::set_permissions(output, std::os::unix::fs::PermissionsExt::from_mode(0o600))?;
        let mut writer = BufWriter::new(file);
        let mut rows = 0usize;
        for event in load_events(&connection, since)? {
            write_jsonl(&mut writer, &ExportRow::Event(Box::new(event)))?;
            rows += 1;
        }
        for experiment in load_experiments(&connection, since)? {
            write_jsonl(&mut writer, &ExportRow::Experiment(experiment))?;
            rows += 1;
        }
        for pair in load_pairs_since(&connection, since)? {
            write_jsonl(&mut writer, &ExportRow::Pair(pair))?;
            rows += 1;
        }
        for observation in load_observations_since(&connection, since)? {
            write_jsonl(&mut writer, &ExportRow::Observation(observation))?;
            rows += 1;
        }
        writer.flush()?;
        writer.get_ref().sync_all()?;
        Ok(MetricsExportReport {
            schema_version: METRICS_SCHEMA_VERSION,
            output: output.display().to_string(),
            rows,
            query_recorded: false,
            content_recorded: false,
            citation_recorded: false,
            path_recorded: false,
        })
    }

    pub fn clear(&self, confirmed: bool) -> Result<bool> {
        if !confirmed {
            return Err(MemoryError::InvalidRequest(
                "metrics clear is destructive; repeat with --yes".into(),
            ));
        }
        let existed = self.database_path.exists();
        for path in [
            self.database_path.clone(),
            self.database_path.with_extension("sqlite3-journal"),
            self.database_path.with_extension("sqlite3-wal"),
            self.database_path.with_extension("sqlite3-shm"),
        ] {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(existed)
    }

    pub fn begin_experiment(
        &self,
        primary_metric: ExperimentPrimaryMetric,
    ) -> Result<ExperimentBeginReport> {
        require_enabled(&self.marker.metrics)?;
        let experiment_id = opaque_id("exp_");
        let connection = self.open_database()?;
        connection.execute(
            "INSERT INTO experiments(experiment_id, created_minute, primary_metric)
             VALUES (?1, ?2, ?3)",
            params![experiment_id, now_minute(), primary_metric.as_str()],
        )?;
        self.enforce_limits(&connection)?;
        Ok(ExperimentBeginReport {
            experiment_id,
            primary_metric,
            task_content_recorded: false,
            next: "Generate an opaque pair with `memoree experiment pair --experiment EXPERIMENT_ID`; keep the task mapping outside Memoree.".into(),
        })
    }

    pub fn create_pair(&self, experiment_id: &str) -> Result<ExperimentPairReport> {
        require_enabled(&self.marker.metrics)?;
        validate_opaque_id(experiment_id, "exp_")?;
        let connection = self.open_database()?;
        require_experiment(&connection, experiment_id)?;
        let pair_id = opaque_id("pair_");
        let digest = blake3::hash(pair_id.as_bytes());
        let memory_first = digest.as_bytes()[0] & 1 == 0;
        connection.execute(
            "INSERT INTO experiment_pairs(
                pair_id, experiment_id, created_minute, memory_first
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                pair_id,
                experiment_id,
                now_minute(),
                i64::from(memory_first)
            ],
        )?;
        self.enforce_limits(&connection)?;
        Ok(ExperimentPairReport {
            experiment_id: experiment_id.into(),
            pair_id,
            first_arm: if memory_first {
                ExperimentArm::Memory
            } else {
                ExperimentArm::Baseline
            },
            second_arm: if memory_first {
                ExperimentArm::Baseline
            } else {
                ExperimentArm::Memory
            },
            assignment_randomized: true,
            task_content_recorded: false,
        })
    }

    pub fn record_observation(
        &self,
        input: &ExperimentObservationInput,
    ) -> Result<ExperimentObservationReport> {
        require_enabled(&self.marker.metrics)?;
        validate_opaque_id(&input.pair_id, "pair_")?;
        if input
            .completeness
            .is_some_and(|value| !(1..=5).contains(&value))
        {
            return Err(MemoryError::InvalidRequest(
                "completeness must be an integer from 1 through 5".into(),
            ));
        }
        let connection = self.open_database()?;
        let (experiment_id, memory_first): (String, bool) = connection
            .query_row(
                "SELECT experiment_id, memory_first FROM experiment_pairs WHERE pair_id = ?1",
                [&input.pair_id],
                |row| Ok((row.get(0)?, row.get::<_, i64>(1)? != 0)),
            )
            .optional()?
            .ok_or_else(|| MemoryError::NotFound("experiment pair was not found".into()))?;
        let first_arm = if memory_first {
            ExperimentArm::Memory
        } else {
            ExperimentArm::Baseline
        };
        let attempt = if input.arm == first_arm { 1 } else { 2 };
        if attempt == 2 {
            let first_exists: bool = connection.query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM experiment_observations
                     WHERE pair_id = ?1 AND arm = ?2
                 )",
                params![input.pair_id, first_arm.as_str()],
                |row| row.get(0),
            )?;
            if !first_exists {
                return Err(MemoryError::InvalidRequest(format!(
                    "record the randomized first arm `{}` before the second arm",
                    first_arm.as_str()
                )));
            }
        }
        let inserted = connection.execute(
            "INSERT OR IGNORE INTO experiment_observations(
                pair_id, ts_minute, arm, attempt, tokens, elapsed_ms,
                tool_calls, completed, completeness
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                input.pair_id,
                now_minute(),
                input.arm.as_str(),
                attempt,
                to_i64(input.tokens)?,
                to_i64(input.elapsed_ms)?,
                to_i64(input.tool_calls)?,
                i64::from(input.completed),
                input.completeness.map(i64::from),
            ],
        )?;
        if inserted == 0 {
            return Err(MemoryError::InvalidRequest(
                "this experiment arm was already recorded; observations are immutable".into(),
            ));
        }
        self.enforce_limits(&connection)?;
        let pair_complete: bool = connection.query_row(
            "SELECT COUNT(*) = 2 FROM experiment_observations WHERE pair_id = ?1",
            [&input.pair_id],
            |row| row.get(0),
        )?;
        Ok(ExperimentObservationReport {
            experiment_id,
            pair_id: input.pair_id.clone(),
            arm: input.arm,
            attempt,
            pair_complete,
        })
    }

    pub fn experiment_report(
        &self,
        experiment_id: &str,
        include_pairs: bool,
    ) -> Result<ExperimentReport> {
        validate_opaque_id(experiment_id, "exp_")?;
        let connection = self.open_database()?;
        let primary = require_experiment(&connection, experiment_id)?;
        let pairs = load_experiment_deltas(&connection, experiment_id)?;
        let all_pairs = connection.query_row(
            "SELECT COUNT(*) FROM experiment_pairs WHERE experiment_id = ?1",
            [experiment_id],
            |row| row.get::<_, i64>(0),
        )? as usize;
        let memory_first_pairs = pairs.iter().filter(|pair| pair.memory_first).count();
        let baseline_first_pairs = pairs.len() - memory_first_pairs;
        let tokens = paired_summary(
            &pairs
                .iter()
                .map(|pair| (pair.tokens_delta, pair.tokens_delta))
                .collect::<Vec<_>>(),
        );
        let elapsed_ms = paired_summary(
            &pairs
                .iter()
                .map(|pair| (pair.elapsed_ms_delta, pair.elapsed_ms_delta))
                .collect::<Vec<_>>(),
        );
        let tool_calls = paired_summary(
            &pairs
                .iter()
                .map(|pair| (pair.tool_calls_delta, pair.tool_calls_delta))
                .collect::<Vec<_>>(),
        );
        // Percent deltas need the baseline denominator, so replace the simple
        // placeholder percentages using the original observations.
        let token_percent = load_percent_deltas(&connection, experiment_id, "tokens")?;
        let elapsed_percent = load_percent_deltas(&connection, experiment_id, "elapsed_ms")?;
        let tool_percent = load_percent_deltas(&connection, experiment_id, "tool_calls")?;
        let tokens = with_percent(tokens, token_percent);
        let elapsed_ms = with_percent(elapsed_ms, elapsed_percent);
        let tool_calls = with_percent(tool_calls, tool_percent);
        let memory_completeness = pairs
            .iter()
            .filter_map(|pair| pair.memory_completeness.map(f64::from))
            .collect::<Vec<_>>();
        let baseline_completeness = pairs
            .iter()
            .filter_map(|pair| pair.baseline_completeness.map(f64::from))
            .collect::<Vec<_>>();
        Ok(ExperimentReport {
            schema_version: METRICS_SCHEMA_VERSION,
            experiment_id: experiment_id.into(),
            primary_metric: primary,
            complete_pairs: pairs.len(),
            unpaired_excluded: all_pairs.saturating_sub(pairs.len()),
            memory_first_pairs,
            baseline_first_pairs,
            tokens,
            elapsed_ms,
            tool_calls,
            memory_completed: pairs.iter().filter(|pair| pair.memory_completed).count(),
            baseline_completed: pairs.iter().filter(|pair| pair.baseline_completed).count(),
            memory_completeness_median: median(memory_completeness),
            baseline_completeness_median: median(baseline_completeness),
            pairs: include_pairs.then_some(pairs),
            caveats: vec![
                "Only complete randomized pairs are summarized; unpaired observations are excluded.".into(),
                "Small samples and human repetition can create order, learning, and optional-stopping effects; at fewer than 30 pairs inspect raw pair deltas.".into(),
                "With fewer than six non-tied complete pairs, a two-sided sign-test p-value below 0.05 is mathematically unattainable.".into(),
                "The declared primary metric is confirmatory; all other comparisons are exploratory.".into(),
                "Provider-reported token accounting must use the same definition in both arms.".into(),
            ],
        })
    }

    fn should_sample(&self) -> bool {
        let rate = self.marker.metrics.sample_rate;
        if rate >= 1.0 {
            return true;
        }
        if rate <= 0.0 {
            return false;
        }
        let digest = blake3::hash(Ulid::r#gen().to_string().as_bytes());
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&digest.as_bytes()[..8]);
        let fraction = u64::from_le_bytes(bytes) as f64 / u64::MAX as f64;
        fraction < rate
    }

    fn open_database(&self) -> Result<Connection> {
        create_private_directory(&self.data_dir)?;
        create_private_directory(&self.data_dir.join(METRICS_DIRECTORY))?;
        create_private_directory(&self.directory)?;
        let is_new = !self.database_path.exists();
        let connection = Connection::open(&self.database_path)?;
        connection.busy_timeout(Duration::ZERO)?;
        #[cfg(unix)]
        fs::set_permissions(
            &self.database_path,
            std::os::unix::fs::PermissionsExt::from_mode(0o600),
        )?;
        connection.execute_batch(
            "PRAGMA journal_mode = DELETE;
             PRAGMA synchronous = NORMAL;
             PRAGMA secure_delete = ON;
             PRAGMA foreign_keys = ON;",
        )?;
        if is_new {
            connection.execute_batch("PRAGMA auto_vacuum = FULL; VACUUM;")?;
        }
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS meta(
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS events(
                id INTEGER PRIMARY KEY,
                ts_minute INTEGER NOT NULL,
                operation TEXT NOT NULL,
                outcome TEXT NOT NULL,
                error_category TEXT NOT NULL,
                duration_ms REAL NOT NULL,
                recall_ms REAL,
                probe_ms REAL,
                citation_fetch_ms REAL,
                model_load_ms REAL,
                inference_ms REAL,
                response_bytes INTEGER,
                qualified_claims INTEGER,
                qualified_artifacts INTEGER,
                recovery_references INTEGER,
                result_count INTEGER,
                indexed_files INTEGER,
                indexed_bytes INTEGER,
                changed_files INTEGER,
                changed_bytes INTEGER,
                project_edges INTEGER,
                project_edge_truncations INTEGER,
                project_mentions INTEGER,
                project_mentions_truncated INTEGER,
                stale INTEGER,
                reindex_attempted INTEGER,
                semantic_state TEXT NOT NULL,
                reranker_state TEXT NOT NULL,
                breaker_open INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS events_ts ON events(ts_minute);
             CREATE TABLE IF NOT EXISTS experiments(
                experiment_id TEXT PRIMARY KEY,
                created_minute INTEGER NOT NULL,
                primary_metric TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS experiment_pairs(
                pair_id TEXT PRIMARY KEY,
                experiment_id TEXT NOT NULL REFERENCES experiments(experiment_id),
                created_minute INTEGER NOT NULL,
                memory_first INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS experiment_pairs_experiment
                ON experiment_pairs(experiment_id);
             CREATE TABLE IF NOT EXISTS experiment_observations(
                pair_id TEXT NOT NULL REFERENCES experiment_pairs(pair_id),
                ts_minute INTEGER NOT NULL,
                arm TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                tokens INTEGER NOT NULL,
                elapsed_ms INTEGER NOT NULL,
                tool_calls INTEGER NOT NULL,
                completed INTEGER NOT NULL,
                completeness INTEGER,
                PRIMARY KEY(pair_id, arm)
             );
             CREATE INDEX IF NOT EXISTS observations_ts
                ON experiment_observations(ts_minute);",
        )?;
        let existing: Option<i64> = connection
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        if existing == Some(1) {
            connection.execute_batch(
                "ALTER TABLE events ADD COLUMN project_edges INTEGER;
                 ALTER TABLE events ADD COLUMN project_edge_truncations INTEGER;
                 ALTER TABLE events ADD COLUMN project_mentions INTEGER;
                 ALTER TABLE events ADD COLUMN project_mentions_truncated INTEGER;
                 UPDATE meta SET value = '2' WHERE key = 'schema_version';",
            )?;
        }
        let existing = if existing == Some(1) {
            Some(METRICS_SCHEMA_VERSION)
        } else {
            existing
        };
        if existing.is_some_and(|version| version != METRICS_SCHEMA_VERSION) {
            return Err(MemoryError::Integrity(format!(
                "unsupported metrics schema {}; clear the disposable metrics store",
                existing.unwrap_or_default()
            )));
        }
        connection.execute(
            "INSERT OR IGNORE INTO meta(key, value) VALUES ('schema_version', ?1)",
            [METRICS_SCHEMA_VERSION.to_string()],
        )?;
        self.enforce_limits(&connection)?;
        Ok(connection)
    }

    fn enforce_limits(&self, connection: &Connection) -> Result<()> {
        let cutoff = now_minute() - i64::from(self.marker.metrics.retention_days) * 24 * 60;
        connection.execute("DELETE FROM events WHERE ts_minute < ?1", [cutoff])?;
        let cap = self.marker.metrics.max_database_bytes;
        let mut rounds = 0usize;
        while database_allocated_bytes(connection)? > cap && rounds < 64 {
            let removed_events = connection.execute(
                "DELETE FROM events WHERE id IN(
                    SELECT id FROM events ORDER BY ts_minute, id LIMIT 1000
                )",
                [],
            )?;
            if removed_events == 0 {
                break;
            }
            rounds += 1;
        }
        if database_allocated_bytes(connection)? > cap {
            return Err(MemoryError::Integrity(
                "metrics database cannot satisfy its configured size cap".into(),
            ));
        }
        Ok(())
    }
}

pub fn safe_error_category(value: &str) -> &'static str {
    match value {
        "invalid_request" => "invalid_request",
        "not_found" => "not_found",
        "citation_error" => "citation_error",
        "revision_conflict" | "idempotency_conflict" => "conflict",
        "index_not_ready" => "index_not_ready",
        "scope_violation" => "scope_violation",
        "content_too_large" => "content_too_large",
        "integrity_error" => "integrity_error",
        "unsupported_version" => "unsupported_version",
        "config_error" | "no_ambient_context" => "config_error",
        "transport_error" => "transport_error",
        "reasoner_error" => "reasoner_error",
        "timeout" => "timeout",
        _ => "internal_error",
    }
}

pub fn safe_runtime_state(value: &str) -> &'static str {
    match value {
        "ready" => "ready",
        "disabled" | "surface_disabled" => "disabled",
        "unavailable" | "not_installed" => "unavailable",
        "stale" => "stale",
        "degraded" => "degraded",
        "open" | "breaker_open" => "breaker_open",
        _ => "unknown",
    }
}

fn validate_metrics_config(config: &MetricsConfig) -> Result<()> {
    if config.retention_days == 0
        || config.retention_days > 365
        || config.max_database_bytes < 1024 * 1024
        || config.max_database_bytes > 1024 * 1024 * 1024
        || !config.sample_rate.is_finite()
        || !(0.0..=1.0).contains(&config.sample_rate)
    {
        return Err(MemoryError::InvalidRequest(
            "metrics require retention_days 1..=365, max_database_bytes 1 MiB..=1 GiB, and sample_rate 0.0..=1.0".into(),
        ));
    }
    Ok(())
}

fn require_enabled(config: &MetricsConfig) -> Result<()> {
    if config.enabled {
        Ok(())
    } else {
        Err(MemoryError::InvalidRequest(
            "project metrics are disabled; run `memoree metrics configure --enabled true`".into(),
        ))
    }
}

fn validate_days(days: u32) -> Result<()> {
    if days == 0 || days > MAX_REPORT_DAYS {
        Err(MemoryError::InvalidRequest(format!(
            "report days must be between 1 and {MAX_REPORT_DAYS}"
        )))
    } else {
        Ok(())
    }
}

fn validate_opaque_id(value: &str, prefix: &str) -> Result<()> {
    let suffix = value.strip_prefix(prefix).ok_or_else(|| {
        MemoryError::InvalidRequest(format!("identifier must begin with {prefix}"))
    })?;
    if suffix.len() != 32 || !suffix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(MemoryError::InvalidRequest(
            "identifier is not a generated opaque Memoree identifier".into(),
        ));
    }
    Ok(())
}

fn opaque_id(prefix: &str) -> String {
    // Hash the timestamp-bearing random seed so the retained opaque ID does
    // not disclose sub-minute creation time.
    let digest = blake3::hash(Ulid::r#gen().to_string().as_bytes());
    format!("{prefix}{}", &digest.to_hex()[..32])
}

fn create_private_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o700))?;
    Ok(())
}

fn private_permissions(path: &Path) -> Result<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        Ok(fs::metadata(path)?.permissions().mode() & 0o077 == 0)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(true)
    }
}

fn now_minute() -> i64 {
    Utc::now().timestamp().div_euclid(MINUTE_SECONDS)
}

fn to_i64(value: u64) -> Result<i64> {
    i64::try_from(value)
        .map_err(|_| MemoryError::InvalidRequest("numeric metric exceeds SQLite range".into()))
}

fn file_bytes(path: &Path) -> u64 {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn database_allocated_bytes(connection: &Connection) -> Result<u64> {
    let pages: i64 = connection.query_row("PRAGMA page_count", [], |row| row.get(0))?;
    let page_size: i64 = connection.query_row("PRAGMA page_size", [], |row| row.get(0))?;
    Ok((pages.max(0) as u64).saturating_mul(page_size.max(0) as u64))
}

fn metrics_pragmas_ok(connection: &Connection) -> Result<bool> {
    let journal_mode: String = connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    let secure_delete: i64 = connection.query_row("PRAGMA secure_delete", [], |row| row.get(0))?;
    let auto_vacuum: i64 = connection.query_row("PRAGMA auto_vacuum", [], |row| row.get(0))?;
    let busy_timeout: i64 = connection.query_row("PRAGMA busy_timeout", [], |row| row.get(0))?;
    let foreign_keys: i64 = connection.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
    Ok(journal_mode.eq_ignore_ascii_case("delete")
        && secure_delete == 1
        && auto_vacuum == 1
        && busy_timeout == 0
        && foreign_keys == 1)
}

fn count(connection: &Connection, table: &str) -> Result<u64> {
    let sql = match table {
        "events" => "SELECT COUNT(*) FROM events",
        "experiments" => "SELECT COUNT(*) FROM experiments",
        _ => return Err(MemoryError::Integrity("invalid metrics count table".into())),
    };
    Ok(connection.query_row(sql, [], |row| row.get::<_, i64>(0))? as u64)
}

fn distribution(values: impl IntoIterator<Item = f64>) -> MetricDistribution {
    let mut values = values
        .into_iter()
        .filter(|value| value.is_finite())
        .collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    if values.is_empty() {
        return MetricDistribution {
            min: 0.0,
            p50: 0.0,
            p95: 0.0,
            max: 0.0,
            mean: 0.0,
        };
    }
    let percentile = |fraction: f64| {
        let rank = (fraction * values.len() as f64).ceil() as usize;
        values[rank.saturating_sub(1).min(values.len() - 1)]
    };
    MetricDistribution {
        min: values[0],
        p50: percentile(0.50),
        p95: percentile(0.95),
        max: *values.last().unwrap_or(&0.0),
        mean: values.iter().sum::<f64>() / values.len() as f64,
    }
}

fn summarize_operation(operation: String, events: &[StoredEvent]) -> OperationMetrics {
    let mut outcomes = BTreeMap::new();
    for event in events {
        *outcomes.entry(event.outcome.clone()).or_insert(0) += 1;
    }
    let count = events.len();
    let outcome_count = |name: &str| outcomes.get(name).copied().unwrap_or(0);
    let feedback_total = [
        "feedback_useful",
        "feedback_miss",
        "feedback_incorrect",
        "feedback_stale",
    ]
    .into_iter()
    .map(outcome_count)
    .sum::<usize>();
    let rate = |value: usize| {
        if count == 0 {
            0.0
        } else {
            value as f64 / count as f64
        }
    };
    let error_rate = rate(outcome_count("error"));
    let qualified_rate = rate(outcome_count("qualified"));
    let recovery_rate = rate(outcome_count("recovered"));
    let abstention_rate = rate(outcome_count("abstained") + outcome_count("empty"));
    let feedback_useful_rate = (feedback_total > 0)
        .then(|| outcome_count("feedback_useful") as f64 / feedback_total as f64);
    OperationMetrics {
        operation,
        count,
        duration_ms: distribution(events.iter().map(|event| event.duration_ms)),
        outcomes,
        error_count: events
            .iter()
            .filter(|event| event.outcome == "error")
            .count(),
        error_rate,
        qualified_rate,
        recovery_rate,
        abstention_rate,
        feedback_useful_rate,
        response_bytes: distribution(
            events
                .iter()
                .filter_map(|event| event.response_bytes.map(|value| value as f64)),
        ),
        recall_ms: distribution(events.iter().filter_map(|event| event.recall_ms)),
        probe_ms: distribution(events.iter().filter_map(|event| event.probe_ms)),
        citation_fetch_ms: distribution(events.iter().filter_map(|event| event.citation_fetch_ms)),
        model_load_ms: distribution(events.iter().filter_map(|event| event.model_load_ms)),
        inference_ms: distribution(events.iter().filter_map(|event| event.inference_ms)),
        qualified_claims: count_distribution(events, |event| event.qualified_claims),
        qualified_artifacts: count_distribution(events, |event| event.qualified_artifacts),
        recovery_references: count_distribution(events, |event| event.recovery_references),
        result_count: count_distribution(events, |event| event.result_count),
        indexed_files: count_distribution(events, |event| event.indexed_files),
        indexed_bytes: count_distribution(events, |event| event.indexed_bytes),
        changed_files: count_distribution(events, |event| event.changed_files),
        changed_bytes: count_distribution(events, |event| event.changed_bytes),
        project_edges: count_distribution(events, |event| event.project_edges),
        project_edge_truncations: count_distribution(events, |event| {
            event.project_edge_truncations
        }),
        project_mentions: count_distribution(events, |event| event.project_mentions),
        project_mentions_truncated_count: events
            .iter()
            .filter(|event| event.project_mentions_truncated == Some(true))
            .count(),
        breaker_open_count: events.iter().filter(|event| event.breaker_open).count(),
        stale_count: events
            .iter()
            .filter(|event| event.stale == Some(true))
            .count(),
        reindex_attempted_count: events
            .iter()
            .filter(|event| event.reindex_attempted == Some(true))
            .count(),
    }
}

fn count_distribution(
    events: &[StoredEvent],
    select: impl Fn(&StoredEvent) -> Option<u64>,
) -> MetricDistribution {
    distribution(events.iter().filter_map(select).map(|value| value as f64))
}

fn load_events(connection: &Connection, since: i64) -> Result<Vec<StoredEvent>> {
    let mut statement = connection.prepare(
        "SELECT ts_minute, operation, outcome, error_category, duration_ms,
                recall_ms, probe_ms, citation_fetch_ms, model_load_ms, inference_ms,
                response_bytes, qualified_claims, qualified_artifacts, recovery_references,
                result_count, indexed_files, indexed_bytes, changed_files, changed_bytes,
                project_edges, project_edge_truncations, project_mentions,
                project_mentions_truncated, stale, reindex_attempted,
                semantic_state, reranker_state, breaker_open
           FROM events WHERE ts_minute >= ?1 ORDER BY ts_minute, id",
    )?;
    let rows = statement.query_map([since], |row| {
        Ok(StoredEvent {
            ts_minute: row.get(0)?,
            operation: row.get(1)?,
            outcome: row.get(2)?,
            error_category: row.get(3)?,
            duration_ms: row.get(4)?,
            recall_ms: row.get(5)?,
            probe_ms: row.get(6)?,
            citation_fetch_ms: row.get(7)?,
            model_load_ms: row.get(8)?,
            inference_ms: row.get(9)?,
            response_bytes: row.get::<_, Option<i64>>(10)?.map(|value| value as u64),
            qualified_claims: row.get::<_, Option<i64>>(11)?.map(|value| value as u64),
            qualified_artifacts: row.get::<_, Option<i64>>(12)?.map(|value| value as u64),
            recovery_references: row.get::<_, Option<i64>>(13)?.map(|value| value as u64),
            result_count: row.get::<_, Option<i64>>(14)?.map(|value| value as u64),
            indexed_files: row.get::<_, Option<i64>>(15)?.map(|value| value as u64),
            indexed_bytes: row.get::<_, Option<i64>>(16)?.map(|value| value as u64),
            changed_files: row.get::<_, Option<i64>>(17)?.map(|value| value as u64),
            changed_bytes: row.get::<_, Option<i64>>(18)?.map(|value| value as u64),
            project_edges: row.get::<_, Option<i64>>(19)?.map(|value| value as u64),
            project_edge_truncations: row.get::<_, Option<i64>>(20)?.map(|value| value as u64),
            project_mentions: row.get::<_, Option<i64>>(21)?.map(|value| value as u64),
            project_mentions_truncated: row.get::<_, Option<i64>>(22)?.map(|value| value != 0),
            stale: row.get::<_, Option<i64>>(23)?.map(|value| value != 0),
            reindex_attempted: row.get::<_, Option<i64>>(24)?.map(|value| value != 0),
            semantic_state: row.get(25)?,
            reranker_state: row.get(26)?,
            breaker_open: row.get::<_, i64>(27)? != 0,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn closed_schema_ok(connection: &Connection) -> Result<bool> {
    let expected = BTreeMap::from([
        ("meta", BTreeSet::from(["key", "value"])),
        (
            "events",
            BTreeSet::from([
                "id",
                "ts_minute",
                "operation",
                "outcome",
                "error_category",
                "duration_ms",
                "recall_ms",
                "probe_ms",
                "citation_fetch_ms",
                "model_load_ms",
                "inference_ms",
                "response_bytes",
                "qualified_claims",
                "qualified_artifacts",
                "recovery_references",
                "result_count",
                "indexed_files",
                "indexed_bytes",
                "changed_files",
                "changed_bytes",
                "project_edges",
                "project_edge_truncations",
                "project_mentions",
                "project_mentions_truncated",
                "stale",
                "reindex_attempted",
                "semantic_state",
                "reranker_state",
                "breaker_open",
            ]),
        ),
        (
            "experiments",
            BTreeSet::from(["experiment_id", "created_minute", "primary_metric"]),
        ),
        (
            "experiment_pairs",
            BTreeSet::from(["pair_id", "experiment_id", "created_minute", "memory_first"]),
        ),
        (
            "experiment_observations",
            BTreeSet::from([
                "pair_id",
                "ts_minute",
                "arm",
                "attempt",
                "tokens",
                "elapsed_ms",
                "tool_calls",
                "completed",
                "completeness",
            ]),
        ),
    ]);
    let expected_tables = expected.keys().copied().collect::<BTreeSet<_>>();
    let mut tables = connection.prepare(
        "SELECT name FROM sqlite_master
          WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
    )?;
    let actual_tables = tables
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<BTreeSet<_>>>()?;
    if actual_tables
        != expected_tables
            .into_iter()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>()
    {
        return Ok(false);
    }
    for (table, expected_columns) in expected {
        let expected_columns = expected_columns
            .into_iter()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
        let actual = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<BTreeSet<_>>>()?;
        if actual != expected_columns {
            return Ok(false);
        }
    }
    Ok(true)
}

fn categorical_allowlists_ok(connection: &Connection) -> Result<bool> {
    let meta = connection
        .prepare("SELECT key, value FROM meta")?
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<BTreeMap<_, _>>>()?;
    let expected_schema = METRICS_SCHEMA_VERSION.to_string();
    if meta
        .keys()
        .any(|key| !matches!(key.as_str(), "schema_version" | "consent_at_minute"))
        || meta.get("schema_version") != Some(&expected_schema)
        || meta
            .get("consent_at_minute")
            .is_some_and(|value| value.parse::<i64>().is_err())
    {
        return Ok(false);
    }
    let mut statement = connection.prepare(
        "SELECT operation, outcome, error_category, semantic_state, reranker_state FROM events",
    )?;
    for row in statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
        ))
    })? {
        let (operation, outcome, error, semantic, reranker) = row?;
        if !EVENT_OPERATIONS.contains(&operation.as_str())
            || !EVENT_OUTCOMES.contains(&outcome.as_str())
            || !ERROR_CATEGORIES.contains(&error.as_str())
            || !RUNTIME_STATES.contains(&semantic.as_str())
            || !RUNTIME_STATES.contains(&reranker.as_str())
        {
            return Ok(false);
        }
    }
    let invalid_event_numeric: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM events
             WHERE duration_ms < 0 OR duration_ms > 86400000
                OR recall_ms < 0 OR probe_ms < 0 OR citation_fetch_ms < 0
                OR model_load_ms < 0 OR inference_ms < 0
                OR response_bytes < 0 OR qualified_claims < 0
                OR qualified_artifacts < 0 OR recovery_references < 0
                OR result_count < 0 OR indexed_files < 0 OR indexed_bytes < 0
                OR changed_files < 0 OR changed_bytes < 0
                OR project_edges < 0 OR project_edge_truncations < 0
                OR project_mentions < 0 OR project_mentions_truncated NOT IN (0, 1)
                OR stale NOT IN (0, 1) OR reindex_attempted NOT IN (0, 1)
                OR breaker_open NOT IN (0, 1)
         )",
        [],
        |row| row.get(0),
    )?;
    let invalid_experiment: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM experiments
             WHERE primary_metric NOT IN ('tokens', 'elapsed')
         )",
        [],
        |row| row.get(0),
    )?;
    let invalid_pair: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM experiment_pairs
             WHERE memory_first NOT IN (0, 1)
         )",
        [],
        |row| row.get(0),
    )?;
    let invalid_observation: bool = connection.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM experiment_observations
             WHERE arm NOT IN ('memory', 'baseline')
                OR attempt NOT IN (1, 2)
                OR tokens < 0 OR elapsed_ms < 0 OR tool_calls < 0
                OR completed NOT IN (0, 1)
                OR (completeness IS NOT NULL AND completeness NOT BETWEEN 1 AND 5)
         )",
        [],
        |row| row.get(0),
    )?;
    let mut experiment_ids = connection.prepare("SELECT experiment_id FROM experiments")?;
    let experiment_ids_ok = experiment_ids
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .all(|id| validate_opaque_id(id, "exp_").is_ok());
    let mut pair_ids = connection.prepare("SELECT pair_id FROM experiment_pairs")?;
    let pair_ids_ok = pair_ids
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .iter()
        .all(|id| validate_opaque_id(id, "pair_").is_ok());
    Ok(!invalid_event_numeric
        && !invalid_experiment
        && !invalid_pair
        && !invalid_observation
        && experiment_ids_ok
        && pair_ids_ok)
}

fn load_experiments(connection: &Connection, since: i64) -> Result<Vec<StoredExperiment>> {
    let mut statement = connection.prepare(
        "SELECT experiment_id, created_minute, primary_metric
           FROM experiments WHERE created_minute >= ?1 ORDER BY created_minute, experiment_id",
    )?;
    let rows = statement.query_map([since], |row| {
        Ok(StoredExperiment {
            experiment_id: row.get(0)?,
            created_minute: row.get(1)?,
            primary_metric: row.get(2)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn load_pairs_since(connection: &Connection, since: i64) -> Result<Vec<StoredPair>> {
    let mut statement = connection.prepare(
        "SELECT pair_id, experiment_id, created_minute, memory_first
           FROM experiment_pairs WHERE created_minute >= ?1 ORDER BY created_minute, pair_id",
    )?;
    let rows = statement.query_map([since], |row| {
        Ok(StoredPair {
            pair_id: row.get(0)?,
            experiment_id: row.get(1)?,
            created_minute: row.get(2)?,
            memory_first: row.get::<_, i64>(3)? != 0,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn load_observations_since(connection: &Connection, since: i64) -> Result<Vec<StoredObservation>> {
    let mut statement = connection.prepare(
        "SELECT pair_id, ts_minute, arm, attempt, tokens, elapsed_ms,
                tool_calls, completed, completeness
           FROM experiment_observations WHERE ts_minute >= ?1
          ORDER BY ts_minute, pair_id, attempt",
    )?;
    let rows = statement.query_map([since], observation_row)?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn observation_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredObservation> {
    Ok(StoredObservation {
        pair_id: row.get(0)?,
        ts_minute: row.get(1)?,
        arm: row.get(2)?,
        attempt: row.get::<_, i64>(3)? as u8,
        tokens: row.get::<_, i64>(4)? as u64,
        elapsed_ms: row.get::<_, i64>(5)? as u64,
        tool_calls: row.get::<_, i64>(6)? as u64,
        completed: row.get::<_, i64>(7)? != 0,
        completeness: row.get::<_, Option<i64>>(8)?.map(|value| value as u8),
    })
}

fn require_experiment(
    connection: &Connection,
    experiment_id: &str,
) -> Result<ExperimentPrimaryMetric> {
    let primary: Option<String> = connection
        .query_row(
            "SELECT primary_metric FROM experiments WHERE experiment_id = ?1",
            [experiment_id],
            |row| row.get(0),
        )
        .optional()?;
    ExperimentPrimaryMetric::parse(
        primary
            .as_deref()
            .ok_or_else(|| MemoryError::NotFound("experiment was not found".into()))?,
    )
}

fn load_experiment_deltas(
    connection: &Connection,
    experiment_id: &str,
) -> Result<Vec<ExperimentPairDelta>> {
    let mut statement = connection.prepare(
        "SELECT pair.pair_id, pair.memory_first,
                memory.tokens, baseline.tokens,
                memory.elapsed_ms, baseline.elapsed_ms,
                memory.tool_calls, baseline.tool_calls,
                memory.completed, baseline.completed,
                memory.completeness, baseline.completeness
           FROM experiment_pairs pair
           JOIN experiment_observations memory
             ON memory.pair_id = pair.pair_id AND memory.arm = 'memory'
           JOIN experiment_observations baseline
             ON baseline.pair_id = pair.pair_id AND baseline.arm = 'baseline'
          WHERE pair.experiment_id = ?1
          ORDER BY pair.created_minute, pair.pair_id",
    )?;
    let rows = statement.query_map([experiment_id], |row| {
        let memory_tokens = row.get::<_, i64>(2)?;
        let baseline_tokens = row.get::<_, i64>(3)?;
        let memory_elapsed = row.get::<_, i64>(4)?;
        let baseline_elapsed = row.get::<_, i64>(5)?;
        let memory_tools = row.get::<_, i64>(6)?;
        let baseline_tools = row.get::<_, i64>(7)?;
        Ok(ExperimentPairDelta {
            pair_id: row.get(0)?,
            memory_first: row.get::<_, i64>(1)? != 0,
            tokens_delta: memory_tokens - baseline_tokens,
            elapsed_ms_delta: memory_elapsed - baseline_elapsed,
            tool_calls_delta: memory_tools - baseline_tools,
            memory_completed: row.get::<_, i64>(8)? != 0,
            baseline_completed: row.get::<_, i64>(9)? != 0,
            memory_completeness: row.get::<_, Option<i64>>(10)?.map(|value| value as u8),
            baseline_completeness: row.get::<_, Option<i64>>(11)?.map(|value| value as u8),
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn load_percent_deltas(
    connection: &Connection,
    experiment_id: &str,
    column: &str,
) -> Result<Vec<f64>> {
    let column = match column {
        "tokens" => "tokens",
        "elapsed_ms" => "elapsed_ms",
        "tool_calls" => "tool_calls",
        _ => return Err(MemoryError::Integrity("invalid experiment metric".into())),
    };
    let sql = format!(
        "SELECT memory.{column}, baseline.{column}
           FROM experiment_pairs pair
           JOIN experiment_observations memory
             ON memory.pair_id = pair.pair_id AND memory.arm = 'memory'
           JOIN experiment_observations baseline
             ON baseline.pair_id = pair.pair_id AND baseline.arm = 'baseline'
          WHERE pair.experiment_id = ?1"
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([experiment_id], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
    })?;
    let mut percentages = Vec::new();
    for row in rows {
        let (memory, baseline) = row?;
        if baseline != 0 {
            percentages.push((memory - baseline) as f64 / baseline as f64 * 100.0);
        }
    }
    Ok(percentages)
}

fn paired_summary(deltas: &[(i64, i64)]) -> PairedMetricSummary {
    let values = deltas
        .iter()
        .map(|(delta, _)| *delta as f64)
        .collect::<Vec<_>>();
    let favors_memory = deltas.iter().filter(|(delta, _)| *delta < 0).count();
    let favors_baseline = deltas.iter().filter(|(delta, _)| *delta > 0).count();
    let ties = deltas.len() - favors_memory - favors_baseline;
    PairedMetricSummary {
        median_delta_memory_minus_baseline: median(values),
        median_percent_delta: None,
        favors_memory,
        favors_baseline,
        ties,
        sign_test_two_sided_p: sign_test(favors_memory, favors_baseline),
    }
}

fn with_percent(mut summary: PairedMetricSummary, percentages: Vec<f64>) -> PairedMetricSummary {
    summary.median_percent_delta = median(percentages);
    summary
}

fn median(mut values: Vec<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        Some((values[middle - 1] + values[middle]) / 2.0)
    } else {
        Some(values[middle])
    }
}

fn sign_test(memory_wins: usize, baseline_wins: usize) -> Option<f64> {
    let n = memory_wins + baseline_wins;
    if n == 0 {
        return None;
    }
    let tail = memory_wins.min(baseline_wins);
    let mut coefficient = 1.0;
    let mut cumulative = 1.0;
    for index in 1..=tail {
        coefficient *= (n + 1 - index) as f64 / index as f64;
        cumulative += coefficient;
    }
    Some((2.0 * cumulative / 2.0_f64.powi(n as i32)).min(1.0))
}

fn write_jsonl(writer: &mut impl Write, row: &ExportRow) -> Result<()> {
    serde_json::to_writer(&mut *writer, row)?;
    writer.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::init_marker;
    use tempfile::TempDir;

    fn enabled_store() -> (TempDir, MetricsStore) {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path().join("project");
        let data = temporary.path().join("data");
        fs::create_dir(&root).unwrap();
        init_marker(&root, "metrics-test", None).unwrap();
        let mut store = MetricsStore::discover(&root, &data).unwrap();
        store
            .configure(Some(true), Some(14), Some(1024 * 1024), Some(1.0))
            .unwrap();
        (temporary, store)
    }

    #[test]
    fn disabled_metrics_create_no_database() {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path().join("project");
        let data = temporary.path().join("data");
        fs::create_dir(&root).unwrap();
        init_marker(&root, "metrics-test", None).unwrap();
        let mut store = MetricsStore::discover(&root, &data).unwrap();
        let marker_before = fs::read(root.join(MARKER_FILE)).unwrap();
        store.configure(Some(false), Some(7), None, None).unwrap();
        assert_eq!(fs::read(root.join(MARKER_FILE)).unwrap(), marker_before);
        assert_eq!(
            MetricsStore::discover(&root, &data)
                .unwrap()
                .config()
                .retention_days,
            7
        );
        assert!(
            !store
                .record_event(&MetricEvent::new("memory.retrieve", "abstained", 1.0))
                .unwrap()
        );
        assert!(!store.database_path.exists());
    }

    #[test]
    fn schema_one_metrics_store_adds_project_map_telemetry_automatically() {
        let (_temporary, store) = enabled_store();
        if store.database_path.exists() {
            fs::remove_file(&store.database_path).unwrap();
        }
        fs::create_dir_all(store.database_path.parent().unwrap()).unwrap();
        let connection = Connection::open(&store.database_path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO meta(key, value) VALUES ('schema_version', '1');
                 CREATE TABLE events(
                    id INTEGER PRIMARY KEY,
                    ts_minute INTEGER NOT NULL,
                    operation TEXT NOT NULL,
                    outcome TEXT NOT NULL,
                    error_category TEXT NOT NULL,
                    duration_ms REAL NOT NULL,
                    recall_ms REAL, probe_ms REAL, citation_fetch_ms REAL,
                    model_load_ms REAL, inference_ms REAL, response_bytes INTEGER,
                    qualified_claims INTEGER, qualified_artifacts INTEGER,
                    recovery_references INTEGER, result_count INTEGER,
                    indexed_files INTEGER, indexed_bytes INTEGER,
                    changed_files INTEGER, changed_bytes INTEGER,
                    stale INTEGER, reindex_attempted INTEGER,
                    semantic_state TEXT NOT NULL, reranker_state TEXT NOT NULL,
                    breaker_open INTEGER NOT NULL
                 );",
            )
            .unwrap();
        drop(connection);

        let connection = store.open_database().unwrap();
        let version = connection
            .query_row(
                "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap();
        assert_eq!(version, METRICS_SCHEMA_VERSION);
        let columns = connection
            .prepare("PRAGMA table_info(events)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<rusqlite::Result<BTreeSet<_>>>()
            .unwrap();
        for column in [
            "project_edges",
            "project_edge_truncations",
            "project_mentions",
            "project_mentions_truncated",
        ] {
            assert!(columns.contains(column));
        }
    }

    #[test]
    fn event_store_has_closed_privacy_schema_and_reports() {
        let (_temporary, store) = enabled_store();
        let mut event = MetricEvent::new("memory.retrieve", "recovered", 12.5);
        event.recall_ms = Some(4.0);
        event.response_bytes = Some(1024);
        event.recovery_references = Some(2);
        assert!(store.record_event(&event).unwrap());
        let report = store.report(7).unwrap();
        assert_eq!(report.operations.len(), 1);
        assert_eq!(report.operations[0].outcomes["recovered"], 1);
        let doctor = store.doctor().unwrap();
        assert_eq!(doctor.status, "ok");
        assert!(doctor.pragmas_ok);
        assert!(doctor.closed_schema_ok);
        assert!(doctor.categorical_allowlists_ok);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                fs::metadata(&store.data_dir).unwrap().permissions().mode() & 0o077,
                0
            );
            assert_eq!(
                fs::metadata(&store.database_path)
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o077,
                0
            );
        }
    }

    #[test]
    fn experiments_require_randomized_first_arm_and_report_pairs() {
        let (_temporary, store) = enabled_store();
        let experiment = store
            .begin_experiment(ExperimentPrimaryMetric::Tokens)
            .unwrap();
        let pair = store.create_pair(&experiment.experiment_id).unwrap();
        let first = pair.first_arm;
        let second = pair.second_arm;
        let second_early = store.record_observation(&ExperimentObservationInput {
            pair_id: pair.pair_id.clone(),
            arm: second,
            tokens: 80,
            elapsed_ms: 900,
            tool_calls: 2,
            completed: true,
            completeness: Some(5),
        });
        assert!(second_early.is_err());
        store
            .record_observation(&ExperimentObservationInput {
                pair_id: pair.pair_id.clone(),
                arm: first,
                tokens: if first == ExperimentArm::Memory {
                    80
                } else {
                    100
                },
                elapsed_ms: if first == ExperimentArm::Memory {
                    900
                } else {
                    1000
                },
                tool_calls: 2,
                completed: true,
                completeness: Some(5),
            })
            .unwrap();
        store
            .record_observation(&ExperimentObservationInput {
                pair_id: pair.pair_id.clone(),
                arm: second,
                tokens: if second == ExperimentArm::Memory {
                    80
                } else {
                    100
                },
                elapsed_ms: if second == ExperimentArm::Memory {
                    900
                } else {
                    1000
                },
                tool_calls: 2,
                completed: true,
                completeness: Some(5),
            })
            .unwrap();
        let report = store
            .experiment_report(&experiment.experiment_id, true)
            .unwrap();
        assert_eq!(report.complete_pairs, 1);
        assert_eq!(report.tokens.favors_memory, 1);
        assert_eq!(report.tokens.median_percent_delta, Some(-20.0));
        assert_eq!(report.pairs.unwrap()[0].tokens_delta, -20);
        let connection = store.open_database().unwrap();
        connection
            .execute(
                "UPDATE experiment_observations SET ts_minute = ?1",
                [now_minute() - 30 * 24 * 60],
            )
            .unwrap();
        drop(connection);
        let retained = store
            .experiment_report(&experiment.experiment_id, false)
            .unwrap();
        assert_eq!(retained.complete_pairs, 1);
        assert_eq!(retained.unpaired_excluded, 0);
    }

    #[test]
    fn export_contains_no_content_channels() {
        let (temporary, store) = enabled_store();
        store
            .record_event(&MetricEvent::new("project.search", "hits", 3.0))
            .unwrap();
        let output = temporary.path().join("metrics.jsonl");
        let report = store.export_jsonl(&output, 7).unwrap();
        assert_eq!(report.rows, 1);
        let exported = fs::read_to_string(output).unwrap();
        for line in exported.lines() {
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            let keys = value
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>();
            for forbidden in ["query", "content", "citation", "path", "prompt"] {
                assert!(!keys.contains(forbidden));
            }
        }
        let inside_project = store.root.join("metrics.jsonl");
        assert!(store.export_jsonl(&inside_project, 7).is_err());
    }

    #[test]
    fn retention_prunes_old_rows_and_clear_removes_the_store() {
        let (_temporary, store) = enabled_store();
        store
            .record_event(&MetricEvent::new("memory.retrieve", "abstained", 1.0))
            .unwrap();
        let connection = store.open_database().unwrap();
        connection
            .execute(
                "UPDATE events SET ts_minute = ?1",
                [now_minute() - 15 * 24 * 60],
            )
            .unwrap();
        drop(connection);
        assert_eq!(store.status().unwrap().recorded_events, 0);
        store
            .record_event(&MetricEvent::new("memory.retrieve", "qualified", 2.0))
            .unwrap();
        let status = store.status().unwrap();
        assert_eq!(status.recorded_events, 1);
        let stale_journal = store.database_path.with_extension("sqlite3-journal");
        fs::write(&stale_journal, b"stale journal bytes").unwrap();
        assert!(store.clear(true).unwrap());
        assert!(!store.database_path.exists());
        assert!(!stale_journal.exists());
        assert!(!store.status().unwrap().database_exists);
    }

    #[test]
    fn zero_sampling_records_no_event() {
        let (_temporary, mut store) = enabled_store();
        store.configure(None, None, None, Some(0.0)).unwrap();
        assert!(
            !store
                .record_event(&MetricEvent::new("memory.retrieve", "qualified", 1.0))
                .unwrap()
        );
        assert_eq!(store.status().unwrap().recorded_events, 0);
    }

    #[test]
    fn contended_metrics_write_drops_without_waiting() {
        let (_temporary, store) = enabled_store();
        let connection = store.open_database().unwrap();
        connection.execute_batch("BEGIN EXCLUSIVE").unwrap();
        let started = std::time::Instant::now();
        let result = store.record_event(&MetricEvent::new("memory.retrieve", "qualified", 1.0));
        assert!(result.is_err());
        assert!(started.elapsed() < std::time::Duration::from_millis(250));
        connection.execute_batch("ROLLBACK").unwrap();
    }

    #[test]
    fn sign_test_is_two_sided_and_exact() {
        assert_eq!(sign_test(0, 0), None);
        assert_eq!(sign_test(1, 0), Some(1.0));
        assert!((sign_test(7, 2).unwrap() - 0.1796875).abs() < 1e-12);
    }
}
