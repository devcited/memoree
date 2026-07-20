//! Optional local dense retrieval projection.
//!
//! SQLite authority and exact citations stay in `store.rs`. This module owns a
//! disposable vector database and explicitly installed model files. Query
//! inference loads only verified local bytes; network-backed model resolution
//! is confined to the explicit installation command.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use candle_core::{DType, Device, IndexOp, Tensor};
use candle_nn::{Linear, Module, VarBuilder, linear};
use candle_transformers::models::bert::{BertModel, Config as BertConfig};
use chrono::Utc;
use hf_hub::{Repo, RepoType, api::sync::ApiBuilder};
use parking_lot::Mutex;
use rusqlite::{Connection, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use tokenizers::{
    EncodeInput, PaddingParams, PaddingStrategy, Tokenizer, TruncationParams, TruncationStrategy,
};

use crate::{
    error::{MemoryError, Result},
    protocol::{
        EntityType, RerankerCircuitBreakerStatus, RerankerRetrievalStatus, SemanticRetrievalStatus,
    },
};

pub const SEMANTIC_MODEL_ID: &str = "snowflake/snowflake-arctic-embed-s";
pub const SEMANTIC_MODEL_REVISION: &str = "e596f507467533e48a2e17c007f0e1dacc837b33";
pub const SEMANTIC_QUERY_PREFIX: &str = "Represent this sentence for searching relevant passages: ";
pub const SEMANTIC_DIMENSIONS: usize = 384;
pub const SEMANTIC_POLICY_VERSION: &str = "local_dense_v2";
pub const RERANKER_MODEL_ID: &str = "cross-encoder/ms-marco-MiniLM-L12-v2";
pub const RERANKER_MODEL_REVISION: &str = "7b0235231ca2674cb8ca8f022859a6eba2b1c968";
pub const RERANKER_POLICY_VERSION: &str = "cross_encoder_ordering_v2";
const COMPATIBLE_RERANKER_INSTALL_POLICY_VERSIONS: &[&str] =
    &["cross_encoder_ordering_v1", "cross_encoder_ordering_v2"];
/// Floor for retaining a dense candidate before fusion. Cosine is never an
/// answerability threshold; it only bounds the local candidate pool.
pub const SEMANTIC_CANDIDATE_MIN_SIMILARITY: f64 = 0.48;
const MODEL_MANIFEST_SCHEMA: u32 = 2;
const RERANKER_MANIFEST_SCHEMA: u32 = 1;
const PROJECTION_SCHEMA: i64 = 1;
const SEMANTIC_REBUILD_BATCH_SIZE: usize = 8;
pub const RERANKER_MAX_SEQUENCE_TOKENS: usize = 256;
pub const RERANKER_MAX_QUERY_TOKENS: usize = 96;
pub const RERANKER_INFERENCE_BATCH_SIZE: usize = 16;
pub const RERANKER_ORDERING_CANDIDATE_LIMIT: usize = 16;
/// Pre-registered local CPU budget for ordering 16 short candidate passages.
/// Qualification has a separate, currently unfulfilled calibration gate.
pub const RERANKER_ORDERING_P95_BUDGET_MS: f64 = 500.0;
pub const RERANKER_BREAKER_TRIP_THRESHOLD: usize = 3;
pub const RERANKER_BREAKER_PROBE_AFTER_SKIPS: usize = 32;
const MODEL_FILES: &[(&str, &str)] = &[
    ("model.safetensors", "model.safetensors"),
    ("tokenizer.json", "tokenizer.json"),
    ("config.json", "config.json"),
    ("special_tokens_map.json", "special_tokens_map.json"),
    ("tokenizer_config.json", "tokenizer_config.json"),
];
const RERANKER_FILES: &[(&str, &str)] = &[
    ("model.safetensors", "model.safetensors"),
    ("tokenizer.json", "tokenizer.json"),
    ("config.json", "config.json"),
    ("special_tokens_map.json", "special_tokens_map.json"),
    ("tokenizer_config.json", "tokenizer_config.json"),
];

const PROJECTION_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;
INSERT OR IGNORE INTO meta(key, value) VALUES
    ('schema_version', '1'),
    ('model_id', ''),
    ('model_revision', ''),
    ('indexed_commit_seq', '0'),
    ('built_at', '');
CREATE TABLE IF NOT EXISTS vectors (
    entity_type TEXT NOT NULL CHECK(entity_type IN ('artifact', 'claim')),
    entity_id TEXT NOT NULL,
    revision_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
    start_byte INTEGER,
    end_byte INTEGER,
    revision_hash TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    embedding BLOB NOT NULL,
    commit_seq INTEGER NOT NULL,
    PRIMARY KEY(entity_type, revision_id, ordinal),
    CHECK((start_byte IS NULL AND end_byte IS NULL)
       OR (start_byte >= 0 AND end_byte > start_byte))
) STRICT;
CREATE INDEX IF NOT EXISTS vectors_revision_idx
    ON vectors(revision_id, entity_type);
"#;

impl SemanticRetrievalStatus {
    pub fn disabled(current_commit_seq: i64, reason: impl Into<String>) -> Self {
        Self {
            state: "disabled".into(),
            policy_version: SEMANTIC_POLICY_VERSION.into(),
            model_id: None,
            model_revision: None,
            indexed_commit_seq: 0,
            current_commit_seq,
            eligible_revision_count: 0,
            indexed_revision_count: 0,
            coverage: 0.0,
            reason: Some(reason.into()),
        }
    }
}

impl RerankerRetrievalStatus {
    pub fn disabled(candidate_count: usize, reason: impl Into<String>) -> Self {
        Self::disabled_on_surface(candidate_count, "claim", reason)
    }

    pub fn disabled_on_surface(
        candidate_count: usize,
        surface: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            state: "disabled".into(),
            policy_version: RERANKER_POLICY_VERSION.into(),
            role: "ordering_only".into(),
            surface: surface.into(),
            model_id: None,
            model_revision: None,
            candidate_count,
            scored_candidate_count: 0,
            ordering_applied: false,
            candidate_limit: RERANKER_ORDERING_CANDIDATE_LIMIT,
            candidate_limit_reached: candidate_count > RERANKER_ORDERING_CANDIDATE_LIMIT,
            inference_latency_ms: None,
            model_load_latency_ms: None,
            breaker: RerankerCircuitBreakerStatus {
                state: "closed".into(),
                budget_ms: RERANKER_ORDERING_P95_BUDGET_MS,
                trip_threshold: RERANKER_BREAKER_TRIP_THRESHOLD,
                consecutive_over_budget: 0,
                probe_after_skips: RERANKER_BREAKER_PROBE_AFTER_SKIPS,
                skipped_since_open: 0,
            },
            reason: Some(reason.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SemanticInstallReport {
    pub enabled: bool,
    pub model_id: String,
    pub model_revision: String,
    pub model_directory: String,
    pub projection_database: String,
    pub document_count: usize,
    pub vector_count: usize,
    pub reused_vector_count: usize,
    pub embedded_vector_count: usize,
    pub deleted_vector_count: usize,
    pub indexed_commit_seq: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RerankerInstallReport {
    pub enabled: bool,
    pub role: String,
    pub policy_version: String,
    pub model_id: String,
    pub model_revision: String,
    pub model_directory: String,
    pub candidate_limit: usize,
    pub inference_batch_size: usize,
    pub qualification_enabled: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SemanticRebuildStats {
    pub vector_count: usize,
    pub reused_vector_count: usize,
    pub embedded_vector_count: usize,
    pub deleted_vector_count: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct SemanticDocument {
    pub entity_type: EntityType,
    pub entity_id: String,
    pub revision_id: String,
    pub ordinal: usize,
    pub start_byte: Option<usize>,
    pub end_byte: Option<usize>,
    pub revision_hash: String,
    pub text: String,
    pub commit_seq: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct SemanticHit {
    pub entity_type: EntityType,
    pub entity_id: String,
    pub revision_id: String,
    pub start_byte: Option<u64>,
    pub end_byte: Option<u64>,
    pub similarity: f64,
}

#[derive(Debug, Clone)]
pub(crate) struct EligibleSemanticRevision {
    pub entity_type: EntityType,
    pub entity_id: String,
    pub revision_hash: String,
}

struct ExistingVector {
    entity_id: String,
    start_byte: Option<i64>,
    end_byte: Option<i64>,
    revision_hash: String,
    content_hash: String,
    embedding_bytes: i64,
    commit_seq: i64,
}

struct CandleEmbedding {
    model: BertModel,
    tokenizer: Tokenizer,
    device: Device,
}

impl CandleEmbedding {
    fn load(model_directory: &Path, safetensors_path: &Path) -> Result<Self> {
        let device = Device::Cpu;
        let config: BertConfig =
            serde_json::from_slice(&fs::read(model_directory.join("config.json"))?)?;
        if config.hidden_size != SEMANTIC_DIMENSIONS || config.max_position_embeddings != 512 {
            return Err(MemoryError::Integrity(
                "Candle semantic model configuration is incompatible".into(),
            ));
        }
        let weights = fs::read(safetensors_path)?;
        let builder = VarBuilder::from_buffered_safetensors(weights, DType::F32, &device)
            .map_err(candle_error)?;
        let model = BertModel::load(builder, &config).map_err(candle_error)?;
        let mut tokenizer =
            Tokenizer::from_bytes(fs::read(model_directory.join("tokenizer.json"))?)
                .map_err(semantic_error)?;
        tokenizer.with_padding(Some(PaddingParams {
            strategy: PaddingStrategy::BatchLongest,
            pad_id: u32::try_from(config.pad_token_id).map_err(|_| MemoryError::ContentTooLarge)?,
            pad_token: "[PAD]".into(),
            ..PaddingParams::default()
        }));
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: config.max_position_embeddings,
                ..TruncationParams::default()
            }))
            .map_err(semantic_error)?;
        Ok(Self {
            model,
            tokenizer,
            device,
        })
    }

    fn embed(&mut self, texts: &[&str]) -> Result<Vec<Vec<f32>>> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let encodings = self
            .tokenizer
            .encode_batch(texts.to_vec(), true)
            .map_err(semantic_error)?;
        let batch = encodings.len();
        let sequence_length = encodings
            .first()
            .map(|encoding| encoding.len())
            .unwrap_or_default();
        if sequence_length == 0 || encodings.iter().any(|item| item.len() != sequence_length) {
            return Err(MemoryError::Integrity(
                "semantic tokenizer returned an invalid padded batch".into(),
            ));
        }
        let input_ids = encodings
            .iter()
            .flat_map(|encoding| encoding.get_ids().iter().copied())
            .collect::<Vec<_>>();
        let token_type_ids = encodings
            .iter()
            .flat_map(|encoding| encoding.get_type_ids().iter().copied())
            .collect::<Vec<_>>();
        let attention_mask = encodings
            .iter()
            .flat_map(|encoding| encoding.get_attention_mask().iter().copied())
            .collect::<Vec<_>>();
        let input_ids = Tensor::from_vec(input_ids, (batch, sequence_length), &self.device)
            .map_err(candle_error)?;
        let token_type_ids =
            Tensor::from_vec(token_type_ids, (batch, sequence_length), &self.device)
                .map_err(candle_error)?;
        let attention_mask =
            Tensor::from_vec(attention_mask, (batch, sequence_length), &self.device)
                .map_err(candle_error)?;
        let hidden = self
            .model
            .forward(&input_ids, &token_type_ids, Some(&attention_mask))
            .map_err(candle_error)?;
        let cls = hidden.i((.., 0, ..)).map_err(candle_error)?;
        let mut embeddings = cls.to_vec2::<f32>().map_err(candle_error)?;
        for embedding in &mut embeddings {
            normalize_embedding(embedding)?;
        }
        Ok(embeddings)
    }
}

/// Pure-Rust reference implementation for a one-logit BERT cross-encoder.
/// It is intentionally isolated from search integration until logit parity,
/// latency, and calibration gates are satisfied.
pub(crate) struct CandleReranker {
    bert: BertModel,
    pooler: Linear,
    classifier: Linear,
    tokenizer: Tokenizer,
    device: Device,
}

impl CandleReranker {
    pub(crate) fn load(model_directory: &Path) -> Result<Self> {
        let device = Device::Cpu;
        let config: BertConfig =
            serde_json::from_slice(&fs::read(model_directory.join("config.json"))?)?;
        if config.hidden_size != 384
            || !matches!(config.num_hidden_layers, 6 | 12)
            || config.max_position_embeddings < RERANKER_MAX_SEQUENCE_TOKENS
        {
            return Err(MemoryError::Integrity(
                "Candle reranker model configuration is incompatible".into(),
            ));
        }
        let weights = fs::read(model_directory.join("model.safetensors"))?;
        let builder = VarBuilder::from_buffered_safetensors(weights, DType::F32, &device)
            .map_err(candle_error)?;
        let bert = BertModel::load(builder.pp("bert"), &config).map_err(candle_error)?;
        let pooler = linear(
            config.hidden_size,
            config.hidden_size,
            builder.pp("bert").pp("pooler").pp("dense"),
        )
        .map_err(candle_error)?;
        let classifier =
            linear(config.hidden_size, 1, builder.pp("classifier")).map_err(candle_error)?;
        let mut tokenizer =
            Tokenizer::from_bytes(fs::read(model_directory.join("tokenizer.json"))?)
                .map_err(semantic_error)?;
        tokenizer.with_padding(Some(PaddingParams {
            strategy: PaddingStrategy::BatchLongest,
            pad_id: u32::try_from(config.pad_token_id).map_err(|_| MemoryError::ContentTooLarge)?,
            pad_token: "[PAD]".into(),
            ..PaddingParams::default()
        }));
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: RERANKER_MAX_SEQUENCE_TOKENS,
                strategy: TruncationStrategy::LongestFirst,
                ..TruncationParams::default()
            }))
            .map_err(semantic_error)?;
        Ok(Self {
            bert,
            pooler,
            classifier,
            tokenizer,
            device,
        })
    }

    pub(crate) fn score(&mut self, query: &str, passages: &[&str]) -> Result<Vec<f32>> {
        self.score_with_batch_size(query, passages, RERANKER_INFERENCE_BATCH_SIZE)
    }

    fn score_with_batch_size(
        &mut self,
        query: &str,
        passages: &[&str],
        batch_size: usize,
    ) -> Result<Vec<f32>> {
        if passages.is_empty() {
            return Ok(Vec::new());
        }
        if batch_size == 0 {
            return Err(MemoryError::InvalidRequest(
                "reranker batch size must be positive".into(),
            ));
        }
        let query_tokens = self
            .tokenizer
            .encode(query, true)
            .map_err(semantic_error)?
            .len();
        if query_tokens > RERANKER_MAX_QUERY_TOKENS {
            return Err(MemoryError::InvalidRequest(format!(
                "reranker query has {query_tokens} tokens; maximum is {RERANKER_MAX_QUERY_TOKENS}"
            )));
        }
        let mut scores = Vec::with_capacity(passages.len());
        for passages in passages.chunks(batch_size) {
            let inputs = passages
                .iter()
                .map(|passage| EncodeInput::from((query, *passage)))
                .collect::<Vec<_>>();
            let encodings = self
                .tokenizer
                .encode_batch(inputs, true)
                .map_err(semantic_error)?;
            let batch = encodings.len();
            let sequence_length = encodings
                .first()
                .map(|encoding| encoding.len())
                .unwrap_or_default();
            if sequence_length == 0
                || encodings
                    .iter()
                    .any(|encoding| encoding.len() != sequence_length)
            {
                return Err(MemoryError::Integrity(
                    "reranker tokenizer returned an invalid padded batch".into(),
                ));
            }
            let input_ids = encodings
                .iter()
                .flat_map(|encoding| encoding.get_ids().iter().copied())
                .collect::<Vec<_>>();
            let token_type_ids = encodings
                .iter()
                .flat_map(|encoding| encoding.get_type_ids().iter().copied())
                .collect::<Vec<_>>();
            let attention_mask = encodings
                .iter()
                .flat_map(|encoding| encoding.get_attention_mask().iter().copied())
                .collect::<Vec<_>>();
            let input_ids = Tensor::from_vec(input_ids, (batch, sequence_length), &self.device)
                .map_err(candle_error)?;
            let token_type_ids =
                Tensor::from_vec(token_type_ids, (batch, sequence_length), &self.device)
                    .map_err(candle_error)?;
            let attention_mask =
                Tensor::from_vec(attention_mask, (batch, sequence_length), &self.device)
                    .map_err(candle_error)?;
            let hidden = self
                .bert
                .forward(&input_ids, &token_type_ids, Some(&attention_mask))
                .map_err(candle_error)?;
            let cls = hidden.i((.., 0, ..)).map_err(candle_error)?;
            let pooled = self
                .pooler
                .forward(&cls)
                .and_then(|values| values.tanh())
                .map_err(candle_error)?;
            let logits = self
                .classifier
                .forward(&pooled)
                .and_then(|values| values.squeeze(1))
                .map_err(candle_error)?;
            scores.extend(logits.to_vec1::<f32>().map_err(candle_error)?);
        }
        Ok(scores)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RerankerBreakerPhase {
    Closed,
    Open,
    HalfOpen,
}

impl RerankerBreakerPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half_open",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RerankerPermit {
    Score,
    Probe,
    Skip,
}

#[derive(Debug)]
struct RerankerBreaker {
    phase: RerankerBreakerPhase,
    consecutive_over_budget: usize,
    skipped_since_open: usize,
}

impl Default for RerankerBreaker {
    fn default() -> Self {
        Self {
            phase: RerankerBreakerPhase::Closed,
            consecutive_over_budget: 0,
            skipped_since_open: 0,
        }
    }
}

impl RerankerBreaker {
    fn permit(&mut self) -> RerankerPermit {
        match self.phase {
            RerankerBreakerPhase::Closed => RerankerPermit::Score,
            RerankerBreakerPhase::HalfOpen => RerankerPermit::Skip,
            RerankerBreakerPhase::Open
                if self.skipped_since_open >= RERANKER_BREAKER_PROBE_AFTER_SKIPS =>
            {
                self.phase = RerankerBreakerPhase::HalfOpen;
                self.skipped_since_open = 0;
                RerankerPermit::Probe
            }
            RerankerBreakerPhase::Open => {
                self.skipped_since_open += 1;
                RerankerPermit::Skip
            }
        }
    }

    fn record(&mut self, permit: RerankerPermit, inference_latency_ms: f64) {
        let over_budget = inference_latency_ms > RERANKER_ORDERING_P95_BUDGET_MS;
        match permit {
            RerankerPermit::Score => {
                if over_budget {
                    self.consecutive_over_budget += 1;
                    if self.consecutive_over_budget >= RERANKER_BREAKER_TRIP_THRESHOLD {
                        self.phase = RerankerBreakerPhase::Open;
                        self.skipped_since_open = 0;
                    }
                } else {
                    self.consecutive_over_budget = 0;
                }
            }
            RerankerPermit::Probe => {
                if over_budget {
                    self.phase = RerankerBreakerPhase::Open;
                    self.consecutive_over_budget = RERANKER_BREAKER_TRIP_THRESHOLD;
                    self.skipped_since_open = 0;
                } else {
                    self.phase = RerankerBreakerPhase::Closed;
                    self.consecutive_over_budget = 0;
                    self.skipped_since_open = 0;
                }
            }
            RerankerPermit::Skip => {}
        }
    }

    fn abort(&mut self, permit: RerankerPermit) {
        if permit == RerankerPermit::Probe {
            self.phase = RerankerBreakerPhase::Open;
            self.skipped_since_open = 0;
        }
    }

    fn reset(&mut self) {
        *self = Self::default();
    }

    fn public(&self) -> RerankerCircuitBreakerStatus {
        RerankerCircuitBreakerStatus {
            state: self.phase.as_str().into(),
            budget_ms: RERANKER_ORDERING_P95_BUDGET_MS,
            trip_threshold: RERANKER_BREAKER_TRIP_THRESHOLD,
            consecutive_over_budget: self.consecutive_over_budget,
            probe_after_skips: RERANKER_BREAKER_PROBE_AFTER_SKIPS,
            skipped_since_open: self.skipped_since_open,
        }
    }
}

#[derive(Clone)]
pub(crate) struct RerankerManager {
    root: PathBuf,
    runtime: Arc<Mutex<Option<CandleReranker>>>,
    verified_manifest: Arc<Mutex<Option<RerankerManifest>>>,
    model_load_latency_ms: Arc<Mutex<Option<f64>>>,
    initialization_error: Arc<Mutex<Option<String>>>,
    breaker: Arc<Mutex<RerankerBreaker>>,
}

impl RerankerManager {
    pub fn new(data_dir: &Path) -> Self {
        let manager = Self {
            root: data_dir.join("semantic/reranker"),
            runtime: Arc::new(Mutex::new(None)),
            verified_manifest: Arc::new(Mutex::new(None)),
            model_load_latency_ms: Arc::new(Mutex::new(None)),
            initialization_error: Arc::new(Mutex::new(None)),
            breaker: Arc::new(Mutex::new(RerankerBreaker::default())),
        };
        if manager.manifest_path().is_file()
            && let Err(error) = manager.initialize_runtime()
        {
            *manager.initialization_error.lock() = Some(error.to_string());
        }
        manager
    }

    fn model_dir(&self) -> PathBuf {
        self.root.join("model")
    }

    fn manifest_path(&self) -> PathBuf {
        self.root.join("model-manifest.json")
    }

    pub(crate) fn is_installed(&self) -> bool {
        self.manifest_path().is_file()
    }

    fn initialize_runtime(&self) -> Result<()> {
        let _manifest = self.verified_manifest()?;
        let started = Instant::now();
        let mut runtime = CandleReranker::load(&self.model_dir())?;
        let model_load_latency_ms = started.elapsed().as_secs_f64() * 1000.0;
        // The warm-up is deliberately outside request handling. Its timing is
        // not breaker input and it never contributes ranking output.
        runtime.score("memory retrieval warmup", &["memory retrieval warmup"])?;
        *self.runtime.lock() = Some(runtime);
        *self.model_load_latency_ms.lock() = Some(model_load_latency_ms);
        *self.initialization_error.lock() = None;
        self.breaker.lock().reset();
        Ok(())
    }

    pub fn install_model(&self) -> Result<RerankerInstallReport> {
        ensure_private_directory(&self.root)?;
        let download_cache = self.root.join("download-cache");
        ensure_private_directory(&download_cache)?;
        let api = ApiBuilder::new()
            .with_cache_dir(download_cache.clone())
            .with_progress(false)
            .build()
            .map_err(semantic_error)?;
        let repository = api.repo(Repo::with_revision(
            RERANKER_MODEL_ID.into(),
            RepoType::Model,
            RERANKER_MODEL_REVISION.into(),
        ));
        let staging = tempfile::Builder::new()
            .prefix(".reranker-model-")
            .tempdir_in(&self.root)?;
        let mut files = BTreeMap::new();
        for (target_name, source_name) in RERANKER_FILES {
            let source = repository.get(source_name).map_err(semantic_error)?;
            let bytes = fs::read(&source).map_err(|error| {
                MemoryError::Integrity(format!(
                    "reranker model snapshot is missing {source_name}: {error}"
                ))
            })?;
            let target = staging.path().join(target_name);
            fs::write(&target, &bytes)?;
            set_private_file(&target)?;
            files.insert(
                (*target_name).to_owned(),
                blake3::hash(&bytes).to_hex().to_string(),
            );
        }
        let manifest = RerankerManifest::new(files);
        self.activate_staged_model(&staging, &manifest)?;
        if download_cache.exists() {
            fs::remove_dir_all(download_cache)?;
        }
        Ok(self.install_report(&manifest))
    }

    /// Install local bytes under the pinned model identity. The source is
    /// loaded once before activation and every installed byte is digest-pinned.
    /// No network access occurs on this path or during retrieval.
    pub fn install_model_from_directory(&self, source: &Path) -> Result<RerankerInstallReport> {
        let _validated = CandleReranker::load(source)?;
        ensure_private_directory(&self.root)?;
        let staging = tempfile::Builder::new()
            .prefix(".reranker-model-")
            .tempdir_in(&self.root)?;
        let mut files = BTreeMap::new();
        for (target_name, source_name) in RERANKER_FILES {
            let bytes = fs::read(source.join(source_name)).map_err(|error| {
                MemoryError::Integrity(format!(
                    "reranker model source is missing {source_name}: {error}"
                ))
            })?;
            let target = staging.path().join(target_name);
            fs::write(&target, &bytes)?;
            set_private_file(&target)?;
            files.insert(
                (*target_name).to_owned(),
                blake3::hash(&bytes).to_hex().to_string(),
            );
        }
        let manifest = RerankerManifest::new(files);
        self.activate_staged_model(&staging, &manifest)?;
        Ok(self.install_report(&manifest))
    }

    fn activate_staged_model(
        &self,
        staging: &tempfile::TempDir,
        manifest: &RerankerManifest,
    ) -> Result<()> {
        validate_current_reranker_manifest_shape(manifest)?;
        let started = Instant::now();
        let mut validated = CandleReranker::load(staging.path())?;
        let model_load_latency_ms = started.elapsed().as_secs_f64() * 1000.0;
        validated.score("memory retrieval warmup", &["memory retrieval warmup"])?;
        let manifest_bytes = serde_json::to_vec_pretty(manifest)?;
        let staged_manifest = staging.path().join("model-manifest.json");
        fs::write(&staged_manifest, &manifest_bytes)?;
        set_private_file(&staged_manifest)?;

        let model_dir = self.model_dir();
        let previous = self.root.join("model.previous");
        if previous.exists() {
            fs::remove_dir_all(&previous)?;
        }
        if model_dir.exists() {
            fs::rename(&model_dir, &previous)?;
        }
        if let Err(error) = fs::rename(staging.path(), &model_dir) {
            if previous.exists() {
                let _ = fs::rename(&previous, &model_dir);
            }
            return Err(error.into());
        }
        if previous.exists() {
            fs::remove_dir_all(previous)?;
        }
        fs::write(self.manifest_path(), &manifest_bytes)?;
        set_private_file(&self.manifest_path())?;
        *self.runtime.lock() = Some(validated);
        *self.verified_manifest.lock() = Some(manifest.clone());
        *self.model_load_latency_ms.lock() = Some(model_load_latency_ms);
        *self.initialization_error.lock() = None;
        self.breaker.lock().reset();
        Ok(())
    }

    fn install_report(&self, manifest: &RerankerManifest) -> RerankerInstallReport {
        RerankerInstallReport {
            enabled: true,
            role: "ordering_only".into(),
            policy_version: RERANKER_POLICY_VERSION.into(),
            model_id: manifest.model_id.clone(),
            model_revision: manifest.model_revision.clone(),
            model_directory: self.model_dir().display().to_string(),
            candidate_limit: RERANKER_ORDERING_CANDIDATE_LIMIT,
            inference_batch_size: RERANKER_INFERENCE_BATCH_SIZE,
            qualification_enabled: false,
        }
    }

    pub fn status(&self) -> Result<RerankerRetrievalStatus> {
        if !self.manifest_path().is_file() {
            let mut status = RerankerRetrievalStatus::disabled_on_surface(
                0,
                "control_plane",
                "reranker model is not installed",
            );
            status.breaker = self.breaker.lock().public();
            return Ok(status);
        }
        let manifest = self.verified_manifest()?;
        let initialization_error = self.initialization_error.lock().clone();
        Ok(RerankerRetrievalStatus {
            state: if initialization_error.is_some() {
                "error".into()
            } else {
                "ready".into()
            },
            policy_version: RERANKER_POLICY_VERSION.into(),
            role: "ordering_only".into(),
            surface: "control_plane".into(),
            model_id: Some(manifest.model_id),
            model_revision: Some(manifest.model_revision),
            candidate_count: 0,
            scored_candidate_count: 0,
            ordering_applied: false,
            candidate_limit: RERANKER_ORDERING_CANDIDATE_LIMIT,
            candidate_limit_reached: false,
            inference_latency_ms: None,
            model_load_latency_ms: *self.model_load_latency_ms.lock(),
            breaker: self.breaker.lock().public(),
            reason: Some(initialization_error.map_or_else(
                || "qualification is disabled until the powered calibration gate passes".into(),
                |error| format!("startup warm-up failed; ordering will fail open: {error}"),
            )),
        })
    }

    pub fn surface_disabled(
        &self,
        surface: &str,
        candidate_count: usize,
    ) -> Result<RerankerRetrievalStatus> {
        let mut status = self.status()?;
        status.state = "surface_disabled".into();
        status.surface = surface.into();
        status.candidate_count = candidate_count;
        status.candidate_limit_reached = candidate_count > RERANKER_ORDERING_CANDIDATE_LIMIT;
        status.scored_candidate_count = 0;
        status.ordering_applied = false;
        status.inference_latency_ms = None;
        status.reason = Some(
            "cross-encoder ordering is disabled on this surface; deterministic fused order is used"
                .into(),
        );
        Ok(status)
    }

    pub fn score(
        &self,
        query: &str,
        passages: &[&str],
        candidate_count: usize,
    ) -> Result<(Vec<f32>, RerankerRetrievalStatus)> {
        if passages.is_empty() {
            let mut status = self.status()?;
            status.surface = "claim".into();
            status.candidate_count = candidate_count;
            status.candidate_limit_reached = candidate_count > RERANKER_ORDERING_CANDIDATE_LIMIT;
            status.reason = Some("no non-exact candidates required model ordering".into());
            return Ok((Vec::new(), status));
        }
        if !self.manifest_path().is_file() {
            let mut status = RerankerRetrievalStatus::disabled(
                candidate_count,
                "reranker model is not installed",
            );
            status.breaker = self.breaker.lock().public();
            return Ok((Vec::new(), status));
        }
        let manifest = self.verified_manifest()?;
        if let Some(error) = self.initialization_error.lock().clone() {
            return Err(MemoryError::Integrity(format!(
                "reranker startup warm-up failed: {error}"
            )));
        }
        let permit = self.breaker.lock().permit();
        if permit == RerankerPermit::Skip {
            return Ok((
                Vec::new(),
                RerankerRetrievalStatus {
                    state: "breaker_open".into(),
                    policy_version: RERANKER_POLICY_VERSION.into(),
                    role: "ordering_only".into(),
                    surface: "claim".into(),
                    model_id: Some(manifest.model_id),
                    model_revision: Some(manifest.model_revision),
                    candidate_count,
                    scored_candidate_count: 0,
                    ordering_applied: false,
                    candidate_limit: RERANKER_ORDERING_CANDIDATE_LIMIT,
                    candidate_limit_reached: candidate_count > RERANKER_ORDERING_CANDIDATE_LIMIT,
                    inference_latency_ms: None,
                    model_load_latency_ms: *self.model_load_latency_ms.lock(),
                    breaker: self.breaker.lock().public(),
                    reason: Some(
                        "latency breaker is open; deterministic fused order is used".into(),
                    ),
                },
            ));
        }
        let scored = {
            let mut runtime = self.runtime.lock();
            let Some(runtime) = runtime.as_mut() else {
                self.breaker.lock().abort(permit);
                return Err(MemoryError::Integrity(
                    "reranker runtime was not initialized during startup warm-up".into(),
                ));
            };
            let started = Instant::now();
            runtime
                .score(query, passages)
                .map(|scores| (scores, started.elapsed().as_secs_f64() * 1000.0))
        };
        let (scores, inference_latency_ms) = match scored {
            Ok(scored) => scored,
            Err(error) => {
                self.breaker.lock().abort(permit);
                return Err(error);
            }
        };
        self.breaker.lock().record(permit, inference_latency_ms);
        Ok((
            scores,
            RerankerRetrievalStatus {
                state: "ready".into(),
                policy_version: RERANKER_POLICY_VERSION.into(),
                role: "ordering_only".into(),
                surface: "claim".into(),
                model_id: Some(manifest.model_id),
                model_revision: Some(manifest.model_revision),
                candidate_count,
                scored_candidate_count: passages.len(),
                ordering_applied: passages.len() > 1,
                candidate_limit: RERANKER_ORDERING_CANDIDATE_LIMIT,
                candidate_limit_reached: candidate_count > RERANKER_ORDERING_CANDIDATE_LIMIT,
                inference_latency_ms: Some(inference_latency_ms),
                model_load_latency_ms: *self.model_load_latency_ms.lock(),
                breaker: self.breaker.lock().public(),
                reason: Some(
                    "raw logits are advisory ordering metadata; qualification is disabled".into(),
                ),
            },
        ))
    }

    fn verified_manifest(&self) -> Result<RerankerManifest> {
        let mut cached = self.verified_manifest.lock();
        if let Some(manifest) = cached.as_ref() {
            return Ok(manifest.clone());
        }
        let manifest: RerankerManifest = serde_json::from_slice(&fs::read(self.manifest_path())?)?;
        validate_compatible_reranker_manifest_shape(&manifest)?;
        for (name, expected_hash) in &manifest.files {
            let bytes = fs::read(self.model_dir().join(name))?;
            if blake3::hash(&bytes).to_hex().as_str() != expected_hash {
                return Err(MemoryError::Integrity(format!(
                    "reranker model file {name} failed digest verification"
                )));
            }
        }
        *cached = Some(manifest.clone());
        Ok(manifest)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RerankerManifest {
    schema_version: u32,
    model_id: String,
    model_revision: String,
    policy_version: String,
    role: String,
    files: BTreeMap<String, String>,
}

impl RerankerManifest {
    fn new(files: BTreeMap<String, String>) -> Self {
        Self {
            schema_version: RERANKER_MANIFEST_SCHEMA,
            model_id: RERANKER_MODEL_ID.into(),
            model_revision: RERANKER_MODEL_REVISION.into(),
            policy_version: RERANKER_POLICY_VERSION.into(),
            role: "ordering_only".into(),
            files,
        }
    }
}

fn validate_current_reranker_manifest_shape(manifest: &RerankerManifest) -> Result<()> {
    validate_reranker_manifest_shape(manifest, false)
}

fn validate_compatible_reranker_manifest_shape(manifest: &RerankerManifest) -> Result<()> {
    validate_reranker_manifest_shape(manifest, true)
}

fn validate_reranker_manifest_shape(
    manifest: &RerankerManifest,
    allow_compatible_install_origin: bool,
) -> Result<()> {
    let expected_files = RERANKER_FILES
        .iter()
        .map(|(name, _)| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    let policy_compatible = if allow_compatible_install_origin {
        COMPATIBLE_RERANKER_INSTALL_POLICY_VERSIONS.contains(&manifest.policy_version.as_str())
    } else {
        manifest.policy_version == RERANKER_POLICY_VERSION
    };
    if manifest.schema_version != RERANKER_MANIFEST_SCHEMA
        || manifest.model_id != RERANKER_MODEL_ID
        || manifest.model_revision != RERANKER_MODEL_REVISION
        || !policy_compatible
        || manifest.role != "ordering_only"
        || manifest.files.keys().cloned().collect::<BTreeSet<_>>() != expected_files
    {
        return Err(MemoryError::Integrity(
            "reranker model manifest is incompatible".into(),
        ));
    }
    Ok(())
}

#[derive(Clone)]
pub(crate) struct SemanticManager {
    root: PathBuf,
    runtime: Arc<Mutex<Option<CandleEmbedding>>>,
    verified_manifest: Arc<Mutex<Option<ModelManifest>>>,
}

impl SemanticManager {
    pub fn new(data_dir: &Path) -> Self {
        Self {
            root: data_dir.join("semantic"),
            runtime: Arc::new(Mutex::new(None)),
            verified_manifest: Arc::new(Mutex::new(None)),
        }
    }

    fn model_dir(&self) -> PathBuf {
        self.root.join("model")
    }

    fn manifest_path(&self) -> PathBuf {
        self.root.join("model-manifest.json")
    }

    pub(crate) fn is_installed(&self) -> bool {
        self.manifest_path().is_file()
    }

    fn projection_path(&self) -> PathBuf {
        self.root.join("semantic.sqlite3")
    }

    pub fn install_model(&self) -> Result<ModelManifest> {
        ensure_private_directory(&self.root)?;
        let download_cache = self.root.join("download-cache");
        ensure_private_directory(&download_cache)?;
        let api = ApiBuilder::new()
            .with_cache_dir(download_cache.clone())
            // CLI and protocol output must remain machine-readable.
            .with_progress(false)
            .build()
            .map_err(semantic_error)?;
        let repository = api.repo(Repo::with_revision(
            SEMANTIC_MODEL_ID.into(),
            RepoType::Model,
            SEMANTIC_MODEL_REVISION.into(),
        ));
        let staging = tempfile::Builder::new()
            .prefix(".semantic-model-")
            .tempdir_in(&self.root)?;
        let mut files = BTreeMap::new();
        for (target_name, source_name) in MODEL_FILES {
            let source = repository.get(source_name).map_err(semantic_error)?;
            let target = staging.path().join(target_name);
            let bytes = fs::read(&source).map_err(|error| {
                MemoryError::Integrity(format!(
                    "semantic model snapshot is missing {source_name}: {error}"
                ))
            })?;
            fs::write(&target, &bytes)?;
            set_private_file(&target)?;
            files.insert(
                (*target_name).to_owned(),
                blake3::hash(&bytes).to_hex().to_string(),
            );
        }
        let manifest = ModelManifest {
            schema_version: MODEL_MANIFEST_SCHEMA,
            model_id: SEMANTIC_MODEL_ID.into(),
            model_revision: SEMANTIC_MODEL_REVISION.into(),
            dimensions: SEMANTIC_DIMENSIONS,
            query_prefix: SEMANTIC_QUERY_PREFIX.into(),
            files,
        };
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        fs::write(staging.path().join("model-manifest.json"), &manifest_bytes)?;
        set_private_file(&staging.path().join("model-manifest.json"))?;

        let model_dir = self.model_dir();
        let previous = self.root.join("model.previous");
        if previous.exists() {
            fs::remove_dir_all(&previous)?;
        }
        if model_dir.exists() {
            fs::rename(&model_dir, &previous)?;
        }
        if let Err(error) = fs::rename(staging.path(), &model_dir) {
            if previous.exists() {
                let _ = fs::rename(&previous, &model_dir);
            }
            return Err(error.into());
        }
        if previous.exists() {
            fs::remove_dir_all(previous)?;
        }
        fs::write(self.manifest_path(), &manifest_bytes)?;
        set_private_file(&self.manifest_path())?;
        if download_cache.exists() {
            fs::remove_dir_all(download_cache)?;
        }
        *self.runtime.lock() = None;
        *self.verified_manifest.lock() = Some(manifest.clone());
        Ok(manifest)
    }

    /// Install an already downloaded, digest-manifested model directory.
    /// This path performs no network access and is suitable for CI, air-gapped
    /// hosts, and reproducible evaluation.
    pub fn install_model_from_directory(&self, source: &Path) -> Result<ModelManifest> {
        let manifest_path = source.join("model-manifest.json");
        let manifest: ModelManifest = serde_json::from_slice(&fs::read(&manifest_path)?)?;
        validate_manifest_shape(&manifest)?;
        ensure_private_directory(&self.root)?;
        let staging = tempfile::Builder::new()
            .prefix(".semantic-model-")
            .tempdir_in(&self.root)?;
        for (name, expected_hash) in &manifest.files {
            let bytes = fs::read(source.join(name))?;
            let actual_hash = blake3::hash(&bytes).to_hex().to_string();
            if &actual_hash != expected_hash {
                return Err(MemoryError::Integrity(format!(
                    "semantic model source file {name} failed digest verification"
                )));
            }
            let target = staging.path().join(name);
            fs::write(&target, bytes)?;
            set_private_file(&target)?;
        }
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        let staged_manifest = staging.path().join("model-manifest.json");
        fs::write(&staged_manifest, &manifest_bytes)?;
        set_private_file(&staged_manifest)?;

        let model_dir = self.model_dir();
        let previous = self.root.join("model.previous");
        if previous.exists() {
            fs::remove_dir_all(&previous)?;
        }
        if model_dir.exists() {
            fs::rename(&model_dir, &previous)?;
        }
        if let Err(error) = fs::rename(staging.path(), &model_dir) {
            if previous.exists() {
                let _ = fs::rename(&previous, &model_dir);
            }
            return Err(error.into());
        }
        if previous.exists() {
            fs::remove_dir_all(previous)?;
        }
        fs::write(self.manifest_path(), &manifest_bytes)?;
        set_private_file(&self.manifest_path())?;
        *self.runtime.lock() = None;
        *self.verified_manifest.lock() = Some(manifest.clone());
        Ok(manifest)
    }

    pub fn rebuild(
        &self,
        documents: &[SemanticDocument],
        indexed_commit_seq: i64,
    ) -> Result<(ModelManifest, SemanticRebuildStats)> {
        let manifest = self.verified_manifest()?;
        ensure_private_directory(&self.root)?;
        let path = self.projection_path();
        let mut connection = Connection::open(&path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.execute_batch(PROJECTION_SQL)?;
        let projection_model_matches = meta_string(&connection, "model_id")? == manifest.model_id
            && meta_string(&connection, "model_revision")? == manifest.model_revision;
        let existing = {
            let mut statement = connection.prepare(
                "SELECT entity_type, entity_id, revision_id, ordinal,
                        start_byte, end_byte, revision_hash, content_hash,
                        length(embedding), commit_seq
                   FROM vectors",
            )?;
            let rows = statement.query_map([], |row| {
                let entity_type = row.get::<_, String>(0)?;
                let revision_id = row.get::<_, String>(2)?;
                let ordinal = row.get::<_, i64>(3)?;
                Ok((
                    (entity_type, revision_id, ordinal),
                    ExistingVector {
                        entity_id: row.get(1)?,
                        start_byte: row.get(4)?,
                        end_byte: row.get(5)?,
                        revision_hash: row.get(6)?,
                        content_hash: row.get(7)?,
                        embedding_bytes: row.get(8)?,
                        commit_seq: row.get(9)?,
                    },
                ))
            })?;
            rows.collect::<rusqlite::Result<BTreeMap<_, _>>>()?
        };
        let mut desired_keys = BTreeSet::new();
        let mut changed = Vec::new();
        let mut reused_vector_count = 0usize;
        for document in documents {
            let ordinal =
                i64::try_from(document.ordinal).map_err(|_| MemoryError::ContentTooLarge)?;
            let key = (
                entity_type_name(document.entity_type).to_owned(),
                document.revision_id.clone(),
                ordinal,
            );
            if !desired_keys.insert(key.clone()) {
                return Err(MemoryError::Integrity(format!(
                    "duplicate semantic document key for revision {} ordinal {}",
                    document.revision_id, document.ordinal
                )));
            }
            let start_byte = document
                .start_byte
                .map(i64::try_from)
                .transpose()
                .map_err(|_| MemoryError::ContentTooLarge)?;
            let end_byte = document
                .end_byte
                .map(i64::try_from)
                .transpose()
                .map_err(|_| MemoryError::ContentTooLarge)?;
            let content_hash = blake3::hash(document.text.as_bytes()).to_hex().to_string();
            let reusable = projection_model_matches
                && existing.get(&key).is_some_and(|stored| {
                    stored.entity_id == document.entity_id
                        && stored.start_byte == start_byte
                        && stored.end_byte == end_byte
                        && stored.revision_hash == document.revision_hash
                        && stored.content_hash == content_hash
                        && stored.embedding_bytes == (SEMANTIC_DIMENSIONS * 4) as i64
                        && stored.commit_seq == document.commit_seq
                });
            if reusable {
                reused_vector_count += 1;
            } else {
                changed.push((document, ordinal, start_byte, end_byte, content_hash));
            }
        }
        let stale_keys = existing
            .keys()
            .filter(|key| !desired_keys.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        let embedded_vector_count = changed.len();
        let deleted_vector_count = stale_keys.len();
        let mut loaded_model = if changed.is_empty() {
            None
        } else {
            Some(self.load_model(&manifest)?)
        };
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for (entity_type, revision_id, ordinal) in &stale_keys {
            transaction.execute(
                "DELETE FROM vectors
                  WHERE entity_type = ?1 AND revision_id = ?2 AND ordinal = ?3",
                params![entity_type, revision_id, ordinal],
            )?;
        }
        for batch in changed.chunks(SEMANTIC_REBUILD_BATCH_SIZE) {
            let texts = batch
                .iter()
                .map(|(document, _, _, _, _)| document.text.as_str())
                .collect::<Vec<_>>();
            let embeddings = loaded_model
                .as_mut()
                .expect("changed documents require a loaded semantic model")
                .embed(&texts)?;
            if embeddings.len() != batch.len() {
                return Err(MemoryError::Integrity(
                    "semantic model returned the wrong embedding count".into(),
                ));
            }
            for ((document, ordinal, start_byte, end_byte, content_hash), mut embedding) in
                batch.iter().zip(embeddings)
            {
                if embedding.len() != SEMANTIC_DIMENSIONS {
                    return Err(MemoryError::Integrity(format!(
                        "semantic model returned {} dimensions, expected {SEMANTIC_DIMENSIONS}",
                        embedding.len()
                    )));
                }
                normalize_embedding(&mut embedding)?;
                transaction.execute(
                    "INSERT INTO vectors(
                         entity_type, entity_id, revision_id, ordinal,
                         start_byte, end_byte, revision_hash, content_hash,
                         embedding, commit_seq
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                     ON CONFLICT(entity_type, revision_id, ordinal) DO UPDATE SET
                         entity_id = excluded.entity_id,
                         start_byte = excluded.start_byte,
                         end_byte = excluded.end_byte,
                         revision_hash = excluded.revision_hash,
                         content_hash = excluded.content_hash,
                         embedding = excluded.embedding,
                         commit_seq = excluded.commit_seq",
                    params![
                        entity_type_name(document.entity_type),
                        document.entity_id,
                        document.revision_id,
                        ordinal,
                        start_byte,
                        end_byte,
                        document.revision_hash,
                        content_hash,
                        encode_embedding(&embedding),
                        document.commit_seq,
                    ],
                )?;
            }
        }
        for (key, value) in [
            ("schema_version", PROJECTION_SCHEMA.to_string()),
            ("model_id", manifest.model_id.clone()),
            ("model_revision", manifest.model_revision.clone()),
            ("indexed_commit_seq", indexed_commit_seq.to_string()),
            ("built_at", Utc::now().to_rfc3339()),
        ] {
            transaction.execute(
                "INSERT INTO meta(key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params![key, value],
            )?;
        }
        transaction.commit()?;
        set_private_file(&path)?;
        if let Some(model) = loaded_model {
            *self.runtime.lock() = Some(model);
        }
        Ok((
            manifest,
            SemanticRebuildStats {
                vector_count: documents.len(),
                reused_vector_count,
                embedded_vector_count,
                deleted_vector_count,
            },
        ))
    }

    pub fn search(
        &self,
        query: &str,
        eligible: &BTreeMap<String, EligibleSemanticRevision>,
        limit: usize,
        current_commit_seq: i64,
    ) -> Result<(Vec<SemanticHit>, SemanticRetrievalStatus)> {
        if eligible.is_empty() {
            return Ok((Vec::new(), self.status(current_commit_seq, eligible)?));
        }
        if !self.manifest_path().is_file() {
            return Ok((
                Vec::new(),
                SemanticRetrievalStatus::disabled(
                    current_commit_seq,
                    "semantic model is not installed",
                ),
            ));
        }
        let manifest = self.verified_manifest()?;
        if !self.projection_path().is_file() {
            return Ok((
                Vec::new(),
                SemanticRetrievalStatus::disabled(
                    current_commit_seq,
                    "semantic projection is not built",
                ),
            ));
        }
        let query_embedding = {
            let mut runtime = self.runtime.lock();
            if runtime.is_none() {
                *runtime = Some(self.load_model(&manifest)?);
            }
            let model = runtime.as_mut().expect("semantic runtime initialized");
            let prefixed = format!("{SEMANTIC_QUERY_PREFIX}{query}");
            let mut embedding = model
                .embed(&[prefixed.as_str()])?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    MemoryError::Integrity("semantic query embedding is empty".into())
                })?;
            normalize_embedding(&mut embedding)?;
            embedding
        };
        let connection = Connection::open_with_flags(
            self.projection_path(),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let indexed_commit_seq = meta_i64(&connection, "indexed_commit_seq")?;
        let projection_model_id = meta_string(&connection, "model_id")?;
        let projection_model_revision = meta_string(&connection, "model_revision")?;
        if projection_model_id != manifest.model_id
            || projection_model_revision != manifest.model_revision
        {
            return Ok((
                Vec::new(),
                SemanticRetrievalStatus {
                    state: "stale".into(),
                    policy_version: SEMANTIC_POLICY_VERSION.into(),
                    model_id: Some(manifest.model_id),
                    model_revision: Some(manifest.model_revision),
                    indexed_commit_seq,
                    current_commit_seq,
                    eligible_revision_count: eligible.len(),
                    indexed_revision_count: 0,
                    coverage: 0.0,
                    reason: Some(
                        "semantic projection was built by a different model revision; run `memoree semantic rebuild`"
                            .into(),
                    ),
                },
            ));
        }
        let revision_json = serde_json::to_string(&eligible.keys().collect::<Vec<_>>())?;
        let mut statement = connection.prepare(
            "SELECT entity_type, entity_id, revision_id, start_byte, end_byte,
                    revision_hash, embedding
               FROM vectors
              WHERE revision_id IN (SELECT value FROM json_each(?1))",
        )?;
        let rows = statement.query_map([revision_json], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<i64>>(3)?,
                row.get::<_, Option<i64>>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Vec<u8>>(6)?,
            ))
        })?;
        let mut best = BTreeMap::<(String, String), SemanticHit>::new();
        let mut indexed_revisions = BTreeSet::new();
        for row in rows {
            let (entity_type, entity_id, revision_id, start, end, revision_hash, bytes) = row?;
            let Some(expected) = eligible.get(&revision_id) else {
                continue;
            };
            if expected.entity_id != entity_id
                || expected.revision_hash != revision_hash
                || entity_type_name(expected.entity_type) != entity_type
            {
                continue;
            }
            let embedding = decode_embedding(&bytes)?;
            let similarity = cosine(&query_embedding, &embedding)?;
            indexed_revisions.insert(revision_id.clone());
            let hit = SemanticHit {
                entity_type: expected.entity_type,
                entity_id: entity_id.clone(),
                revision_id: revision_id.clone(),
                start_byte: start.map(|value| value as u64),
                end_byte: end.map(|value| value as u64),
                similarity,
            };
            best.entry((entity_type, revision_id))
                .and_modify(|current| {
                    if hit.similarity > current.similarity {
                        *current = hit.clone();
                    }
                })
                .or_insert(hit);
        }
        let mut hits = best.into_values().collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            right
                .similarity
                .partial_cmp(&left.similarity)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.revision_id.cmp(&right.revision_id))
        });
        hits.retain(|hit| hit.similarity >= SEMANTIC_CANDIDATE_MIN_SIMILARITY);
        hits.truncate(limit);
        let coverage = indexed_revisions.len() as f64 / eligible.len() as f64;
        let state = if indexed_commit_seq >= current_commit_seq && coverage == 1.0 {
            "ready"
        } else {
            "stale"
        };
        Ok((
            hits,
            SemanticRetrievalStatus {
                state: state.into(),
                policy_version: SEMANTIC_POLICY_VERSION.into(),
                model_id: Some(manifest.model_id),
                model_revision: Some(manifest.model_revision),
                indexed_commit_seq,
                current_commit_seq,
                eligible_revision_count: eligible.len(),
                indexed_revision_count: indexed_revisions.len(),
                coverage,
                reason: (state == "stale").then(|| {
                    format!(
                        "semantic projection covers {}/{} eligible revisions through commit {indexed_commit_seq}; missing or mismatched revisions were excluded; run `memoree semantic rebuild`",
                        indexed_revisions.len(),
                        eligible.len()
                    )
                }),
            },
        ))
    }

    pub fn status(
        &self,
        current_commit_seq: i64,
        eligible: &BTreeMap<String, EligibleSemanticRevision>,
    ) -> Result<SemanticRetrievalStatus> {
        if !self.manifest_path().is_file() {
            return Ok(SemanticRetrievalStatus::disabled(
                current_commit_seq,
                "semantic model is not installed",
            ));
        }
        let manifest = self.verified_manifest()?;
        if !self.projection_path().is_file() {
            return Ok(SemanticRetrievalStatus::disabled(
                current_commit_seq,
                "semantic projection is not built",
            ));
        }
        let connection = Connection::open_with_flags(
            self.projection_path(),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let indexed_commit_seq = meta_i64(&connection, "indexed_commit_seq")?;
        let projection_model_id = meta_string(&connection, "model_id")?;
        let projection_model_revision = meta_string(&connection, "model_revision")?;
        if projection_model_id != manifest.model_id
            || projection_model_revision != manifest.model_revision
        {
            return Ok(SemanticRetrievalStatus {
                state: "stale".into(),
                policy_version: SEMANTIC_POLICY_VERSION.into(),
                model_id: Some(manifest.model_id),
                model_revision: Some(manifest.model_revision),
                indexed_commit_seq,
                current_commit_seq,
                eligible_revision_count: eligible.len(),
                indexed_revision_count: 0,
                coverage: 0.0,
                reason: Some(
                    "semantic projection was built by a different model revision; run `memoree semantic rebuild`"
                        .into(),
                ),
            });
        }
        let revision_json = serde_json::to_string(&eligible.keys().collect::<Vec<_>>())?;
        let mut statement = connection.prepare(
            "SELECT entity_type, entity_id, revision_id, revision_hash
               FROM vectors
              WHERE revision_id IN (SELECT value FROM json_each(?1))
              GROUP BY entity_type, entity_id, revision_id, revision_hash",
        )?;
        let rows = statement.query_map([revision_json], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        let mut indexed_revisions = BTreeSet::new();
        for row in rows {
            let (entity_type, entity_id, revision_id, revision_hash) = row?;
            let Some(expected) = eligible.get(&revision_id) else {
                continue;
            };
            if entity_type_name(expected.entity_type) == entity_type
                && expected.entity_id == entity_id
                && expected.revision_hash == revision_hash
            {
                indexed_revisions.insert(revision_id);
            }
        }
        let indexed_revision_count = indexed_revisions.len();
        let coverage = if eligible.is_empty() {
            1.0
        } else {
            indexed_revision_count as f64 / eligible.len() as f64
        };
        let ready = indexed_commit_seq >= current_commit_seq && coverage == 1.0;
        Ok(SemanticRetrievalStatus {
            state: if ready {
                "ready".into()
            } else {
                "stale".into()
            },
            policy_version: SEMANTIC_POLICY_VERSION.into(),
            model_id: Some(manifest.model_id),
            model_revision: Some(manifest.model_revision),
            indexed_commit_seq,
            current_commit_seq,
            eligible_revision_count: eligible.len(),
            indexed_revision_count,
            coverage,
            reason: (!ready).then(|| {
                format!(
                    "semantic projection covers {indexed_revision_count}/{} eligible revisions through commit {indexed_commit_seq}; run `memoree semantic rebuild`",
                    eligible.len()
                )
            }),
        })
    }

    fn load_manifest(&self) -> Result<ModelManifest> {
        let manifest: ModelManifest = serde_json::from_slice(&fs::read(self.manifest_path())?)?;
        validate_manifest_shape(&manifest)?;
        for (name, expected_hash) in &manifest.files {
            let bytes = fs::read(self.model_dir().join(name))?;
            let actual = blake3::hash(&bytes).to_hex().to_string();
            if &actual != expected_hash {
                return Err(MemoryError::Integrity(format!(
                    "semantic model file {name} failed digest verification"
                )));
            }
        }
        Ok(manifest)
    }

    fn verified_manifest(&self) -> Result<ModelManifest> {
        let mut cached = self.verified_manifest.lock();
        if let Some(manifest) = cached.as_ref() {
            return Ok(manifest.clone());
        }
        let manifest = self.load_manifest()?;
        *cached = Some(manifest.clone());
        Ok(manifest)
    }

    fn load_model(&self, manifest: &ModelManifest) -> Result<CandleEmbedding> {
        // Manifest verification happens before reading model bytes. This path
        // has no hub client and therefore cannot perform network I/O.
        for (name, expected_hash) in &manifest.files {
            let bytes = fs::read(self.model_dir().join(name))?;
            if blake3::hash(&bytes).to_hex().as_str() != expected_hash {
                return Err(MemoryError::Integrity(format!(
                    "semantic model file {name} changed during load"
                )));
            }
        }
        CandleEmbedding::load(
            &self.model_dir(),
            &self.model_dir().join("model.safetensors"),
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ModelManifest {
    schema_version: u32,
    pub model_id: String,
    pub model_revision: String,
    dimensions: usize,
    query_prefix: String,
    files: BTreeMap<String, String>,
}

fn validate_manifest_shape(manifest: &ModelManifest) -> Result<()> {
    let expected_files = MODEL_FILES
        .iter()
        .map(|(name, _)| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    let actual_files = manifest.files.keys().cloned().collect::<BTreeSet<_>>();
    if manifest.schema_version != MODEL_MANIFEST_SCHEMA
        || manifest.model_id != SEMANTIC_MODEL_ID
        || manifest.model_revision != SEMANTIC_MODEL_REVISION
        || manifest.dimensions != SEMANTIC_DIMENSIONS
        || manifest.query_prefix != SEMANTIC_QUERY_PREFIX
        || actual_files != expected_files
    {
        return Err(MemoryError::Integrity(
            "semantic model manifest is incompatible".into(),
        ));
    }
    Ok(())
}

fn entity_type_name(entity_type: EntityType) -> &'static str {
    match entity_type {
        EntityType::Artifact => "artifact",
        EntityType::Claim => "claim",
    }
}

fn encode_embedding(embedding: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(embedding.len() * 4);
    for value in embedding {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

fn decode_embedding(bytes: &[u8]) -> Result<Vec<f32>> {
    if bytes.len() != SEMANTIC_DIMENSIONS * 4 {
        return Err(MemoryError::Integrity(format!(
            "semantic vector has {} bytes, expected {}",
            bytes.len(),
            SEMANTIC_DIMENSIONS * 4
        )));
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().expect("four-byte chunk")))
        .collect())
}

fn cosine(left: &[f32], right: &[f32]) -> Result<f64> {
    if left.len() != right.len() || left.len() != SEMANTIC_DIMENSIONS {
        return Err(MemoryError::Integrity(
            "semantic cosine received incompatible vectors".into(),
        ));
    }
    let dot = left
        .iter()
        .zip(right)
        .map(|(left, right)| f64::from(*left) * f64::from(*right))
        .sum::<f64>();
    let left_norm = left
        .iter()
        .map(|value| f64::from(*value).powi(2))
        .sum::<f64>()
        .sqrt();
    let right_norm = right
        .iter()
        .map(|value| f64::from(*value).powi(2))
        .sum::<f64>()
        .sqrt();
    if left_norm <= f64::EPSILON || right_norm <= f64::EPSILON {
        return Err(MemoryError::Integrity(
            "semantic cosine received a zero-length vector".into(),
        ));
    }
    Ok((dot / (left_norm * right_norm)).clamp(-1.0, 1.0))
}

fn normalize_embedding(embedding: &mut [f32]) -> Result<()> {
    let norm = embedding
        .iter()
        .map(|value| f64::from(*value).powi(2))
        .sum::<f64>()
        .sqrt();
    if !norm.is_finite() || norm <= f64::EPSILON {
        return Err(MemoryError::Integrity(
            "semantic model returned a non-normalizable embedding".into(),
        ));
    }
    for value in embedding {
        *value = (f64::from(*value) / norm) as f32;
    }
    Ok(())
}

fn meta_i64(connection: &Connection, key: &str) -> Result<i64> {
    let value = meta_string(connection, key)?;
    value.parse().map_err(|error| {
        MemoryError::Integrity(format!("semantic metadata {key} is invalid: {error}"))
    })
}

fn meta_string(connection: &Connection, key: &str) -> Result<String> {
    Ok(
        connection.query_row("SELECT value FROM meta WHERE key = ?1", [key], |row| {
            row.get(0)
        })?,
    )
}

fn semantic_error(error: impl std::fmt::Display) -> MemoryError {
    MemoryError::Config(format!("semantic model error: {error}"))
}

fn candle_error(error: impl std::fmt::Display) -> MemoryError {
    MemoryError::Config(format!("Candle semantic model error: {error}"))
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn set_private_file(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastembed::{
        InitOptionsUserDefined, Pooling, RerankInitOptionsUserDefined, TextEmbedding, TextRerank,
        TokenizerFiles, UserDefinedEmbeddingModel, UserDefinedRerankingModel,
    };

    #[test]
    fn reranker_breaker_trips_only_after_three_consecutive_slow_inferences() {
        let mut breaker = RerankerBreaker::default();
        assert_eq!(breaker.permit(), RerankerPermit::Score);
        breaker.record(RerankerPermit::Score, 501.0);
        assert_eq!(breaker.public().consecutive_over_budget, 1);
        breaker.record(RerankerPermit::Score, 499.0);
        assert_eq!(breaker.public().consecutive_over_budget, 0);
        breaker.record(RerankerPermit::Score, 501.0);
        breaker.record(RerankerPermit::Score, 502.0);
        assert_eq!(breaker.public().state, "closed");
        breaker.record(RerankerPermit::Score, 503.0);
        assert_eq!(breaker.public().state, "open");
        assert_eq!(breaker.public().consecutive_over_budget, 3);
    }

    #[test]
    fn reranker_breaker_probes_after_exactly_thirty_two_skips() {
        let mut breaker = RerankerBreaker::default();
        for _ in 0..RERANKER_BREAKER_TRIP_THRESHOLD {
            breaker.record(RerankerPermit::Score, 501.0);
        }
        for expected in 1..=RERANKER_BREAKER_PROBE_AFTER_SKIPS {
            assert_eq!(breaker.permit(), RerankerPermit::Skip);
            assert_eq!(breaker.public().skipped_since_open, expected);
        }
        assert_eq!(breaker.permit(), RerankerPermit::Probe);
        assert_eq!(breaker.public().state, "half_open");
        breaker.record(RerankerPermit::Probe, 499.0);
        assert_eq!(breaker.public().state, "closed");
        assert_eq!(breaker.public().consecutive_over_budget, 0);
    }

    #[test]
    fn reranker_status_telemetry_has_a_closed_numeric_allowlist() {
        let status = RerankerRetrievalStatus::disabled(7, "test");
        let value = serde_json::to_value(status).unwrap();
        let keys = value
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        assert_eq!(
            keys,
            BTreeSet::from([
                "breaker".into(),
                "candidate_count".into(),
                "candidate_limit".into(),
                "candidate_limit_reached".into(),
                "ordering_applied".into(),
                "policy_version".into(),
                "reason".into(),
                "role".into(),
                "scored_candidate_count".into(),
                "state".into(),
                "surface".into(),
            ])
        );
        let encoded = value.to_string();
        for forbidden in ["query", "passage", "excerpt", "statement", "citation"] {
            assert!(!encoded.contains(forbidden));
        }
    }

    #[test]
    fn v1_reranker_install_manifest_loads_without_rewrite() {
        let temporary = tempfile::tempdir().unwrap();
        // Construct before publishing the fixture so startup warm-up is not
        // involved; this test isolates read-path manifest compatibility.
        let manager = RerankerManager::new(temporary.path());
        fs::create_dir_all(manager.model_dir()).unwrap();
        let mut files = BTreeMap::new();
        for (name, _) in RERANKER_FILES {
            let bytes = format!("fixture-{name}").into_bytes();
            fs::write(manager.model_dir().join(name), &bytes).unwrap();
            files.insert(
                (*name).to_owned(),
                blake3::hash(&bytes).to_hex().to_string(),
            );
        }
        let mut manifest = RerankerManifest::new(files);
        manifest.policy_version = "cross_encoder_ordering_v1".into();
        assert!(validate_compatible_reranker_manifest_shape(&manifest).is_ok());
        assert!(validate_current_reranker_manifest_shape(&manifest).is_err());
        let frozen = serde_json::to_vec_pretty(&manifest).unwrap();
        fs::write(manager.manifest_path(), &frozen).unwrap();

        manager.verified_manifest().unwrap();
        let status = manager.status().unwrap();

        assert_eq!(status.state, "ready");
        assert_eq!(status.policy_version, "cross_encoder_ordering_v2");
        assert_eq!(fs::read(manager.manifest_path()).unwrap(), frozen);
    }

    #[test]
    fn unknown_reranker_install_policy_is_rejected() {
        let files = RERANKER_FILES
            .iter()
            .map(|(name, _)| ((*name).to_owned(), "fixture-digest".into()))
            .collect();
        let mut manifest = RerankerManifest::new(files);
        manifest.policy_version = "cross_encoder_ordering_v3".into();
        assert!(validate_compatible_reranker_manifest_shape(&manifest).is_err());
        assert!(validate_current_reranker_manifest_shape(&manifest).is_err());
    }

    #[test]
    fn embedding_binary_encoding_round_trips_exactly() {
        let values = (0..SEMANTIC_DIMENSIONS)
            .map(|index| (index as f32 - 100.0) / 37.0)
            .collect::<Vec<_>>();
        assert_eq!(
            decode_embedding(&encode_embedding(&values)).unwrap(),
            values
        );
    }

    #[test]
    fn explicit_normalization_is_idempotent_and_cosine_is_bounded() {
        let mut values = vec![0.0; SEMANTIC_DIMENSIONS];
        values[0] = 3.0;
        values[1] = 4.0;
        normalize_embedding(&mut values).unwrap();
        let norm = values
            .iter()
            .map(|value| f64::from(*value).powi(2))
            .sum::<f64>()
            .sqrt();
        assert!((norm - 1.0).abs() < 1e-6);
        let once = values.clone();
        normalize_embedding(&mut values).unwrap();
        for (left, right) in once.iter().zip(&values) {
            assert!((left - right).abs() < 1e-6);
        }
        assert!((cosine(&values, &values).unwrap() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn zero_embedding_is_rejected() {
        let mut values = vec![0.0; SEMANTIC_DIMENSIONS];
        assert!(normalize_embedding(&mut values).is_err());
        assert!(cosine(&values, &values).is_err());
    }

    #[test]
    fn status_reports_only_hash_matching_eligible_revision_coverage() {
        let temporary = tempfile::tempdir().unwrap();
        let manager = SemanticManager::new(temporary.path());
        ensure_private_directory(&manager.model_dir()).unwrap();
        let mut files = BTreeMap::new();
        for (name, _) in MODEL_FILES {
            let bytes = format!("test-{name}").into_bytes();
            fs::write(manager.model_dir().join(name), &bytes).unwrap();
            files.insert(
                (*name).to_owned(),
                blake3::hash(&bytes).to_hex().to_string(),
            );
        }
        let manifest = ModelManifest {
            schema_version: MODEL_MANIFEST_SCHEMA,
            model_id: SEMANTIC_MODEL_ID.into(),
            model_revision: SEMANTIC_MODEL_REVISION.into(),
            dimensions: SEMANTIC_DIMENSIONS,
            query_prefix: SEMANTIC_QUERY_PREFIX.into(),
            files,
        };
        fs::write(
            manager.manifest_path(),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        let projection = Connection::open(manager.projection_path()).unwrap();
        projection.execute_batch(PROJECTION_SQL).unwrap();
        for (key, value) in [
            ("model_id", SEMANTIC_MODEL_ID),
            ("model_revision", SEMANTIC_MODEL_REVISION),
            ("indexed_commit_seq", "7"),
        ] {
            projection
                .execute(
                    "UPDATE meta SET value = ?2 WHERE key = ?1",
                    params![key, value],
                )
                .unwrap();
        }
        projection
            .execute(
                "INSERT INTO vectors(
                     entity_type, entity_id, revision_id, ordinal,
                     revision_hash, content_hash, embedding, commit_seq
                 ) VALUES ('claim', 'claim-1', 'revision-1', 0,
                           'hash-1', 'content-1', ?1, 7)",
                [encode_embedding(&vec![0.0; SEMANTIC_DIMENSIONS])],
            )
            .unwrap();
        let mut eligible = BTreeMap::from([(
            "revision-1".into(),
            EligibleSemanticRevision {
                entity_type: EntityType::Claim,
                entity_id: "claim-1".into(),
                revision_hash: "hash-1".into(),
            },
        )]);
        let ready = manager.status(7, &eligible).unwrap();
        assert_eq!(ready.state, "ready");
        assert_eq!(ready.indexed_revision_count, 1);
        assert_eq!(ready.coverage, 1.0);

        eligible.get_mut("revision-1").unwrap().revision_hash = "changed".into();
        let stale = manager.status(8, &eligible).unwrap();
        assert_eq!(stale.state, "stale");
        assert_eq!(stale.indexed_revision_count, 0);
        assert_eq!(stale.coverage, 0.0);
        assert!(stale.reason.unwrap().contains("semantic rebuild"));
    }

    #[test]
    #[ignore = "requires explicit ONNX and safetensors parity assets"]
    fn candle_embeddings_match_the_frozen_onnx_reference() {
        let data_directory = std::env::var_os("MEMOREE_PARITY_DATA_DIR")
            .map(PathBuf::from)
            .expect("set MEMOREE_PARITY_DATA_DIR");
        let safetensors_path = std::env::var_os("MEMOREE_PARITY_SAFETENSORS")
            .map(PathBuf::from)
            .expect("set MEMOREE_PARITY_SAFETENSORS");
        let model_directory = data_directory.join("semantic/model");
        let tokenizer_files = TokenizerFiles {
            tokenizer_file: fs::read(model_directory.join("tokenizer.json")).unwrap(),
            config_file: fs::read(model_directory.join("config.json")).unwrap(),
            special_tokens_map_file: fs::read(model_directory.join("special_tokens_map.json"))
                .unwrap(),
            tokenizer_config_file: fs::read(model_directory.join("tokenizer_config.json")).unwrap(),
        };
        let onnx_model = UserDefinedEmbeddingModel::new(
            fs::read(model_directory.join("model.onnx")).unwrap(),
            tokenizer_files,
        )
        .with_pooling(Pooling::Cls);
        let mut onnx = TextEmbedding::try_new_from_user_defined(
            onnx_model,
            InitOptionsUserDefined::new()
                .with_max_length(512)
                .with_intra_threads(4),
        )
        .unwrap();
        let mut candle = CandleEmbedding::load(&model_directory, &safetensors_path).unwrap();
        let probes = [
            "Represent this sentence for searching relevant passages: where is the solver allowed to write state?",
            "The solver worker may write only to the optimizer store; UI state remains owned by the interface layer.",
            "Represent this sentence for searching relevant passages: what happens when verification remains open?",
            "Keep the release blocked while the verification item is unresolved.",
            "Represent this sentence for searching relevant passages: how large may the payload become?",
            "The serialized request must remain below the eight megabyte resource boundary.",
            "Represent this sentence for searching relevant passages: who acknowledges the incident?",
            "The on-call incident commander owns the acknowledgement step.",
            "Represent this sentence for searching relevant passages: unrelated lunch recommendation",
            "Use a bounded context packet with exact source citations and explicit conflicts.",
            "UTF-8 parity probe: Muscat, 東京, and an emoji 🧭 remain stable.",
            "APP-BOUNDARY-101",
        ];
        let onnx_embeddings = onnx.embed(probes, Some(64)).unwrap();
        let candle_embeddings = candle.embed(&probes).unwrap();
        assert_eq!(onnx_embeddings.len(), candle_embeddings.len());
        for (index, (onnx, candle)) in onnx_embeddings.iter().zip(&candle_embeddings).enumerate() {
            let similarity = cosine(onnx, candle).unwrap();
            assert!(
                similarity >= 0.9995,
                "probe {index} Candle/ONNX cosine was {similarity}"
            );
        }
    }

    #[test]
    #[ignore = "requires explicit ONNX and safetensors reranker parity assets"]
    fn candle_reranker_logits_match_the_frozen_onnx_reference() {
        let model_directory = std::env::var_os("MEMOREE_RERANKER_MODEL_DIR")
            .map(PathBuf::from)
            .expect("set MEMOREE_RERANKER_MODEL_DIR");
        let onnx_path = std::env::var_os("MEMOREE_RERANKER_ONNX")
            .map(PathBuf::from)
            .expect("set MEMOREE_RERANKER_ONNX");
        let tokenizer_files = TokenizerFiles {
            tokenizer_file: fs::read(model_directory.join("tokenizer.json")).unwrap(),
            config_file: fs::read(model_directory.join("config.json")).unwrap(),
            special_tokens_map_file: fs::read(model_directory.join("special_tokens_map.json"))
                .unwrap(),
            tokenizer_config_file: fs::read(model_directory.join("tokenizer_config.json")).unwrap(),
        };
        let model = UserDefinedRerankingModel::new(onnx_path, tokenizer_files);
        let mut onnx = TextRerank::try_new_from_user_defined(
            model,
            RerankInitOptionsUserDefined::new()
                .with_max_length(RERANKER_MAX_SEQUENCE_TOKENS)
                .with_intra_threads(4),
        )
        .unwrap();
        let mut candle = CandleReranker::load(&model_directory).unwrap();
        let probes = [
            (
                "Can the interface own a second solver implementation?",
                "The Rust and WebAssembly engine is authoritative; a solver fork in the interface language is prohibited.",
            ),
            (
                "Can the interface own a second solver implementation?",
                "The interface owns panels, routing, themes, and accessibility around the retained canvas.",
            ),
            (
                "Are browser verification gates closed?",
                "Authenticated browser E2E, visual fidelity, lifecycle soak, and measured performance remain open.",
            ),
            (
                "Are browser verification gates closed?",
                "Native unit tests and architecture enforcement passed.",
            ),
            (
                "Who issues the terminal acknowledgement?",
                "The engine session alone originates the exact terminal acknowledgement; adapters only observe it.",
            ),
            (
                "Who issues the terminal acknowledgement?",
                "The adapter issues the terminal acknowledgement after the engine stops.",
            ),
            (
                "Does raw-size success approve the renderer?",
                "Passing the raw guard is not acceptance because the independent gzip target still applies.",
            ),
            (
                "Does raw-size success approve the renderer?",
                "The renderer passed its raw module byte ceiling.",
            ),
            (
                "UTF-8 owner probe 🧭 東京",
                "The session in Muscat owns the final acknowledgement 🧭.",
            ),
            (
                "APP-BOUNDARY-101",
                "APP-BOUNDARY-101 prohibits a UI-language solver fork.",
            ),
        ];
        for (index, (query, passage)) in probes.iter().enumerate() {
            let onnx_score = onnx.rerank(*query, [*passage], false, Some(1)).unwrap()[0].score;
            let candle_score = candle.score(query, &[*passage]).unwrap()[0];
            let delta = (onnx_score - candle_score).abs();
            assert!(
                delta < 1e-3,
                "probe {index} Candle/ONNX logit delta was {delta}: {candle_score} vs {onnx_score}"
            );
        }
    }

    #[test]
    #[ignore = "local CPU microbenchmark; requires an explicit reranker model directory"]
    fn candle_reranker_reports_warm_latency() {
        use std::time::Instant;

        let model_directory = std::env::var_os("MEMOREE_RERANKER_MODEL_DIR")
            .map(PathBuf::from)
            .expect("set MEMOREE_RERANKER_MODEL_DIR");
        let model_id =
            std::env::var("MEMOREE_RERANKER_MODEL_ID").unwrap_or_else(|_| RERANKER_MODEL_ID.into());
        let load_started = Instant::now();
        let mut model = CandleReranker::load(&model_directory).unwrap();
        let cold_load_ms = load_started.elapsed().as_secs_f64() * 1000.0;
        let query = "Which subsystem is allowed to originate the final terminal acknowledgement?";
        let passages = (0..16)
            .map(|index| {
                format!(
                    "Candidate {index}: the engine session owns terminal acknowledgement while adapters observe it. {}",
                    "Bounded deterministic context remains exact. ".repeat(index % 7)
                )
            })
            .collect::<Vec<_>>();
        let refs = passages.iter().map(String::as_str).collect::<Vec<_>>();
        for batch_size in [1usize, 2, 4, 8, 16] {
            for _ in 0..2 {
                model
                    .score_with_batch_size(query, &refs, batch_size)
                    .unwrap();
            }
            let mut samples_ms = Vec::new();
            for _ in 0..12 {
                let started = Instant::now();
                let scores = model
                    .score_with_batch_size(query, &refs, batch_size)
                    .unwrap();
                assert_eq!(scores.len(), refs.len());
                samples_ms.push(started.elapsed().as_secs_f64() * 1000.0);
            }
            samples_ms.sort_by(f64::total_cmp);
            let percentile = |numerator: usize, denominator: usize| {
                let index = ((samples_ms.len() - 1) * numerator).div_ceil(denominator);
                samples_ms[index]
            };
            let warm_p95_ms = percentile(95, 100);
            println!(
                "{}",
                serde_json::json!({
                    "model": model_id,
                    "backend": "candle_f32_cpu",
                    "pairs": refs.len(),
                    "max_sequence_tokens": RERANKER_MAX_SEQUENCE_TOKENS,
                    "batch_size": batch_size,
                    "cold_load_ms": cold_load_ms,
                    "warm_p50_ms": percentile(50, 100),
                    "warm_p95_ms": warm_p95_ms,
                    "warm_max_ms": *samples_ms.last().unwrap(),
                    "ordering_p95_budget_ms": RERANKER_ORDERING_P95_BUDGET_MS,
                    "within_ordering_budget": batch_size != 16
                        || warm_p95_ms <= RERANKER_ORDERING_P95_BUDGET_MS,
                })
            );
            if batch_size == 16 {
                assert!(
                    warm_p95_ms <= RERANKER_ORDERING_P95_BUDGET_MS,
                    "batch-16 p95 {warm_p95_ms:.3}ms exceeds the pre-registered ordering budget of {RERANKER_ORDERING_P95_BUDGET_MS:.3}ms"
                );
            }
        }
    }

    #[test]
    #[ignore = "requires an explicit installed semantic model directory"]
    fn incremental_rebuild_reuses_byte_identical_vectors() {
        let source = std::env::var_os("MEMOREE_SEMANTIC_MODEL_DIR")
            .map(PathBuf::from)
            .expect("set MEMOREE_SEMANTIC_MODEL_DIR");
        let temporary = tempfile::tempdir().unwrap();
        let manager = SemanticManager::new(temporary.path());
        manager.install_model_from_directory(&source).unwrap();
        let document =
            |revision: &str, ordinal: usize, text: &str, commit_seq: i64| SemanticDocument {
                entity_type: EntityType::Claim,
                entity_id: format!("claim-{revision}"),
                revision_id: revision.into(),
                ordinal,
                start_byte: None,
                end_byte: None,
                revision_hash: format!("hash-{revision}"),
                text: text.into(),
                commit_seq,
            };
        let first_documents = vec![
            document("one", 0, "The session owns terminal acknowledgement.", 1),
            document("two", 0, "The canvas owns domain geometry.", 2),
        ];
        let (_, first) = manager.rebuild(&first_documents, 2).unwrap();
        assert_eq!(first.embedded_vector_count, 2);
        assert_eq!(first.reused_vector_count, 0);
        let snapshot = |manager: &SemanticManager| {
            let connection = Connection::open(manager.projection_path()).unwrap();
            let mut statement = connection
                .prepare(
                    "SELECT revision_id, ordinal, embedding
                       FROM vectors ORDER BY revision_id, ordinal",
                )
                .unwrap();
            statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                    ))
                })
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        let frozen = snapshot(&manager);

        let (_, identical) = manager.rebuild(&first_documents, 2).unwrap();
        assert_eq!(identical.reused_vector_count, 2);
        assert_eq!(identical.embedded_vector_count, 0);
        assert_eq!(snapshot(&manager), frozen);

        let mut extended = first_documents.clone();
        extended.push(document(
            "three",
            0,
            "The preload never initializes a worker.",
            3,
        ));
        let (_, added) = manager.rebuild(&extended, 3).unwrap();
        assert_eq!(added.reused_vector_count, 2);
        assert_eq!(added.embedded_vector_count, 1);
        let extended_snapshot = snapshot(&manager);
        for unchanged in &frozen {
            assert!(extended_snapshot.contains(unchanged));
        }

        let (_, removed) = manager.rebuild(&first_documents, 4).unwrap();
        assert_eq!(removed.reused_vector_count, 2);
        assert_eq!(removed.embedded_vector_count, 0);
        assert_eq!(removed.deleted_vector_count, 1);
        assert_eq!(snapshot(&manager), frozen);
    }
}
