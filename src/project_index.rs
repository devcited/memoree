//! Disposable, project-local source index.
//!
//! This projection is intentionally separate from durable Memoree artifacts
//! and claims. Git remains authoritative; exact citations are hash-verified
//! against the working tree before bytes are returned.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::Read,
    path::{Component, Path, PathBuf},
    process::Command,
    thread,
    time::{Duration, Instant},
};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Utc};
use fs2::FileExt;
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;

use crate::{
    context::{
        AutoReindexMode, LocalProjectSettings, MARKER_FILE, Marker, ProjectIndexConfig,
        find_marker, load_local_project_settings, read_marker, update_local_project_settings,
    },
    error::{MemoryError, Result},
    project_structure::{
        STRUCTURAL_GRAMMAR_REVISION, STRUCTURAL_POLICY_VERSION, StructuralConfidence,
        StructuralEdgeKind, parse_file,
    },
    protocol::ErrorCode,
};

const PROJECT_INDEX_SCHEMA: i64 = 3;
const CHUNK_BYTES: usize = 2 * 1024;
const CHUNK_OVERLAP_BYTES: usize = 128;
const MAX_PROJECT_SEARCH_RESULTS: usize = 50;
const MAX_PROJECT_GET_BYTES: usize = 16 * 1024;
pub const MAX_PROJECT_MAP_BYTES: usize = 12 * 1024;
pub const DEFAULT_PROJECT_MAP_BYTES: usize = 8 * 1024;
const MAX_PROJECT_MAP_LEADS: usize = 8;
const MAX_PROJECT_MAP_RELATION_LEADS: usize = 2;
const MAX_PROJECT_MAP_ALTERNATIVES: usize = 3;
const MAX_PROJECT_MAP_CANDIDATES: usize = 128;
const MAX_PROJECT_MAP_EDGE_CANDIDATES: usize = 512;
const MAX_PROJECT_MAP_EDGES_PER_LEAD: usize = 8;
const MAX_PROJECT_MAP_TEST_TRAVERSAL_NODES: usize = 256;
const MAX_PROJECT_MAP_TEST_CALLERS_PER_NODE: usize = 16;
const MAX_PROJECT_MAP_NON_TEST_CALLERS_PER_NODE: usize = 8;
const MAX_PROJECT_MAP_TEST_HOPS: usize = 3;
const MAX_PROJECT_MAP_EXCERPT_BYTES: usize = 768;
const MAX_PROJECT_MAP_RELATION_EXCERPT_BYTES: usize = 256;
const MAX_PROJECT_MAP_REFERENCE_EXCERPT_BYTES: usize = 640;
const MAX_PROJECT_MAP_MENTION_EXCERPT_BYTES: usize = 240;
const MAX_PROJECT_MAP_MENTIONS: usize = 16;
const MAX_PROJECT_MAP_MENTION_CANDIDATES: usize = 512;
const MAX_PROJECT_MAP_MENTION_LINES: usize = 32;

#[derive(Debug, Clone)]
pub struct ProjectIndex {
    root: PathBuf,
    marker: Marker,
    data_dir: PathBuf,
    index_dir: PathBuf,
    database_path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectIndexReport {
    pub schema_version: i64,
    pub project_id: String,
    pub root: String,
    pub snapshot: String,
    pub head: String,
    pub dirty: bool,
    pub indexed_files: usize,
    pub indexed_bytes: u64,
    pub changed_files: usize,
    pub changed_bytes: u64,
    pub removed_files: usize,
    pub skipped_files: usize,
    pub chunk_count: usize,
    pub structural_files: usize,
    pub fallback_files: usize,
    pub partial_parse_files: usize,
    pub parse_error_files: usize,
    pub symbol_count: usize,
    pub edge_count: usize,
    pub structural_parse_ms: f64,
    pub completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectIndexStatus {
    pub schema_version: i64,
    pub project_id: String,
    pub root: String,
    pub database: String,
    pub ready: bool,
    pub stale: bool,
    pub auto_reindex: AutoReindexMode,
    pub include_untracked: bool,
    pub indexed_snapshot: Option<String>,
    pub current_snapshot: String,
    pub head: String,
    pub dirty: bool,
    pub indexed_files: usize,
    pub indexed_bytes: u64,
    pub chunk_count: usize,
    pub structural_policy: String,
    pub structural_grammar_revision: String,
    pub structural_files: usize,
    pub fallback_files: usize,
    pub partial_parse_files: usize,
    pub parse_error_files: usize,
    pub symbol_count: usize,
    pub edge_count: usize,
    pub last_completed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectSearchHit {
    pub path: String,
    pub start_byte: usize,
    pub end_byte: usize,
    pub start_line: usize,
    pub end_line: usize,
    pub excerpt: String,
    pub citation: String,
    pub score: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectSearchReport {
    pub content_is_untrusted: bool,
    pub authority: String,
    pub project_id: String,
    pub snapshot: String,
    pub stale: bool,
    pub reindex_attempted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reindex_error: Option<String>,
    pub hits: Vec<ProjectSearchHit>,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectGetResult {
    pub content_is_untrusted: bool,
    pub authority: String,
    pub citation: String,
    pub path: String,
    pub content_hash: String,
    pub start_byte: usize,
    pub end_byte: usize,
    pub content: String,
    pub truncated: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectMapEdge {
    #[serde(skip)]
    pub reference_path_internal: String,
    #[serde(skip)]
    pub reference_start_byte: usize,
    #[serde(skip)]
    pub reference_end_byte: usize,
    pub direction: String,
    pub kind: String,
    pub confidence: String,
    pub hops: usize,
    pub via: Vec<ProjectMapVia>,
    pub name: String,
    pub qualified_name: String,
    pub path: String,
    pub related_is_test: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub test_surface: Option<String>,
    pub symbol_start_line: usize,
    pub symbol_end_line: usize,
    pub reference_start_line: usize,
    pub reference_end_line: usize,
    pub citation: String,
    pub excerpt: String,
    pub excerpt_truncated: bool,
    pub site_index: usize,
    pub site_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectMapFacetSummary {
    pub state: String,
    pub returned: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectMapFacets {
    pub definition: ProjectMapFacetSummary,
    pub direct_callers: ProjectMapFacetSummary,
    pub direct_callees: ProjectMapFacetSummary,
    pub direct_tests: ProjectMapFacetSummary,
    pub behavioral_test_leads: ProjectMapFacetSummary,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectMapVia {
    pub name: String,
    pub path: String,
    pub start_line: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectMapMention {
    pub classification: String,
    pub anchor_name: String,
    pub path: String,
    pub context: String,
    pub occurrence_count: usize,
    pub occurrence_lines: Vec<usize>,
    pub start_line: usize,
    pub end_line: usize,
    pub citation: String,
    pub excerpt: String,
    pub excerpt_truncated: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectMapCoverage {
    pub indexed_files: usize,
    pub include_untracked: bool,
    pub untracked_excluded: usize,
    pub excluded_paths: usize,
    pub non_allowlisted: usize,
    pub non_allowlisted_extensions: BTreeMap<String, usize>,
    pub oversize_skipped: usize,
    pub non_utf8_skipped: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectMapAlternative {
    pub name: String,
    pub kind: String,
    pub path: String,
    pub start_line: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectMapLead {
    #[serde(skip)]
    pub symbol_key: String,
    pub name: String,
    pub kind: String,
    pub qualified_name: String,
    pub path: String,
    #[serde(skip)]
    pub start_byte: usize,
    #[serde(skip)]
    pub end_byte: usize,
    pub start_line: usize,
    pub end_line: usize,
    pub citation: String,
    pub excerpt: String,
    pub excerpt_truncated: bool,
    #[serde(skip)]
    pub score: f64,
    pub facets: ProjectMapFacets,
    pub edges: Vec<ProjectMapEdge>,
    pub edges_truncated: bool,
    pub test_leads_truncated: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectMapReport {
    pub content_is_untrusted: bool,
    pub authority: String,
    pub project_id: String,
    pub snapshot: String,
    pub stale: bool,
    pub reindex_attempted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reindex_error: Option<String>,
    pub structural_state: String,
    pub query_mode: String,
    pub presence: String,
    pub absence: String,
    pub leads: Vec<ProjectMapLead>,
    pub alternatives: Vec<ProjectMapAlternative>,
    pub lexical_anchors: Vec<String>,
    pub lexical_residue: Vec<ProjectMapMention>,
    pub mentions_truncated: bool,
    pub coverage: ProjectMapCoverage,
    pub limits: Vec<String>,
    pub text_fallback: Vec<ProjectSearchHit>,
    pub truncated: bool,
    pub max_bytes: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectWatchReport {
    pub project_id: String,
    pub polls: usize,
    pub reindexes: usize,
    pub failed_reindexes: usize,
    pub failed_snapshot_reads: usize,
    pub last_snapshot: String,
}

#[derive(Debug, Clone)]
pub enum ProjectWatchObservation {
    Reindexed {
        report: Box<ProjectIndexReport>,
        duration_ms: f64,
    },
    ReindexFailed {
        error_code: ErrorCode,
        duration_ms: f64,
    },
    SnapshotFailed {
        error_code: ErrorCode,
        duration_ms: f64,
    },
}

#[derive(Debug, Clone)]
struct IndexedFile {
    path: String,
    hash: String,
    bytes: Vec<u8>,
}

type VerifiedReferenceExcerpt = (String, String, bool, usize, usize);

#[derive(Debug, Clone)]
struct IndexedSymbol {
    symbol_key: String,
    path: String,
    content_hash: String,
    structural_state: String,
    name: String,
    kind: String,
    qualified_name: String,
    start_byte: usize,
    end_byte: usize,
    start_line: usize,
    end_line: usize,
    score: f64,
    exact_name_match: bool,
}

#[derive(Debug, Clone)]
struct IndexedMapEdge {
    direction: String,
    kind: String,
    confidence: String,
    hops: usize,
    via: Vec<ProjectMapVia>,
    related: IndexedSymbol,
    related_is_test: bool,
    reference_path: String,
    reference_hash: String,
    reference_start: usize,
    reference_end: usize,
}

#[derive(Debug, Clone)]
struct ProjectMapFacetCompleteness {
    direct_callers: bool,
    direct_callees: bool,
    direct_tests: bool,
    behavioral_test_leads: bool,
}

impl Default for ProjectMapFacetCompleteness {
    fn default() -> Self {
        Self {
            direct_callers: true,
            direct_callees: true,
            direct_tests: true,
            behavioral_test_leads: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TraversalCallForm {
    Free,
    Static { qualifier: String },
    Receiver { receiver: String },
}

impl ProjectIndex {
    pub fn discover(cwd: &Path, data_dir: &Path) -> Result<Self> {
        let marker_path = find_marker(cwd)?.ok_or_else(|| {
            MemoryError::Config(format!(
                "no {MARKER_FILE} was found from {}; initialize project memory first",
                cwd.display()
            ))
        })?;
        let mut marker = read_marker(&marker_path)?;
        marker.project_index = load_local_project_settings(data_dir, &marker.project_id)?
            .map(|settings| settings.project_index)
            .unwrap_or_default();
        let root = marker_path
            .parent()
            .ok_or_else(|| MemoryError::Config("project marker has no parent directory".into()))?
            .to_path_buf();
        let project_key = blake3::hash(marker.project_id.as_bytes())
            .to_hex()
            .to_string();
        let index_dir = data_dir.join("project-index").join(&project_key[..32]);
        let database_path = index_dir.join("index.sqlite3");
        Ok(Self {
            root,
            marker,
            data_dir: data_dir.to_path_buf(),
            index_dir,
            database_path,
        })
    }

    pub fn config(&self) -> &ProjectIndexConfig {
        &self.marker.project_index
    }

    pub fn configure(
        &mut self,
        auto_reindex: AutoReindexMode,
        include_untracked: Option<bool>,
    ) -> Result<ProjectIndexConfig> {
        let initial = LocalProjectSettings::from_marker(&self.marker);
        let settings = update_local_project_settings(
            &self.data_dir,
            &self.marker.project_id,
            &initial,
            |settings| {
                settings.project_index.auto_reindex = auto_reindex;
                if let Some(include_untracked) = include_untracked {
                    settings.project_index.include_untracked = include_untracked;
                }
                Ok(())
            },
        )?;
        self.marker.project_index = settings.project_index;
        Ok(self.marker.project_index.clone())
    }

    pub fn index(&self) -> Result<ProjectIndexReport> {
        create_private_directory(&self.data_dir)?;
        create_private_directory(&self.index_dir)?;
        let lock_path = self.index_dir.join("index.lock");
        let lock = private_lock_file(&lock_path)?;
        lock.try_lock_exclusive().map_err(|error| {
            MemoryError::InvalidRequest(format!(
                "another project index operation holds {}: {error}",
                lock_path.display()
            ))
        })?;

        let (head, snapshot, dirty) = self.git_snapshot()?;
        let candidates = self.list_git_files()?;
        let untracked_excluded = if self.marker.project_index.include_untracked {
            0
        } else {
            self.list_untracked_files()?.len()
        };
        if candidates.len() > self.marker.project_index.max_files {
            return Err(MemoryError::InvalidRequest(format!(
                "project index budget exceeded: {} files is greater than max_files {}",
                candidates.len(),
                self.marker.project_index.max_files
            )));
        }
        let ignores = read_memoree_ignore(&self.root)?;
        let mut skipped_files = 0usize;
        let mut excluded_paths = 0usize;
        let mut non_allowlisted = 0usize;
        let mut non_allowlisted_extensions = BTreeMap::new();
        let mut oversize_skipped = 0usize;
        let mut non_utf8_skipped = 0usize;
        let mut indexed_bytes = 0u64;
        let mut files = Vec::new();
        for path in candidates {
            if excluded_path(&path) || ignored_by_memoree(&path, &ignores) {
                skipped_files += 1;
                excluded_paths += 1;
                continue;
            }
            if !safe_project_path(&path) || !indexable_extension(&path) {
                skipped_files += 1;
                non_allowlisted += 1;
                *non_allowlisted_extensions
                    .entry(project_path_extension(&path))
                    .or_insert(0usize) += 1;
                continue;
            }
            let absolute = self.root.join(&path);
            let metadata = match fs::symlink_metadata(&absolute) {
                Ok(metadata) if metadata.file_type().is_file() => metadata,
                _ => {
                    skipped_files += 1;
                    non_allowlisted += 1;
                    *non_allowlisted_extensions
                        .entry("<non_file>".into())
                        .or_insert(0usize) += 1;
                    continue;
                }
            };
            if metadata.len() > self.marker.project_index.max_file_bytes {
                skipped_files += 1;
                oversize_skipped += 1;
                continue;
            }
            indexed_bytes = indexed_bytes.saturating_add(metadata.len());
            if indexed_bytes > self.marker.project_index.max_total_bytes {
                return Err(MemoryError::InvalidRequest(format!(
                    "project index budget exceeded: candidate bytes exceed max_total_bytes {}",
                    self.marker.project_index.max_total_bytes
                )));
            }
            let bytes = fs::read(&absolute)?;
            if bytes.contains(&0) || std::str::from_utf8(&bytes).is_err() {
                skipped_files += 1;
                non_utf8_skipped += 1;
                indexed_bytes = indexed_bytes.saturating_sub(metadata.len());
                continue;
            }
            files.push(IndexedFile {
                path,
                hash: blake3::hash(&bytes).to_hex().to_string(),
                bytes,
            });
        }

        let mut connection = self.open_database()?;
        let previous = existing_file_hashes(&connection)?;
        let current_paths = files
            .iter()
            .map(|file| file.path.clone())
            .collect::<BTreeSet<_>>();
        let removed = previous
            .keys()
            .filter(|path| !current_paths.contains(*path))
            .cloned()
            .collect::<Vec<_>>();
        let changed = files
            .iter()
            .filter(|file| previous.get(&file.path) != Some(&file.hash))
            .collect::<Vec<_>>();
        let changed_bytes = changed
            .iter()
            .map(|file| file.bytes.len() as u64)
            .sum::<u64>();
        if !previous.is_empty() && changed_bytes > self.marker.project_index.max_changed_bytes {
            return Err(MemoryError::InvalidRequest(format!(
                "project reindex budget exceeded: {changed_bytes} changed bytes is greater than max_changed_bytes {}; the previous valid index remains active",
                self.marker.project_index.max_changed_bytes
            )));
        }

        let parsed_changed = changed
            .iter()
            .map(|file| {
                let text = std::str::from_utf8(&file.bytes)
                    .map_err(|_| MemoryError::Integrity("validated UTF-8 file changed".into()))?;
                Ok((file.path.clone(), parse_file(&file.path, text)))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let structural_parse_ms = parsed_changed
            .values()
            .map(|parsed| parsed.parse_ms)
            .sum::<f64>();

        let completed_at = Utc::now();
        let transaction = connection.transaction()?;
        for path in &removed {
            transaction.execute("DELETE FROM chunks_fts WHERE path = ?1", [path])?;
            transaction.execute("DELETE FROM project_symbols_fts WHERE path = ?1", [path])?;
            transaction.execute(
                "DELETE FROM project_references WHERE source_path = ?1",
                [path],
            )?;
            transaction.execute("DELETE FROM project_symbols WHERE path = ?1", [path])?;
            transaction.execute("DELETE FROM project_files WHERE path = ?1", [path])?;
        }
        for file in &changed {
            transaction.execute("DELETE FROM chunks_fts WHERE path = ?1", [&file.path])?;
            transaction.execute(
                "DELETE FROM project_symbols_fts WHERE path = ?1",
                [&file.path],
            )?;
            transaction.execute(
                "DELETE FROM project_references WHERE source_path = ?1",
                [&file.path],
            )?;
            transaction.execute("DELETE FROM project_symbols WHERE path = ?1", [&file.path])?;
            let parsed = parsed_changed.get(&file.path).ok_or_else(|| {
                MemoryError::Integrity("changed file has no structural parse result".into())
            })?;
            transaction.execute(
                "INSERT INTO project_files(
                    path, content_hash, byte_count, structural_language,
                    structural_state, structural_parse_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(path) DO UPDATE SET
                    content_hash = excluded.content_hash,
                    byte_count = excluded.byte_count,
                    structural_language = excluded.structural_language,
                    structural_state = excluded.structural_state,
                    structural_parse_ms = excluded.structural_parse_ms",
                params![
                    file.path,
                    file.hash,
                    file.bytes.len() as i64,
                    parsed.language.map(|language| language.as_str()),
                    parsed.state.as_str(),
                    parsed.parse_ms,
                ],
            )?;
            let text = std::str::from_utf8(&file.bytes)
                .map_err(|_| MemoryError::Integrity("validated UTF-8 file changed".into()))?;
            for (start, end) in chunk_spans(text) {
                let excerpt = &text[start..end];
                let start_line = 1 + text[..start].bytes().filter(|byte| *byte == b'\n').count();
                let end_line = start_line + excerpt.bytes().filter(|byte| *byte == b'\n').count();
                transaction.execute(
                    "INSERT INTO chunks_fts(
                        path, start_byte, end_byte, start_line, end_line, content
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        file.path,
                        start as i64,
                        end as i64,
                        start_line as i64,
                        end_line as i64,
                        excerpt
                    ],
                )?;
            }
            for symbol in &parsed.symbols {
                transaction.execute(
                    "INSERT INTO project_symbols(
                        symbol_key, path, name, kind, qualified_name, parent_key,
                        start_byte, end_byte, start_line, end_line, is_test
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                    params![
                        symbol.symbol_key,
                        file.path,
                        symbol.name,
                        symbol.kind,
                        symbol.qualified_name,
                        symbol.parent_key,
                        symbol.start_byte as i64,
                        symbol.end_byte as i64,
                        symbol.start_line as i64,
                        symbol.end_line as i64,
                        i64::from(symbol.is_test),
                    ],
                )?;
                transaction.execute(
                    "INSERT INTO project_symbols_fts(
                        symbol_key, path, name, qualified_name, kind, search_text
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        symbol.symbol_key,
                        file.path,
                        symbol.name,
                        symbol.qualified_name,
                        symbol.kind,
                        symbol_search_text(
                            &symbol.name,
                            &symbol.qualified_name,
                            &symbol.kind,
                            &file.path,
                        ),
                    ],
                )?;
            }
            for reference in &parsed.references {
                let source_key = reference.source_key.as_deref().ok_or_else(|| {
                    MemoryError::Integrity("structural reference has no source symbol".into())
                })?;
                transaction.execute(
                    "INSERT INTO project_references(
                        source_path, source_key, target_name, kind, start_byte, end_byte
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        file.path,
                        source_key,
                        reference.target_name,
                        reference.kind.as_str(),
                        reference.start_byte as i64,
                        reference.end_byte as i64,
                    ],
                )?;
            }
        }
        rebuild_project_edges(&transaction)?;
        set_meta(
            &transaction,
            "schema_version",
            &PROJECT_INDEX_SCHEMA.to_string(),
        )?;
        set_meta(&transaction, "project_id", &self.marker.project_id)?;
        set_meta(&transaction, "snapshot", &snapshot)?;
        set_meta(&transaction, "head", &head)?;
        set_meta(&transaction, "dirty", if dirty { "1" } else { "0" })?;
        set_meta(
            &transaction,
            "coverage_untracked_excluded",
            &untracked_excluded.to_string(),
        )?;
        set_meta(
            &transaction,
            "coverage_excluded_paths",
            &excluded_paths.to_string(),
        )?;
        set_meta(
            &transaction,
            "coverage_non_allowlisted",
            &non_allowlisted.to_string(),
        )?;
        set_meta(
            &transaction,
            "coverage_non_allowlisted_extensions",
            &serde_json::to_string(&non_allowlisted_extensions)?,
        )?;
        set_meta(
            &transaction,
            "coverage_oversize_skipped",
            &oversize_skipped.to_string(),
        )?;
        set_meta(
            &transaction,
            "coverage_non_utf8_skipped",
            &non_utf8_skipped.to_string(),
        )?;
        set_meta(&transaction, "structural_policy", STRUCTURAL_POLICY_VERSION)?;
        set_meta(
            &transaction,
            "structural_grammar_revision",
            STRUCTURAL_GRAMMAR_REVISION,
        )?;
        set_meta(&transaction, "completed_at", &completed_at.to_rfc3339())?;
        transaction.commit()?;
        let chunk_count = connection.query_row("SELECT COUNT(*) FROM chunks_fts", [], |row| {
            row.get::<_, i64>(0)
        })? as usize;
        let structural_files = count_where(
            &connection,
            "SELECT COUNT(*) FROM project_files WHERE structural_state IN ('ready', 'partial_parse')",
        )?;
        let fallback_files = count_where(
            &connection,
            "SELECT COUNT(*) FROM project_files WHERE structural_state = 'unsupported'",
        )?;
        let partial_parse_files = count_where(
            &connection,
            "SELECT COUNT(*) FROM project_files WHERE structural_state = 'partial_parse'",
        )?;
        let parse_error_files = count_where(
            &connection,
            "SELECT COUNT(*) FROM project_files WHERE structural_state IN ('parse_error', 'timed_out')",
        )?;
        let symbol_count = count_where(&connection, "SELECT COUNT(*) FROM project_symbols")?;
        let edge_count = count_where(&connection, "SELECT COUNT(*) FROM project_edges")?;
        Ok(ProjectIndexReport {
            schema_version: PROJECT_INDEX_SCHEMA,
            project_id: self.marker.project_id.clone(),
            root: self.root.display().to_string(),
            snapshot,
            head,
            dirty,
            indexed_files: files.len(),
            indexed_bytes,
            changed_files: changed.len(),
            changed_bytes,
            removed_files: removed.len(),
            skipped_files,
            chunk_count,
            structural_files,
            fallback_files,
            partial_parse_files,
            parse_error_files,
            symbol_count,
            edge_count,
            structural_parse_ms,
            completed_at,
        })
    }

    pub fn status(&self) -> Result<ProjectIndexStatus> {
        let (head, current_snapshot, dirty) = self.git_snapshot()?;
        let connection = self
            .database_path
            .exists()
            .then(|| self.open_database())
            .transpose()?;
        let indexed_snapshot = connection
            .as_ref()
            .map(|connection| get_meta(connection, "snapshot"))
            .transpose()?
            .flatten();
        let indexed_files = connection
            .as_ref()
            .map(|connection| {
                connection.query_row("SELECT COUNT(*) FROM project_files", [], |row| {
                    row.get::<_, i64>(0)
                })
            })
            .transpose()?
            .unwrap_or(0) as usize;
        let indexed_bytes = connection
            .as_ref()
            .map(|connection| {
                connection.query_row(
                    "SELECT COALESCE(SUM(byte_count), 0) FROM project_files",
                    [],
                    |row| row.get::<_, i64>(0),
                )
            })
            .transpose()?
            .unwrap_or(0) as u64;
        let chunk_count = connection
            .as_ref()
            .map(|connection| {
                connection.query_row("SELECT COUNT(*) FROM chunks_fts", [], |row| {
                    row.get::<_, i64>(0)
                })
            })
            .transpose()?
            .unwrap_or(0) as usize;
        let structural_files = optional_count(
            connection.as_ref(),
            "SELECT COUNT(*) FROM project_files WHERE structural_state IN ('ready', 'partial_parse')",
        )?;
        let fallback_files = optional_count(
            connection.as_ref(),
            "SELECT COUNT(*) FROM project_files WHERE structural_state = 'unsupported'",
        )?;
        let partial_parse_files = optional_count(
            connection.as_ref(),
            "SELECT COUNT(*) FROM project_files WHERE structural_state = 'partial_parse'",
        )?;
        let parse_error_files = optional_count(
            connection.as_ref(),
            "SELECT COUNT(*) FROM project_files WHERE structural_state IN ('parse_error', 'timed_out')",
        )?;
        let symbol_count =
            optional_count(connection.as_ref(), "SELECT COUNT(*) FROM project_symbols")?;
        let edge_count = optional_count(connection.as_ref(), "SELECT COUNT(*) FROM project_edges")?;
        let last_completed_at = connection
            .as_ref()
            .map(|connection| get_meta(connection, "completed_at"))
            .transpose()?
            .flatten();
        let ready = indexed_snapshot.is_some();
        Ok(ProjectIndexStatus {
            schema_version: PROJECT_INDEX_SCHEMA,
            project_id: self.marker.project_id.clone(),
            root: self.root.display().to_string(),
            database: self.database_path.display().to_string(),
            ready,
            stale: indexed_snapshot.as_deref() != Some(current_snapshot.as_str()),
            auto_reindex: self.marker.project_index.auto_reindex,
            include_untracked: self.marker.project_index.include_untracked,
            indexed_snapshot,
            current_snapshot,
            head,
            dirty,
            indexed_files,
            indexed_bytes,
            chunk_count,
            structural_policy: STRUCTURAL_POLICY_VERSION.into(),
            structural_grammar_revision: STRUCTURAL_GRAMMAR_REVISION.into(),
            structural_files,
            fallback_files,
            partial_parse_files,
            parse_error_files,
            symbol_count,
            edge_count,
            last_completed_at,
        })
    }

    pub fn search(
        &self,
        query: &str,
        limit: usize,
        allow_auto_reindex: bool,
    ) -> Result<ProjectSearchReport> {
        if query.trim().is_empty() || query.len() > crate::protocol::MAX_QUERY_BYTES {
            return Err(MemoryError::InvalidRequest(format!(
                "project search query must contain 1..={} bytes",
                crate::protocol::MAX_QUERY_BYTES
            )));
        }
        if limit == 0 || limit > MAX_PROJECT_SEARCH_RESULTS {
            return Err(MemoryError::InvalidRequest(format!(
                "project search limit must be between 1 and {MAX_PROJECT_SEARCH_RESULTS}"
            )));
        }
        let mut status = self.status()?;
        let mut reindex_attempted = false;
        let mut reindex_error = None;
        if allow_auto_reindex
            && status.stale
            && matches!(
                self.marker.project_index.auto_reindex,
                AutoReindexMode::OnSearch | AutoReindexMode::Watch
            )
        {
            reindex_attempted = true;
            if let Err(error) = self.index() {
                reindex_error = Some(error.to_string());
            }
            status = self.status()?;
        }
        if !status.ready {
            return Err(MemoryError::InvalidRequest(reindex_error.unwrap_or_else(
                || "project index is not ready; run `memoree project index`".into(),
            )));
        }
        let expression = fts_expression(query)?;
        let connection = self.open_database()?;
        let mut statement = connection.prepare(
            "SELECT path, start_byte, end_byte, start_line, end_line, content,
                    bm25(chunks_fts)
               FROM chunks_fts
              WHERE chunks_fts MATCH ?1
              ORDER BY bm25(chunks_fts), path, start_byte
              LIMIT ?2",
        )?;
        let rows = statement.query_map(params![expression, (limit + 1) as i64], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)? as usize,
                row.get::<_, i64>(2)? as usize,
                row.get::<_, i64>(3)? as usize,
                row.get::<_, i64>(4)? as usize,
                row.get::<_, String>(5)?,
                row.get::<_, f64>(6)?,
            ))
        })?;
        let mut hits = Vec::new();
        for row in rows {
            let (path, start_byte, end_byte, start_line, end_line, excerpt, bm25) = row?;
            let hash: String = connection.query_row(
                "SELECT content_hash FROM project_files WHERE path = ?1",
                [&path],
                |row| row.get(0),
            )?;
            hits.push(ProjectSearchHit {
                citation: project_citation(
                    &self.marker.project_id,
                    &path,
                    &hash,
                    start_byte,
                    end_byte,
                ),
                path,
                start_byte,
                end_byte,
                start_line,
                end_line,
                excerpt,
                score: -bm25,
            });
        }
        let truncated = hits.len() > limit;
        hits.truncate(limit);
        Ok(ProjectSearchReport {
            content_is_untrusted: true,
            authority: "working_tree_projection; Git/repository state remains authoritative".into(),
            project_id: self.marker.project_id.clone(),
            snapshot: status.indexed_snapshot.unwrap_or_default(),
            stale: status.stale,
            reindex_attempted,
            reindex_error,
            hits,
            truncated,
        })
    }

    /// Build one bounded, task-oriented structural navigation packet.
    ///
    /// Structural rows are hints only. Every returned lead and relation is
    /// reread from the working tree and must match its indexed content hash.
    pub fn map(
        &self,
        query: &str,
        max_bytes: usize,
        allow_auto_reindex: bool,
    ) -> Result<ProjectMapReport> {
        if query.trim().is_empty() || query.len() > crate::protocol::MAX_QUERY_BYTES {
            return Err(MemoryError::InvalidRequest(format!(
                "project map query must contain 1..={} bytes",
                crate::protocol::MAX_QUERY_BYTES
            )));
        }
        if !(2_048..=MAX_PROJECT_MAP_BYTES).contains(&max_bytes) {
            return Err(MemoryError::InvalidRequest(format!(
                "project map max_bytes must be between 2048 and {MAX_PROJECT_MAP_BYTES}"
            )));
        }
        let mut status = self.status()?;
        let mut reindex_attempted = false;
        let mut reindex_error = None;
        if allow_auto_reindex
            && status.stale
            && matches!(
                self.marker.project_index.auto_reindex,
                AutoReindexMode::OnSearch | AutoReindexMode::Watch
            )
        {
            reindex_attempted = true;
            if let Err(error) = self.index() {
                reindex_error = Some(error.to_string());
            }
            status = self.status()?;
        }
        if !status.ready {
            let coverage = ProjectMapCoverage {
                indexed_files: 0,
                include_untracked: self.marker.project_index.include_untracked,
                untracked_excluded: if self.marker.project_index.include_untracked {
                    0
                } else {
                    self.list_untracked_files()?.len()
                },
                excluded_paths: 0,
                non_allowlisted: 0,
                non_allowlisted_extensions: BTreeMap::new(),
                oversize_skipped: 0,
                non_utf8_skipped: 0,
            };
            let mut report = ProjectMapReport {
                content_is_untrusted: true,
                authority: "no_project_projection; repository state remains authoritative".into(),
                project_id: self.marker.project_id.clone(),
                snapshot: String::new(),
                stale: true,
                reindex_attempted,
                reindex_error,
                structural_state: "not_ready".into(),
                query_mode: project_map_mode(query),
                presence: "none".into(),
                absence:
                    "project_index_not_ready; use repository tools and do not infer source absence"
                        .into(),
                leads: vec![],
                alternatives: vec![],
                lexical_anchors: vec![],
                lexical_residue: vec![],
                mentions_truncated: true,
                coverage,
                limits: project_map_limits(),
                text_fallback: vec![],
                truncated: false,
                max_bytes,
            };
            enforce_project_map_budget(&mut report)?;
            return Ok(report);
        }

        let connection = self.open_database()?;
        let (coverage, coverage_complete) = self.map_coverage(&connection, &status)?;
        let query_mode = project_map_mode(query).to_owned();
        let mut structural_state = if status.symbol_count == 0 {
            "fts_fallback"
        } else {
            "ready"
        }
        .to_owned();
        let expression = project_map_expression(query)?;
        let exact_terms = project_map_terms(query);
        let explicit_terms = project_map_explicit_terms(query);
        let mut statement = connection.prepare(
            "SELECT symbol.symbol_key, symbol.path, file.content_hash,
                    symbol.name, symbol.kind, symbol.qualified_name,
                    symbol.start_byte, symbol.end_byte,
                    symbol.start_line, symbol.end_line, file.structural_state,
                    bm25(project_symbols_fts, 0.0, 0.0, 8.0, 4.0, 1.0, 2.0)
               FROM project_symbols_fts
               JOIN project_symbols symbol
                 ON symbol.symbol_key = project_symbols_fts.symbol_key
               JOIN project_files file ON file.path = symbol.path
              WHERE project_symbols_fts MATCH ?1
              ORDER BY bm25(project_symbols_fts, 0.0, 0.0, 8.0, 4.0, 1.0, 2.0),
                       symbol.path, symbol.start_byte
              LIMIT ?2",
        )?;
        let rows = statement.query_map(
            params![expression, MAX_PROJECT_MAP_CANDIDATES as i64],
            |row| {
                Ok(IndexedSymbol {
                    symbol_key: row.get(0)?,
                    path: row.get(1)?,
                    content_hash: row.get(2)?,
                    structural_state: row.get(10)?,
                    name: row.get(3)?,
                    kind: row.get(4)?,
                    qualified_name: row.get(5)?,
                    start_byte: row.get::<_, i64>(6)? as usize,
                    end_byte: row.get::<_, i64>(7)? as usize,
                    start_line: row.get::<_, i64>(8)? as usize,
                    end_line: row.get::<_, i64>(9)? as usize,
                    score: -row.get::<_, f64>(11)?,
                    exact_name_match: false,
                })
            },
        )?;
        let mut candidates = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        // FTS is deliberately broad for natural-language discovery, but its
        // candidate cap must never crowd an explicitly named symbol out of
        // the packet. Add exact-name rows before local scoring and deduping.
        let mut exact_statement = connection.prepare(
            "SELECT symbol.symbol_key, symbol.path, file.content_hash,
                    symbol.name, symbol.kind, symbol.qualified_name,
                    symbol.start_byte, symbol.end_byte,
                    symbol.start_line, symbol.end_line, file.structural_state
               FROM project_symbols symbol
               JOIN project_files file ON file.path = symbol.path
              WHERE lower(symbol.name) = ?1
              ORDER BY symbol.path, symbol.start_byte
              LIMIT ?2",
        )?;
        for term in explicit_terms.iter().take(32) {
            let rows =
                exact_statement.query_map(params![term, MAX_PROJECT_MAP_LEADS as i64], |row| {
                    Ok(IndexedSymbol {
                        symbol_key: row.get(0)?,
                        path: row.get(1)?,
                        content_hash: row.get(2)?,
                        structural_state: row.get(10)?,
                        name: row.get(3)?,
                        kind: row.get(4)?,
                        qualified_name: row.get(5)?,
                        start_byte: row.get::<_, i64>(6)? as usize,
                        end_byte: row.get::<_, i64>(7)? as usize,
                        start_line: row.get::<_, i64>(8)? as usize,
                        end_line: row.get::<_, i64>(9)? as usize,
                        score: 0.0,
                        exact_name_match: true,
                    })
                })?;
            candidates.extend(rows.collect::<rusqlite::Result<Vec<_>>>()?);
        }
        for symbol in &mut candidates {
            let searchable_terms = identifier_words(&format!(
                "{} {} {}",
                symbol.name, symbol.qualified_name, symbol.path
            ))
            .into_iter()
            .collect::<BTreeSet<_>>();
            let matched_terms = exact_terms
                .iter()
                .filter(|term| searchable_terms.contains(*term))
                .count();
            symbol.score += matched_terms as f64 * 12.0;
            symbol.exact_name_match = explicit_terms
                .iter()
                .any(|term| symbol.name.eq_ignore_ascii_case(term));
            if symbol.exact_name_match {
                symbol.score += 100.0;
            }
            if matches!(symbol.kind.as_str(), "function" | "method")
                && (project_map_is_relation_focused(&query_mode) || query_mode == "mixed")
            {
                symbol.score += 4.0;
            }
            if symbol.kind == "file" {
                symbol.score -= 1.0;
            }
        }
        candidates.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.path.cmp(&right.path))
                .then_with(|| left.start_byte.cmp(&right.start_byte))
        });
        candidates.dedup_by(|left, right| left.symbol_key == right.symbol_key);
        let has_exact_anchor = candidates.iter().any(|symbol| symbol.exact_name_match);

        let relation_focused = project_map_is_relation_focused(&query_mode);
        let lead_limit = if relation_focused {
            MAX_PROJECT_MAP_RELATION_LEADS
        } else {
            5.min(MAX_PROJECT_MAP_LEADS)
        };
        let excerpt_bytes = if relation_focused {
            MAX_PROJECT_MAP_RELATION_EXCERPT_BYTES
        } else {
            MAX_PROJECT_MAP_EXCERPT_BYTES
        };
        let mut leads = Vec::new();
        let mut alternatives = Vec::new();
        let mut stale_projection_rows = false;
        let mut partial_structural_lead = false;
        for symbol in candidates.into_iter() {
            if has_exact_anchor && !symbol.exact_name_match {
                if alternatives.len() < MAX_PROJECT_MAP_ALTERNATIVES {
                    alternatives.push(ProjectMapAlternative {
                        name: symbol.name,
                        kind: symbol.kind,
                        path: symbol.path,
                        start_line: symbol.start_line,
                    });
                }
                continue;
            }
            if leads.len() >= lead_limit {
                if alternatives.len() < MAX_PROJECT_MAP_ALTERNATIVES {
                    alternatives.push(ProjectMapAlternative {
                        name: symbol.name,
                        kind: symbol.kind,
                        path: symbol.path,
                        start_line: symbol.start_line,
                    });
                }
                if alternatives.len() == MAX_PROJECT_MAP_ALTERNATIVES {
                    break;
                }
                continue;
            }
            let Some((citation, excerpt, excerpt_truncated)) =
                self.verified_symbol_excerpt(&symbol, excerpt_bytes)?
            else {
                stale_projection_rows = true;
                continue;
            };
            partial_structural_lead |= symbol.structural_state != "ready";
            let (edges, edges_truncated, test_leads_truncated, facet_completeness, stale_edges) =
                self.verified_symbol_edges(&connection, &symbol, &query_mode, leads.is_empty())?;
            stale_projection_rows |= stale_edges;
            let facets = project_map_facets(&query_mode, &edges, &facet_completeness);
            leads.push(ProjectMapLead {
                symbol_key: symbol.symbol_key,
                name: symbol.name,
                kind: symbol.kind,
                qualified_name: symbol.qualified_name,
                path: symbol.path,
                start_byte: symbol.start_byte,
                end_byte: symbol.end_byte,
                start_line: symbol.start_line,
                end_line: symbol.end_line,
                citation,
                excerpt,
                excerpt_truncated,
                score: symbol.score,
                facets,
                edges,
                edges_truncated,
                test_leads_truncated,
            });
        }

        let lexical_anchors = if relation_focused {
            leads
                .first()
                .map(|lead| vec![lead.name.clone()])
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        let (lexical_residue, mut mentions_truncated) = if !lexical_anchors.is_empty() {
            self.verified_lexical_residue(&connection, &leads[..1])?
        } else {
            (Vec::new(), false)
        };
        mentions_truncated |= !coverage_complete;

        let mut text_fallback = Vec::new();
        if leads.is_empty() || structural_state != "ready" {
            let fallback = self.search(query, 3, false)?;
            for hit in fallback.hits {
                if leads.iter().any(|lead| {
                    lead.path == hit.path
                        && lead.start_byte <= hit.start_byte
                        && lead.end_byte >= hit.end_byte
                }) {
                    continue;
                }
                match self.get(&hit.citation, MAX_PROJECT_MAP_EXCERPT_BYTES) {
                    Ok(verified) => text_fallback.push(ProjectSearchHit {
                        path: hit.path,
                        start_byte: verified.start_byte,
                        end_byte: verified.end_byte,
                        start_line: hit.start_line,
                        end_line: hit.end_line,
                        excerpt: verified.content,
                        citation: verified.citation,
                        score: hit.score,
                    }),
                    Err(_) => stale_projection_rows = true,
                }
            }
        }
        if leads.is_empty() && !text_fallback.is_empty() {
            structural_state = "fts_fallback".into();
        } else if partial_structural_lead || !text_fallback.is_empty() {
            structural_state = "partial".into();
        }
        let presence = if !leads.is_empty() {
            "symbols"
        } else if !text_fallback.is_empty() {
            "text_only"
        } else {
            "none"
        }
        .to_owned();
        let absence = match presence.as_str() {
            "symbols" => "none; bounded structural leads were found",
            "text_only" => {
                "structural_match_not_found; bounded text fallback was found and is not proof of structural absence"
            }
            _ => {
                "not_found_within_the_bounded_index; this is not proof that the project lacks the requested behavior"
            }
        }
        .to_owned();
        let mut report = ProjectMapReport {
            content_is_untrusted: true,
            authority: "disposable_navigation_projection; only included hash_verified_working_tree_bytes are evidence; repository state remains authoritative".into(),
            project_id: self.marker.project_id.clone(),
            snapshot: status.indexed_snapshot.unwrap_or_default(),
            stale: status.stale || stale_projection_rows,
            reindex_attempted,
            reindex_error,
            structural_state,
            query_mode,
            presence,
            absence,
            leads,
            alternatives,
            lexical_anchors,
            lexical_residue,
            mentions_truncated,
            coverage,
            limits: project_map_limits(),
            text_fallback,
            truncated: stale_projection_rows,
            max_bytes,
        };
        enforce_project_map_budget(&mut report)?;
        Ok(report)
    }

    fn verified_lexical_residue(
        &self,
        connection: &Connection,
        leads: &[ProjectMapLead],
    ) -> Result<(Vec<ProjectMapMention>, bool)> {
        let mut occurrences = BTreeSet::new();
        let mut candidate_truncated = false;
        for lead in leads {
            if lead.name.is_empty() {
                continue;
            }
            let expression = fts_expression(&lead.name)?;
            let mut statement = connection.prepare(
                "SELECT chunk.path, file.content_hash, chunk.start_byte, chunk.content
                   FROM chunks_fts chunk
                   JOIN project_files file ON file.path = chunk.path
                  WHERE chunks_fts MATCH ?1
                  ORDER BY chunk.path, chunk.start_byte
                  LIMIT ?2",
            )?;
            let rows = statement.query_map(
                params![expression, (MAX_PROJECT_MAP_MENTION_CANDIDATES + 1) as i64],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)? as usize,
                        row.get::<_, String>(3)?,
                    ))
                },
            )?;
            let rows = rows.collect::<rusqlite::Result<Vec<_>>>()?;
            candidate_truncated |= rows.len() > MAX_PROJECT_MAP_MENTION_CANDIDATES;
            for (path, hash, chunk_start, content) in
                rows.into_iter().take(MAX_PROJECT_MAP_MENTION_CANDIDATES)
            {
                for (offset, _) in content.match_indices(&lead.name) {
                    if !exact_identifier_occurrence(&content, offset, lead.name.len()) {
                        continue;
                    }
                    let start = chunk_start + offset;
                    let end = start + lead.name.len();
                    let covered_by_anchor = leads.iter().any(|candidate| {
                        candidate.path == path
                            && candidate.start_byte <= start
                            && candidate.end_byte >= end
                    });
                    let covered_by_edge =
                        leads
                            .iter()
                            .flat_map(|candidate| &candidate.edges)
                            .any(|edge| {
                                edge.reference_path_internal == path
                                    && edge.reference_start_byte <= start
                                    && edge.reference_end_byte >= end
                            });
                    if !covered_by_anchor && !covered_by_edge {
                        occurrences.insert((
                            lead.name.clone(),
                            path.clone(),
                            hash.clone(),
                            start,
                            end,
                        ));
                    }
                }
            }
        }
        let mut groups = BTreeMap::<(String, String, String, String), Vec<(usize, usize)>>::new();
        for (anchor_name, path, hash, start, end) in occurrences {
            let context = connection
                .query_row(
                    "SELECT qualified_name FROM project_symbols
                      WHERE path = ?1 AND start_byte <= ?2 AND end_byte >= ?3
                      ORDER BY (end_byte - start_byte), start_byte LIMIT 1",
                    params![path, start as i64, end as i64],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .unwrap_or_else(|| "file".into());
            groups
                .entry((anchor_name, path, hash, context))
                .or_default()
                .push((start, end));
        }
        let mut mentions_truncated = candidate_truncated || groups.len() > MAX_PROJECT_MAP_MENTIONS;
        let mut mentions = Vec::new();
        let mut stale = false;
        for ((anchor_name, path, hash, context), mut positions) in
            groups.into_iter().take(MAX_PROJECT_MAP_MENTIONS)
        {
            positions.sort();
            positions.dedup();
            let bytes = match fs::read(self.root.join(&path)) {
                Ok(bytes) => bytes,
                Err(_) => {
                    stale = true;
                    continue;
                }
            };
            if blake3::hash(&bytes).to_hex().as_str() != hash {
                stale = true;
                continue;
            }
            let text = match std::str::from_utf8(&bytes) {
                Ok(text) => text,
                Err(_) => {
                    stale = true;
                    continue;
                }
            };
            let occurrence_count = positions.len();
            mentions_truncated |= occurrence_count > MAX_PROJECT_MAP_MENTION_LINES;
            let occurrence_lines = positions
                .iter()
                .take(MAX_PROJECT_MAP_MENTION_LINES)
                .map(|(start, _)| 1 + text[..*start].bytes().filter(|byte| *byte == b'\n').count())
                .collect::<Vec<_>>();
            let (start, end) = positions[0];
            let Some((citation, excerpt, excerpt_truncated, start_line, end_line)) = self
                .verified_reference_excerpt(
                    &path,
                    &hash,
                    start,
                    end,
                    MAX_PROJECT_MAP_MENTION_EXCERPT_BYTES,
                    true,
                )?
            else {
                stale = true;
                continue;
            };
            mentions.push(ProjectMapMention {
                classification: "unresolved_mention".into(),
                anchor_name,
                path,
                context,
                occurrence_count,
                occurrence_lines,
                start_line,
                end_line,
                citation,
                excerpt,
                excerpt_truncated,
            });
        }
        Ok((mentions, mentions_truncated || stale))
    }

    fn map_coverage(
        &self,
        connection: &Connection,
        status: &ProjectIndexStatus,
    ) -> Result<(ProjectMapCoverage, bool)> {
        let keys = [
            "coverage_excluded_paths",
            "coverage_non_allowlisted",
            "coverage_oversize_skipped",
            "coverage_non_utf8_skipped",
        ];
        let values = keys
            .iter()
            .map(|key| {
                get_meta(connection, key)
                    .map(|value| value.and_then(|value| value.parse::<usize>().ok()))
            })
            .collect::<Result<Vec<_>>>()?;
        let complete = values.iter().all(Option::is_some);
        let non_allowlisted_extensions =
            get_meta(connection, "coverage_non_allowlisted_extensions")?
                .and_then(|value| serde_json::from_str::<BTreeMap<String, usize>>(&value).ok());
        let complete = complete && non_allowlisted_extensions.is_some();
        let untracked_excluded = if self.marker.project_index.include_untracked {
            0
        } else {
            self.list_untracked_files()?.len()
        };
        Ok((
            ProjectMapCoverage {
                indexed_files: status.indexed_files,
                include_untracked: self.marker.project_index.include_untracked,
                untracked_excluded,
                excluded_paths: values[0].unwrap_or(0),
                non_allowlisted: values[1].unwrap_or(0),
                non_allowlisted_extensions: non_allowlisted_extensions.unwrap_or_default(),
                oversize_skipped: values[2].unwrap_or(0),
                non_utf8_skipped: values[3].unwrap_or(0),
            },
            complete,
        ))
    }

    fn verified_symbol_excerpt(
        &self,
        symbol: &IndexedSymbol,
        max_bytes: usize,
    ) -> Result<Option<(String, String, bool)>> {
        let bytes = match fs::read(self.root.join(&symbol.path)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if blake3::hash(&bytes).to_hex().as_str() != symbol.content_hash {
            return Ok(None);
        }
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| MemoryError::Integrity("indexed structural source is not UTF-8".into()))?;
        if symbol.start_byte >= symbol.end_byte
            || symbol.end_byte > text.len()
            || !text.is_char_boundary(symbol.start_byte)
            || !text.is_char_boundary(symbol.end_byte)
        {
            return Ok(None);
        }
        let mut returned_end = symbol
            .end_byte
            .min(symbol.start_byte.saturating_add(max_bytes));
        while returned_end > symbol.start_byte && !text.is_char_boundary(returned_end) {
            returned_end -= 1;
        }
        Ok(Some((
            project_citation(
                &self.marker.project_id,
                &symbol.path,
                &symbol.content_hash,
                symbol.start_byte,
                returned_end,
            ),
            text[symbol.start_byte..returned_end].to_owned(),
            returned_end < symbol.end_byte,
        )))
    }

    fn transitive_test_leads(
        &self,
        connection: &Connection,
        target: &IndexedSymbol,
    ) -> Result<(Vec<IndexedMapEdge>, bool, bool, bool)> {
        #[derive(Clone)]
        struct TraversalState {
            current: IndexedSymbol,
            chain_from_target: Vec<IndexedSymbol>,
        }

        let mut frontier = vec![TraversalState {
            current: target.clone(),
            chain_from_target: Vec::new(),
        }];
        let mut visited = BTreeMap::from([(target.symbol_key.clone(), 0usize)]);
        let mut results = BTreeMap::<String, IndexedMapEdge>::new();
        let mut visited_nodes = 0usize;
        let mut truncated = false;
        let mut test_leads_truncated = false;
        let mut stale = false;

        for depth in 1..=MAX_PROJECT_MAP_TEST_HOPS {
            frontier.sort_by(|left, right| {
                left.current
                    .path
                    .cmp(&right.current.path)
                    .then_with(|| left.current.start_byte.cmp(&right.current.start_byte))
                    .then_with(|| {
                        left.current
                            .qualified_name
                            .cmp(&right.current.qualified_name)
                    })
            });
            let mut next = Vec::new();
            let current_frontier = std::mem::take(&mut frontier);
            for state in current_frontier {
                let (callers, local_truncated, local_test_leads_truncated) =
                    self.unique_incoming_callers(connection, &state.current)?;
                truncated |= local_truncated;
                test_leads_truncated |= local_test_leads_truncated;
                for mut caller in callers {
                    visited_nodes += 1;
                    if visited_nodes > MAX_PROJECT_MAP_TEST_TRAVERSAL_NODES {
                        truncated = true;
                        test_leads_truncated = true;
                        break;
                    }
                    if caller.related_is_test {
                        if depth >= 2 && !results.contains_key(&caller.related.symbol_key) {
                            let mut via = Vec::new();
                            let mut verified = true;
                            for intermediate in state.chain_from_target.iter().rev() {
                                if self.verified_symbol_excerpt(intermediate, 64)?.is_none() {
                                    stale = true;
                                    verified = false;
                                    break;
                                }
                                via.push(ProjectMapVia {
                                    name: intermediate.name.clone(),
                                    path: intermediate.path.clone(),
                                    start_line: intermediate.start_line,
                                });
                            }
                            if verified {
                                caller.direction = "outgoing".into();
                                caller.kind = "behavioral_test_lead".into();
                                caller.hops = depth;
                                caller.via = via;
                                results.insert(caller.related.symbol_key.clone(), caller);
                            }
                        }
                        continue;
                    }
                    if visited
                        .get(&caller.related.symbol_key)
                        .is_some_and(|seen_depth| *seen_depth <= depth)
                    {
                        continue;
                    }
                    visited.insert(caller.related.symbol_key.clone(), depth);
                    let mut chain = state.chain_from_target.clone();
                    chain.push(caller.related.clone());
                    next.push(TraversalState {
                        current: caller.related,
                        chain_from_target: chain,
                    });
                }
                if visited_nodes > MAX_PROJECT_MAP_TEST_TRAVERSAL_NODES {
                    break;
                }
            }
            if visited_nodes > MAX_PROJECT_MAP_TEST_TRAVERSAL_NODES {
                break;
            }
            frontier = next;
            if frontier.is_empty() {
                break;
            }
        }
        if !frontier.is_empty() {
            truncated = true;
            test_leads_truncated = true;
        }
        let mut results = results.into_values().collect::<Vec<_>>();
        results.sort_by(|left, right| {
            left.hops
                .cmp(&right.hops)
                .then_with(|| left.reference_path.cmp(&right.reference_path))
                .then_with(|| left.reference_start.cmp(&right.reference_start))
                .then_with(|| {
                    left.related
                        .qualified_name
                        .cmp(&right.related.qualified_name)
                })
        });
        Ok((results, truncated, test_leads_truncated, stale))
    }

    fn unique_incoming_callers(
        &self,
        connection: &Connection,
        target: &IndexedSymbol,
    ) -> Result<(Vec<IndexedMapEdge>, bool, bool)> {
        let ambiguity_pruned = connection.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM project_edges
                 WHERE target_key = ?1 AND kind = 'calls' AND confidence = 'ambiguous'
             )",
            [&target.symbol_key],
            |row| row.get::<_, bool>(0),
        )?;
        let mut statement = connection.prepare(
            "SELECT edge.confidence,
                    source.symbol_key, source.name, source.kind, source.qualified_name,
                    source.path, source_file.content_hash, source_file.structural_state,
                    source.start_byte, source.end_byte, source.start_line, source.end_line,
                    source.is_test, edge.source_path, reference_file.content_hash,
                    edge.start_byte, edge.end_byte
               FROM project_edges edge
               JOIN project_symbols source ON source.symbol_key = edge.source_key
               JOIN project_files source_file ON source_file.path = source.path
               JOIN project_files reference_file ON reference_file.path = edge.source_path
              WHERE edge.target_key = ?1
                AND edge.kind = 'calls'
                AND edge.confidence = 'inferred'
                AND source.is_test = ?2
                AND edge.edge_id = (
                    SELECT candidate.edge_id FROM project_edges candidate
                     WHERE candidate.source_key = edge.source_key
                       AND candidate.target_key = edge.target_key
                       AND candidate.kind = edge.kind
                       AND candidate.confidence = edge.confidence
                     ORDER BY candidate.source_path, candidate.start_byte,
                              candidate.end_byte, candidate.edge_id
                     LIMIT 1
                )
              ORDER BY CASE WHEN EXISTS(
                           SELECT 1
                             FROM project_edges child_edge
                             JOIN project_symbols child_source
                               ON child_source.symbol_key = child_edge.source_key
                            WHERE child_edge.target_key = source.symbol_key
                              AND child_edge.kind = 'calls'
                              AND child_edge.confidence = 'inferred'
                              AND child_source.is_test = 1
                       ) THEN 0 ELSE 1 END,
                       source.path, source.start_byte, source.qualified_name,
                       source.symbol_key
              LIMIT ?3",
        )?;
        let mut callers = Vec::new();
        let mut callers_truncated = false;
        let mut test_callers_truncated = false;
        let mut resolution_pruned = false;
        let target_owner = self.verified_symbol_owner_words(target)?;
        for (is_test, cap) in [
            (true, MAX_PROJECT_MAP_TEST_CALLERS_PER_NODE),
            (false, MAX_PROJECT_MAP_NON_TEST_CALLERS_PER_NODE),
        ] {
            let scan_limit = cap.saturating_mul(4).saturating_add(1);
            let rows = statement.query_map(
                params![target.symbol_key, i64::from(is_test), scan_limit as i64],
                |row| {
                    Ok(IndexedMapEdge {
                        direction: "incoming".into(),
                        kind: StructuralEdgeKind::Calls.as_str().into(),
                        confidence: row.get(0)?,
                        hops: 1,
                        via: Vec::new(),
                        related: IndexedSymbol {
                            symbol_key: row.get(1)?,
                            name: row.get(2)?,
                            kind: row.get(3)?,
                            qualified_name: row.get(4)?,
                            path: row.get(5)?,
                            content_hash: row.get(6)?,
                            structural_state: row.get(7)?,
                            start_byte: row.get::<_, i64>(8)? as usize,
                            end_byte: row.get::<_, i64>(9)? as usize,
                            start_line: row.get::<_, i64>(10)? as usize,
                            end_line: row.get::<_, i64>(11)? as usize,
                            score: 0.0,
                            exact_name_match: false,
                        },
                        related_is_test: row.get::<_, i64>(12)? != 0,
                        reference_path: row.get(13)?,
                        reference_hash: row.get(14)?,
                        reference_start: row.get::<_, i64>(15)? as usize,
                        reference_end: row.get::<_, i64>(16)? as usize,
                    })
                },
            )?;
            let raw_class = rows.collect::<rusqlite::Result<Vec<_>>>()?;
            if raw_class.len() == scan_limit {
                callers_truncated = true;
                test_callers_truncated |= is_test;
            }
            let mut class = Vec::new();
            for edge in raw_class {
                if self.traversal_call_compatible(&edge, target, target_owner.as_deref())? {
                    class.push(edge);
                } else {
                    resolution_pruned = true;
                }
            }
            if class.len() > cap {
                callers_truncated = true;
                test_callers_truncated |= is_test;
                class.truncate(cap);
            }
            callers.extend(class);
        }
        callers.sort_by(|left, right| {
            right
                .related_is_test
                .cmp(&left.related_is_test)
                .then_with(|| left.related.path.cmp(&right.related.path))
                .then_with(|| left.related.start_byte.cmp(&right.related.start_byte))
                .then_with(|| left.related.symbol_key.cmp(&right.related.symbol_key))
        });
        let traversal_truncated = ambiguity_pruned || callers_truncated || resolution_pruned;
        let test_leads_truncated =
            ambiguity_pruned || callers_truncated || test_callers_truncated || resolution_pruned;
        Ok((callers, traversal_truncated, test_leads_truncated))
    }

    fn traversal_call_compatible(
        &self,
        edge: &IndexedMapEdge,
        target: &IndexedSymbol,
        target_owner: Option<&[String]>,
    ) -> Result<bool> {
        let bytes = match fs::read(self.root.join(&edge.reference_path)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        if blake3::hash(&bytes).to_hex().as_str() != edge.reference_hash {
            return Ok(false);
        }
        let text = match std::str::from_utf8(&bytes) {
            Ok(text) => text,
            Err(_) => return Ok(false),
        };
        if edge.reference_start >= edge.reference_end
            || edge.reference_end > text.len()
            || !text.is_char_boundary(edge.reference_start)
            || !text.is_char_boundary(edge.reference_end)
        {
            return Ok(false);
        }
        let expression = &text[edge.reference_start..edge.reference_end];
        let Some(form) = classify_call_form(expression, &target.name) else {
            return Ok(false);
        };
        match form {
            TraversalCallForm::Free => Ok(true),
            TraversalCallForm::Static { qualifier } => {
                if matches!(qualifier.as_str(), "crate" | "self" | "super") {
                    return Ok(true);
                }
                let Some(owner) = target_owner else {
                    return Ok(true);
                };
                Ok(receiver_matches_owner(&qualifier, owner))
            }
            TraversalCallForm::Receiver { receiver } => {
                let Some(owner) = target_owner else {
                    return Ok(false);
                };
                if matches!(receiver.as_str(), "self" | "this") {
                    let source_owner = self.verified_symbol_owner_words(&edge.related)?;
                    return Ok(source_owner.as_deref() == Some(owner));
                }
                Ok(receiver_matches_owner(&receiver, owner))
            }
        }
    }

    fn verified_symbol_owner_words(&self, symbol: &IndexedSymbol) -> Result<Option<Vec<String>>> {
        let bytes = match fs::read(self.root.join(&symbol.path)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if blake3::hash(&bytes).to_hex().as_str() != symbol.content_hash {
            return Ok(None);
        }
        let text = match std::str::from_utf8(&bytes) {
            Ok(text) => text,
            Err(_) => return Ok(None),
        };
        let suffix = symbol
            .qualified_name
            .strip_prefix(&format!("{}::", symbol.path))
            .unwrap_or(&symbol.qualified_name);
        let lineage = suffix.split('.').collect::<Vec<_>>();
        if lineage.len() >= 2 {
            let words = identifier_words(lineage[lineage.len() - 2]);
            if !words.is_empty() {
                return Ok(Some(words));
            }
        }
        if symbol.path.ends_with(".rs") && symbol.start_byte <= text.len() {
            return Ok(
                rust_impl_owner(&text[..symbol.start_byte]).map(|owner| identifier_words(&owner))
            );
        }
        Ok(None)
    }

    fn verified_symbol_edges(
        &self,
        connection: &Connection,
        symbol: &IndexedSymbol,
        query_mode: &str,
        include_behavioral_test_leads: bool,
    ) -> Result<(
        Vec<ProjectMapEdge>,
        bool,
        bool,
        ProjectMapFacetCompleteness,
        bool,
    )> {
        let mut statement = connection.prepare(
            "SELECT relation.direction, relation.kind, relation.confidence,
                    related.symbol_key, related.name, related.kind,
                    related.qualified_name, related.path,
                    related_file.content_hash, related_file.structural_state,
                    related.start_byte, related.end_byte,
                    related.start_line, related.end_line, related.is_test,
                    relation.source_path, reference_file.content_hash,
                    relation.start_byte, relation.end_byte
               FROM (
                    SELECT 'outgoing' AS direction, kind, confidence,
                           target_key AS related_key, source_path,
                           start_byte, end_byte
                      FROM project_edges WHERE source_key = ?1
                    UNION ALL
                    SELECT 'incoming' AS direction, kind, confidence,
                           source_key AS related_key, source_path,
                           start_byte, end_byte
                      FROM project_edges WHERE target_key = ?1
               ) relation
               JOIN project_symbols related ON related.symbol_key = relation.related_key
               JOIN project_files related_file ON related_file.path = related.path
               JOIN project_files reference_file ON reference_file.path = relation.source_path
              ORDER BY CASE relation.confidence
                         WHEN 'extracted' THEN 0 WHEN 'inferred' THEN 1 ELSE 2 END,
                       relation.kind, relation.direction, related.path, related.start_byte
              LIMIT ?2",
        )?;
        let rows = statement.query_map(
            params![
                symbol.symbol_key,
                (MAX_PROJECT_MAP_EDGE_CANDIDATES + 1) as i64
            ],
            |row| {
                Ok(IndexedMapEdge {
                    direction: row.get(0)?,
                    kind: row.get(1)?,
                    confidence: row.get(2)?,
                    hops: 1,
                    via: Vec::new(),
                    related: IndexedSymbol {
                        symbol_key: row.get(3)?,
                        name: row.get(4)?,
                        kind: row.get(5)?,
                        qualified_name: row.get(6)?,
                        path: row.get(7)?,
                        content_hash: row.get(8)?,
                        structural_state: row.get(9)?,
                        start_byte: row.get::<_, i64>(10)? as usize,
                        end_byte: row.get::<_, i64>(11)? as usize,
                        start_line: row.get::<_, i64>(12)? as usize,
                        end_line: row.get::<_, i64>(13)? as usize,
                        score: 0.0,
                        exact_name_match: false,
                    },
                    related_is_test: row.get::<_, i64>(14)? != 0,
                    reference_path: row.get(15)?,
                    reference_hash: row.get(16)?,
                    reference_start: row.get::<_, i64>(17)? as usize,
                    reference_end: row.get::<_, i64>(18)? as usize,
                })
            },
        )?;
        let raw_rows = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        let mut facet_completeness = ProjectMapFacetCompleteness::default();
        if raw_rows.len() > MAX_PROJECT_MAP_EDGE_CANDIDATES {
            facet_completeness = ProjectMapFacetCompleteness {
                direct_callers: false,
                direct_callees: false,
                direct_tests: false,
                behavioral_test_leads: false,
            };
        }
        let mut resolution_pruned = false;
        let mut rows = Vec::new();
        for edge in raw_rows {
            let call_target = if edge.kind == StructuralEdgeKind::TestedBy.as_str()
                || (edge.kind == StructuralEdgeKind::Calls.as_str() && edge.direction == "incoming")
            {
                Some(symbol)
            } else if edge.kind == StructuralEdgeKind::Calls.as_str()
                && edge.direction == "outgoing"
            {
                Some(&edge.related)
            } else {
                None
            };
            if let Some(call_target) = call_target {
                let owner = self.verified_symbol_owner_words(call_target)?;
                if !self.traversal_call_compatible(&edge, call_target, owner.as_deref())? {
                    resolution_pruned = true;
                    mark_indexed_facet_incomplete(&mut facet_completeness, &edge);
                    continue;
                }
            }
            rows.push(edge);
        }
        let mut candidate_truncated = rows.len() > MAX_PROJECT_MAP_EDGE_CANDIDATES;
        candidate_truncated |= resolution_pruned;
        let mut transitive_stale = false;
        let has_test_intent = query_mode.split('+').any(|intent| intent == "tests");
        let mut test_leads_truncated =
            has_test_intent && (!include_behavioral_test_leads || resolution_pruned);
        if has_test_intent && !include_behavioral_test_leads {
            facet_completeness.behavioral_test_leads = false;
        }
        if include_behavioral_test_leads && has_test_intent {
            let (test_leads, traversal_truncated, traversal_test_truncated, stale) =
                self.transitive_test_leads(connection, symbol)?;
            candidate_truncated |= traversal_truncated;
            test_leads_truncated |= traversal_test_truncated;
            facet_completeness.behavioral_test_leads &= !traversal_test_truncated;
            transitive_stale |= stale;
            rows.extend(test_leads);
        }
        let mut rows = rows
            .into_iter()
            .filter_map(|edge| {
                project_map_edge_priority(query_mode, &edge).map(|priority| (priority, edge))
            })
            .collect::<Vec<_>>();
        rows.sort_by(|(left_priority, left), (right_priority, right)| {
            left_priority
                .cmp(right_priority)
                .then_with(|| left.hops.cmp(&right.hops))
                .then_with(|| left.reference_path.cmp(&right.reference_path))
                .then_with(|| left.reference_start.cmp(&right.reference_start))
                .then_with(|| {
                    left.related
                        .qualified_name
                        .cmp(&right.related.qualified_name)
                })
        });
        if rows.len() > MAX_PROJECT_MAP_EDGE_CANDIDATES {
            candidate_truncated = true;
            for (_, edge) in &rows[MAX_PROJECT_MAP_EDGE_CANDIDATES..] {
                mark_indexed_facet_incomplete(&mut facet_completeness, edge);
            }
            rows.truncate(MAX_PROJECT_MAP_EDGE_CANDIDATES);
        }
        let mut seen_sites = BTreeSet::new();
        rows.retain(|(_, edge)| {
            seen_sites.insert((
                edge.related.symbol_key.clone(),
                edge.reference_path.clone(),
                edge.reference_start,
                edge.reference_end,
            ))
        });
        let mut occurrence_counts = BTreeMap::new();
        for (_, edge) in &rows {
            *occurrence_counts
                .entry((edge.kind.clone(), edge.related.symbol_key.clone()))
                .or_insert(0usize) += 1;
        }
        let mut occurrence_indices = BTreeMap::new();
        let mut edges = Vec::new();
        let mut stale = transitive_stale;
        if transitive_stale {
            facet_completeness.behavioral_test_leads = false;
        }
        let edges_truncated = candidate_truncated || rows.len() > MAX_PROJECT_MAP_EDGES_PER_LEAD;
        if rows.len() > MAX_PROJECT_MAP_EDGES_PER_LEAD {
            for (_, edge) in &rows[MAX_PROJECT_MAP_EDGES_PER_LEAD..] {
                mark_indexed_facet_incomplete(&mut facet_completeness, edge);
            }
        }
        if has_test_intent && edges_truncated {
            test_leads_truncated = true;
        }
        for (_, edge) in rows.into_iter().take(MAX_PROJECT_MAP_EDGES_PER_LEAD) {
            let Some(_) = self.verified_symbol_excerpt(&edge.related, 64)? else {
                stale = true;
                mark_indexed_facet_incomplete(&mut facet_completeness, &edge);
                continue;
            };
            let Some((
                citation,
                excerpt,
                excerpt_truncated,
                reference_start_line,
                reference_end_line,
            )) = self.verified_reference_excerpt(
                &edge.reference_path,
                &edge.reference_hash,
                edge.reference_start,
                edge.reference_end,
                MAX_PROJECT_MAP_REFERENCE_EXCERPT_BYTES,
                false,
            )?
            else {
                stale = true;
                mark_indexed_facet_incomplete(&mut facet_completeness, &edge);
                continue;
            };
            let occurrence_key = (edge.kind.clone(), edge.related.symbol_key.clone());
            let site_index = occurrence_indices
                .entry(occurrence_key.clone())
                .or_insert(0usize);
            *site_index += 1;
            edges.push(ProjectMapEdge {
                reference_path_internal: edge.reference_path.clone(),
                reference_start_byte: edge.reference_start,
                reference_end_byte: edge.reference_end,
                direction: edge.direction,
                kind: edge.kind,
                confidence: edge.confidence,
                hops: edge.hops,
                via: edge.via,
                name: edge.related.name,
                qualified_name: edge.related.qualified_name,
                path: edge.related.path.clone(),
                related_is_test: edge.related_is_test,
                test_surface: edge
                    .related_is_test
                    .then(|| project_test_surface(&edge.related.path).to_owned()),
                symbol_start_line: edge.related.start_line,
                symbol_end_line: edge.related.end_line,
                reference_start_line,
                reference_end_line,
                citation,
                excerpt,
                excerpt_truncated,
                site_index: *site_index,
                site_count: occurrence_counts.get(&occurrence_key).copied().unwrap_or(1),
            });
        }
        Ok((
            edges,
            edges_truncated,
            test_leads_truncated,
            facet_completeness,
            stale,
        ))
    }

    fn verified_reference_excerpt(
        &self,
        path: &str,
        expected_hash: &str,
        reference_start: usize,
        reference_end: usize,
        max_bytes: usize,
        adjacent_lines: bool,
    ) -> Result<Option<VerifiedReferenceExcerpt>> {
        let bytes = match fs::read(self.root.join(path)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if blake3::hash(&bytes).to_hex().as_str() != expected_hash {
            return Ok(None);
        }
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| MemoryError::Integrity("indexed structural source is not UTF-8".into()))?;
        if reference_start >= reference_end
            || reference_end > text.len()
            || !text.is_char_boundary(reference_start)
            || !text.is_char_boundary(reference_end)
        {
            return Ok(None);
        }
        let mut line_start = text[..reference_start]
            .rfind('\n')
            .map_or(0, |index| index + 1);
        let mut line_end = text[reference_end..]
            .find('\n')
            .map_or(text.len(), |index| reference_end + index + 1);
        if adjacent_lines && line_end - line_start <= max_bytes {
            loop {
                let mut changed = false;
                let previous_start = line_start
                    .checked_sub(1)
                    .and_then(|end| text[..end].rfind('\n').map(|index| index + 1))
                    .unwrap_or(0);
                if previous_start < line_start && line_end - previous_start <= max_bytes {
                    line_start = previous_start;
                    changed = true;
                }
                let next_end = if line_end < text.len() {
                    text[line_end..]
                        .find('\n')
                        .map_or(text.len(), |index| line_end + index + 1)
                } else {
                    line_end
                };
                if next_end > line_end && next_end - line_start <= max_bytes {
                    line_end = next_end;
                    changed = true;
                }
                if !changed {
                    break;
                }
            }
        }
        let mut excerpt_start = line_start;
        let mut excerpt_end = line_end;
        let mut truncated = false;
        if excerpt_end - excerpt_start > max_bytes {
            truncated = true;
            let reference_bytes = reference_end - reference_start;
            if reference_bytes >= max_bytes {
                excerpt_start = reference_start;
                excerpt_end = reference_start.saturating_add(max_bytes).min(reference_end);
            } else {
                let context = max_bytes - reference_bytes;
                excerpt_start = reference_start.saturating_sub(context / 2).max(line_start);
                excerpt_end = excerpt_start.saturating_add(max_bytes).min(line_end);
                excerpt_start = excerpt_end.saturating_sub(max_bytes).max(line_start);
            }
            while excerpt_start < reference_start && !text.is_char_boundary(excerpt_start) {
                excerpt_start += 1;
            }
            while excerpt_end > reference_end && !text.is_char_boundary(excerpt_end) {
                excerpt_end -= 1;
            }
            if reference_end - reference_start >= max_bytes {
                let mut floor = excerpt_start + max_bytes / 2;
                while floor < excerpt_end && !text.is_char_boundary(floor) {
                    floor += 1;
                }
                if let Some(relative) = text[floor..excerpt_end].rfind(|character: char| {
                    character.is_whitespace() || matches!(character, ',' | ')' | '}' | ';')
                }) {
                    let candidate = floor + relative + 1;
                    if candidate > reference_start {
                        excerpt_end = candidate;
                    }
                }
            }
        }
        let start_line = 1 + text[..excerpt_start]
            .bytes()
            .filter(|byte| *byte == b'\n')
            .count();
        let end_line = start_line
            + text[excerpt_start..excerpt_end]
                .bytes()
                .filter(|byte| *byte == b'\n')
                .count();
        Ok(Some((
            project_citation(
                &self.marker.project_id,
                path,
                expected_hash,
                excerpt_start,
                excerpt_end,
            ),
            text[excerpt_start..excerpt_end].to_owned(),
            truncated,
            start_line,
            end_line,
        )))
    }

    pub fn get(&self, citation: &str, max_bytes: usize) -> Result<ProjectGetResult> {
        if max_bytes == 0 || max_bytes > MAX_PROJECT_GET_BYTES {
            return Err(MemoryError::InvalidRequest(format!(
                "project get max_bytes must be between 1 and {MAX_PROJECT_GET_BYTES}"
            )));
        }
        let parsed = parse_project_citation(citation)?;
        if parsed.project_id != self.marker.project_id
            || !safe_project_path(&parsed.path)
            || excluded_path(&parsed.path)
        {
            return Err(MemoryError::InvalidRequest(
                "project citation does not belong to an allowed file in the active project".into(),
            ));
        }
        let absolute = self.root.join(&parsed.path);
        let metadata = fs::symlink_metadata(&absolute)?;
        if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
            return Err(MemoryError::InvalidRequest(
                "project citation target must be a regular non-symlink file".into(),
            ));
        }
        let mut options = OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.custom_flags(libc::O_NOFOLLOW);
        }
        let mut file = options.open(&absolute)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let actual_hash = blake3::hash(&bytes).to_hex().to_string();
        if actual_hash != parsed.hash {
            return Err(MemoryError::InvalidRequest(
                "project citation is stale because the working-tree file changed; reindex and search again".into(),
            ));
        }
        let text = std::str::from_utf8(&bytes).map_err(|_| {
            MemoryError::InvalidRequest("project citation is not UTF-8 text".into())
        })?;
        if parsed.start >= parsed.end
            || parsed.end > text.len()
            || !text.is_char_boundary(parsed.start)
            || !text.is_char_boundary(parsed.end)
        {
            return Err(MemoryError::InvalidRequest(
                "project citation byte range is invalid".into(),
            ));
        }
        let requested_end = parsed.end;
        let mut returned_end = requested_end.min(parsed.start.saturating_add(max_bytes));
        while returned_end > parsed.start && !text.is_char_boundary(returned_end) {
            returned_end -= 1;
        }
        Ok(ProjectGetResult {
            content_is_untrusted: true,
            authority: "hash_verified_working_tree_bytes".into(),
            citation: project_citation(
                &parsed.project_id,
                &parsed.path,
                &parsed.hash,
                parsed.start,
                returned_end,
            ),
            path: parsed.path,
            content_hash: parsed.hash,
            start_byte: parsed.start,
            end_byte: returned_end,
            content: text[parsed.start..returned_end].to_owned(),
            truncated: returned_end < requested_end,
        })
    }

    pub fn watch(
        &self,
        poll_ms: u64,
        max_poll_ms: u64,
        debounce_ms: u64,
        max_polls: Option<usize>,
    ) -> Result<ProjectWatchReport> {
        self.watch_observed(poll_ms, max_poll_ms, debounce_ms, max_polls, |_| {})
    }

    pub fn watch_observed(
        &self,
        poll_ms: u64,
        max_poll_ms: u64,
        debounce_ms: u64,
        max_polls: Option<usize>,
        mut observer: impl FnMut(ProjectWatchObservation),
    ) -> Result<ProjectWatchReport> {
        if poll_ms < 250 || max_poll_ms < poll_ms || debounce_ms < 100 {
            return Err(MemoryError::InvalidRequest(
                "watch requires poll_ms >= 250, max_poll_ms >= poll_ms, and debounce_ms >= 100"
                    .into(),
            ));
        }
        if !matches!(
            self.marker.project_index.auto_reindex,
            AutoReindexMode::Watch
        ) {
            return Err(MemoryError::InvalidRequest(
                "continuous project watching is disabled; configure auto_reindex=watch explicitly"
                    .into(),
            ));
        }
        create_private_directory(&self.data_dir)?;
        create_private_directory(&self.index_dir)?;
        let watcher_lock_path = self.index_dir.join("watch.lock");
        let watcher_lock = private_lock_file(&watcher_lock_path)?;
        watcher_lock.try_lock_exclusive().map_err(|error| {
            MemoryError::InvalidRequest(format!(
                "another project watcher holds {}: {error}",
                watcher_lock_path.display()
            ))
        })?;
        let mut polls = 0usize;
        let mut reindexes = 0usize;
        let mut failed_reindexes = 0usize;
        let mut failed_snapshot_reads = 0usize;
        let mut interval = poll_ms;
        let snapshot_started = Instant::now();
        let mut last_snapshot = match self.git_snapshot() {
            Ok((_, snapshot, _)) => Some(snapshot),
            Err(error) => {
                failed_snapshot_reads += 1;
                observer(ProjectWatchObservation::SnapshotFailed {
                    error_code: error.code(),
                    duration_ms: snapshot_started.elapsed().as_secs_f64() * 1_000.0,
                });
                eprintln!(
                    "project watcher kept the previous valid index after a Git snapshot error: {error}"
                );
                None
            }
        };
        loop {
            if max_polls.is_some_and(|maximum| polls >= maximum) {
                break;
            }
            thread::sleep(Duration::from_millis(interval));
            polls += 1;
            let snapshot_started = Instant::now();
            let observed = match self.git_snapshot() {
                Ok((_, snapshot, _)) => snapshot,
                Err(error) => {
                    failed_snapshot_reads += 1;
                    observer(ProjectWatchObservation::SnapshotFailed {
                        error_code: error.code(),
                        duration_ms: snapshot_started.elapsed().as_secs_f64() * 1_000.0,
                    });
                    eprintln!(
                        "project watcher kept the previous valid index after a Git snapshot error: {error}"
                    );
                    interval = (interval.saturating_mul(2)).min(max_poll_ms);
                    continue;
                }
            };
            let Some(previous_snapshot) = last_snapshot.as_ref() else {
                last_snapshot = Some(observed);
                interval = poll_ms;
                continue;
            };
            if observed == *previous_snapshot {
                interval = (interval.saturating_mul(2)).min(max_poll_ms);
                continue;
            }
            thread::sleep(Duration::from_millis(debounce_ms));
            let snapshot_started = Instant::now();
            let settled = match self.git_snapshot() {
                Ok((_, snapshot, _)) => snapshot,
                Err(error) => {
                    failed_snapshot_reads += 1;
                    observer(ProjectWatchObservation::SnapshotFailed {
                        error_code: error.code(),
                        duration_ms: snapshot_started.elapsed().as_secs_f64() * 1_000.0,
                    });
                    eprintln!(
                        "project watcher kept the previous valid index after a Git snapshot error: {error}"
                    );
                    interval = (interval.saturating_mul(2)).min(max_poll_ms);
                    continue;
                }
            };
            if settled != observed {
                last_snapshot = Some(settled);
                interval = poll_ms;
                continue;
            }
            let reindex_started = Instant::now();
            match self.index() {
                Ok(report) => {
                    reindexes += 1;
                    observer(ProjectWatchObservation::Reindexed {
                        report: Box::new(report.clone()),
                        duration_ms: reindex_started.elapsed().as_secs_f64() * 1_000.0,
                    });
                    last_snapshot = Some(report.snapshot);
                }
                Err(error) => {
                    failed_reindexes += 1;
                    observer(ProjectWatchObservation::ReindexFailed {
                        error_code: error.code(),
                        duration_ms: reindex_started.elapsed().as_secs_f64() * 1_000.0,
                    });
                    eprintln!("project watcher kept the previous valid index: {error}");
                    if retryable_watch_reindex_error(&error) {
                        interval = (interval.saturating_mul(2)).min(max_poll_ms);
                        continue;
                    }
                    last_snapshot = Some(settled);
                }
            }
            interval = poll_ms;
        }
        Ok(ProjectWatchReport {
            project_id: self.marker.project_id.clone(),
            polls,
            reindexes,
            failed_reindexes,
            failed_snapshot_reads,
            last_snapshot: last_snapshot.unwrap_or_else(|| "unavailable".into()),
        })
    }

    fn list_git_files(&self) -> Result<Vec<String>> {
        let mut arguments = vec!["ls-files", "-z", "--cached"];
        if self.marker.project_index.include_untracked {
            arguments.extend(["--others", "--exclude-standard"]);
        }
        let output = git_output(&self.root, &arguments)?;
        let mut files = output
            .split(|byte| *byte == 0)
            .filter(|path| !path.is_empty())
            .filter_map(|path| std::str::from_utf8(path).ok().map(str::to_owned))
            .collect::<Vec<_>>();
        files.sort();
        files.dedup();
        Ok(files)
    }

    fn list_untracked_files(&self) -> Result<Vec<String>> {
        let mut files = nul_paths(&git_output(
            &self.root,
            &["ls-files", "-z", "--others", "--exclude-standard"],
        )?);
        files.sort();
        files.dedup();
        Ok(files)
    }

    fn git_snapshot(&self) -> Result<(String, String, bool)> {
        let head = match git_output(&self.root, &["rev-parse", "--verify", "HEAD"]) {
            Ok(head_bytes) => String::from_utf8(head_bytes)
                .map_err(|_| MemoryError::Config("git HEAD is not UTF-8".into()))?
                .trim()
                .to_owned(),
            Err(head_error) => {
                let inside = git_output(&self.root, &["rev-parse", "--is-inside-work-tree"])?;
                if inside == b"true\n" {
                    "UNBORN".into()
                } else {
                    return Err(head_error);
                }
            }
        };
        let untracked = if self.marker.project_index.include_untracked {
            "--untracked-files=normal"
        } else {
            "--untracked-files=no"
        };
        let status = git_output(&self.root, &["status", "--porcelain=v1", "-z", untracked])?;
        let mut changed_paths = if head == "UNBORN" {
            nul_paths(&git_output(&self.root, &["ls-files", "-z", "--cached"])?)
        } else {
            nul_paths(&git_output(
                &self.root,
                &["diff", "--name-only", "-z", "HEAD", "--"],
            )?)
        };
        if self.marker.project_index.include_untracked {
            changed_paths.extend(nul_paths(&git_output(
                &self.root,
                &["ls-files", "-z", "--others", "--exclude-standard"],
            )?));
        }
        changed_paths.sort();
        changed_paths.dedup();
        let ignores = read_memoree_ignore(&self.root)?;
        let mut hasher = blake3::Hasher::new();
        for path in changed_paths {
            let control_file = matches!(path.as_str(), ".memoreeignore" | ".gitignore");
            if !safe_project_path(&path)
                || (!control_file && excluded_path(&path))
                || ignored_by_memoree(&path, &ignores)
                || (!control_file && !indexable_extension(&path))
            {
                continue;
            }
            hasher.update(path.as_bytes());
            hasher.update(&[0]);
            let absolute = self.root.join(&path);
            match fs::symlink_metadata(&absolute) {
                Ok(metadata) if metadata.file_type().is_file() => {
                    hasher.update(&metadata.len().to_le_bytes());
                    if metadata.len() <= self.marker.project_index.max_file_bytes {
                        hasher.update(&fs::read(absolute)?);
                    }
                }
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    hasher.update(b"symlink");
                    hasher.update(fs::read_link(absolute)?.as_os_str().as_encoded_bytes());
                }
                Ok(_) => {
                    hasher.update(b"non_file");
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    hasher.update(b"missing");
                }
                Err(error) => return Err(error.into()),
            }
            hasher.update(&[0xff]);
        }
        let digest = hasher.finalize().to_hex().to_string();
        Ok((
            head.clone(),
            format!("{head}:{}", &digest[..24]),
            !status.is_empty(),
        ))
    }

    fn open_database(&self) -> Result<Connection> {
        create_private_directory(&self.data_dir)?;
        create_private_directory(&self.index_dir)?;
        let connection = Connection::open(&self.database_path)?;
        #[cfg(unix)]
        fs::set_permissions(
            &self.database_path,
            std::os::unix::fs::PermissionsExt::from_mode(0o600),
        )?;
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE IF NOT EXISTS project_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
             );",
        )?;
        let version = get_meta(&connection, "schema_version")?
            .and_then(|version| version.parse::<i64>().ok());
        let indexed_snapshot = get_meta(&connection, "snapshot")?;
        let structural_policy = get_meta(&connection, "structural_policy")?;
        let grammar_revision = get_meta(&connection, "structural_grammar_revision")?;
        let compatible_projection = version == Some(PROJECT_INDEX_SCHEMA)
            && (indexed_snapshot.is_none()
                || (structural_policy.as_deref() == Some(STRUCTURAL_POLICY_VERSION)
                    && grammar_revision.as_deref() == Some(STRUCTURAL_GRAMMAR_REVISION)));
        if !compatible_projection {
            // The project index is a disposable current-source projection. An
            // incompatible schema is therefore invalidated atomically rather
            // than burdening users with a manual data migration.
            connection.execute_batch(
                "DROP TABLE IF EXISTS project_edges;
                 DROP TABLE IF EXISTS project_references;
                 DROP TABLE IF EXISTS project_symbols_fts;
                 DROP TABLE IF EXISTS project_symbols;
                 DROP TABLE IF EXISTS chunks_fts;
                 DROP TABLE IF EXISTS project_files;
                 DELETE FROM project_meta;",
            )?;
        }
        initialize_project_schema(&connection)?;
        set_meta(
            &connection,
            "schema_version",
            &PROJECT_INDEX_SCHEMA.to_string(),
        )?;
        set_meta(&connection, "structural_policy", STRUCTURAL_POLICY_VERSION)?;
        set_meta(
            &connection,
            "structural_grammar_revision",
            STRUCTURAL_GRAMMAR_REVISION,
        )?;
        Ok(connection)
    }
}

fn initialize_project_schema(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS project_files (
            path TEXT PRIMARY KEY,
            content_hash TEXT NOT NULL,
            byte_count INTEGER NOT NULL,
            structural_language TEXT,
            structural_state TEXT NOT NULL,
            structural_parse_ms REAL NOT NULL
         );
         CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
            path UNINDEXED,
            start_byte UNINDEXED,
            end_byte UNINDEXED,
            start_line UNINDEXED,
            end_line UNINDEXED,
            content,
            tokenize = 'unicode61 remove_diacritics 2 tokenchars ''_-'''
         );
         CREATE TABLE IF NOT EXISTS project_symbols (
            symbol_key TEXT PRIMARY KEY,
            path TEXT NOT NULL,
            name TEXT NOT NULL,
            kind TEXT NOT NULL,
            qualified_name TEXT NOT NULL,
            parent_key TEXT,
            start_byte INTEGER NOT NULL,
            end_byte INTEGER NOT NULL,
            start_line INTEGER NOT NULL,
            end_line INTEGER NOT NULL,
            is_test INTEGER NOT NULL CHECK(is_test IN (0, 1))
         );
         CREATE INDEX IF NOT EXISTS idx_project_symbols_path
            ON project_symbols(path);
         CREATE INDEX IF NOT EXISTS idx_project_symbols_name
            ON project_symbols(name COLLATE NOCASE);
         CREATE VIRTUAL TABLE IF NOT EXISTS project_symbols_fts USING fts5(
            symbol_key UNINDEXED,
            path UNINDEXED,
            name,
            qualified_name,
            kind,
            search_text,
            tokenize = 'unicode61 remove_diacritics 2 tokenchars ''_-'''
         );
         CREATE TABLE IF NOT EXISTS project_references (
            reference_id INTEGER PRIMARY KEY,
            source_path TEXT NOT NULL,
            source_key TEXT NOT NULL,
            target_name TEXT NOT NULL,
            kind TEXT NOT NULL,
            start_byte INTEGER NOT NULL,
            end_byte INTEGER NOT NULL
         );
         CREATE INDEX IF NOT EXISTS idx_project_references_source_path
            ON project_references(source_path);
         CREATE INDEX IF NOT EXISTS idx_project_references_target
            ON project_references(target_name COLLATE NOCASE);
         CREATE TABLE IF NOT EXISTS project_edges (
            edge_id INTEGER PRIMARY KEY,
            source_key TEXT NOT NULL,
            target_key TEXT NOT NULL,
            target_name TEXT NOT NULL,
            kind TEXT NOT NULL,
            confidence TEXT NOT NULL,
            source_path TEXT NOT NULL,
            start_byte INTEGER NOT NULL,
            end_byte INTEGER NOT NULL,
            UNIQUE(source_key, target_key, kind, start_byte, end_byte)
         );
         CREATE INDEX IF NOT EXISTS idx_project_edges_source
            ON project_edges(source_key);
         CREATE INDEX IF NOT EXISTS idx_project_edges_target
            ON project_edges(target_key);",
    )?;
    Ok(())
}

fn rebuild_project_edges(connection: &Connection) -> Result<()> {
    connection.execute("DELETE FROM project_edges", [])?;
    connection.execute(
        "INSERT INTO project_edges(
            source_key, target_key, target_name, kind, confidence,
            source_path, start_byte, end_byte
         )
         SELECT parent_key, symbol_key, name, ?1, ?2, path, start_byte, end_byte
           FROM project_symbols
          WHERE parent_key IS NOT NULL",
        params![
            StructuralEdgeKind::Contains.as_str(),
            StructuralConfidence::Extracted.as_str()
        ],
    )?;
    connection.execute(
        "WITH resolved AS (
            SELECT r.source_key,
                   candidate.symbol_key AS target_key,
                   candidate.name AS target_name,
                   r.kind,
                   r.source_path,
                   r.start_byte,
                   r.end_byte,
                   CASE
                     WHEN EXISTS (
                        SELECT 1 FROM project_symbols local
                         WHERE local.path = r.source_path
                           AND local.name = r.target_name COLLATE NOCASE
                     ) THEN (
                        SELECT COUNT(*) FROM project_symbols local
                         WHERE local.path = r.source_path
                           AND local.name = r.target_name COLLATE NOCASE
                     )
                     ELSE (
                        SELECT COUNT(*) FROM project_symbols global
                         WHERE global.name = r.target_name COLLATE NOCASE
                     )
                   END AS candidate_count
              FROM project_references r
              JOIN project_symbols candidate
                ON candidate.name = r.target_name COLLATE NOCASE
             WHERE (
                    candidate.path = r.source_path
                    AND EXISTS (
                        SELECT 1 FROM project_symbols local
                         WHERE local.path = r.source_path
                           AND local.name = r.target_name COLLATE NOCASE
                    )
                   )
                OR (
                    NOT EXISTS (
                        SELECT 1 FROM project_symbols local
                         WHERE local.path = r.source_path
                           AND local.name = r.target_name COLLATE NOCASE
                    )
                   )
         )
         INSERT OR IGNORE INTO project_edges(
            source_key, target_key, target_name, kind, confidence,
            source_path, start_byte, end_byte
         )
         SELECT source_key, target_key, target_name, kind,
                CASE WHEN candidate_count = 1 THEN ?1 ELSE ?2 END,
                source_path, start_byte, end_byte
           FROM resolved",
        params![
            StructuralConfidence::Inferred.as_str(),
            StructuralConfidence::Ambiguous.as_str()
        ],
    )?;
    connection.execute(
        "INSERT OR IGNORE INTO project_edges(
            source_key, target_key, target_name, kind, confidence,
            source_path, start_byte, end_byte
         )
         SELECT edge.target_key, edge.source_key, source.name, ?1, edge.confidence,
                edge.source_path, edge.start_byte, edge.end_byte
           FROM project_edges edge
           JOIN project_symbols source ON source.symbol_key = edge.source_key
          WHERE edge.kind = ?2 AND source.is_test = 1",
        params![
            StructuralEdgeKind::TestedBy.as_str(),
            StructuralEdgeKind::Calls.as_str()
        ],
    )?;
    Ok(())
}

fn count_where(connection: &Connection, sql: &str) -> Result<usize> {
    Ok(connection.query_row(sql, [], |row| row.get::<_, i64>(0))? as usize)
}

fn optional_count(connection: Option<&Connection>, sql: &str) -> Result<usize> {
    connection
        .map(|connection| count_where(connection, sql))
        .transpose()
        .map(|value| value.unwrap_or(0))
}

#[derive(Debug)]
struct ParsedCitation {
    project_id: String,
    path: String,
    hash: String,
    start: usize,
    end: usize,
}

fn project_citation(project_id: &str, path: &str, hash: &str, start: usize, end: usize) -> String {
    let encoded_path = URL_SAFE_NO_PAD.encode(path.as_bytes());
    format!("memoree-project://{project_id}/{encoded_path}@{hash}#{start}-{end}")
}

fn parse_project_citation(citation: &str) -> Result<ParsedCitation> {
    let tail = citation
        .strip_prefix("memoree-project://")
        .ok_or_else(|| MemoryError::InvalidRequest("invalid project citation scheme".into()))?;
    let (project_id, tail) = tail
        .split_once('/')
        .ok_or_else(|| MemoryError::InvalidRequest("invalid project citation project".into()))?;
    let (path_and_hash, span) = tail.split_once('#').ok_or_else(|| {
        MemoryError::InvalidRequest("project citation requires a byte range".into())
    })?;
    let (encoded_path, hash) = path_and_hash
        .rsplit_once('@')
        .ok_or_else(|| MemoryError::InvalidRequest("project citation requires a hash".into()))?;
    let path = String::from_utf8(
        URL_SAFE_NO_PAD
            .decode(encoded_path)
            .map_err(|_| MemoryError::InvalidRequest("invalid project citation path".into()))?,
    )
    .map_err(|_| MemoryError::InvalidRequest("project citation path is not UTF-8".into()))?;
    let (start, end) = span
        .split_once('-')
        .ok_or_else(|| MemoryError::InvalidRequest("invalid project citation byte range".into()))?;
    Ok(ParsedCitation {
        project_id: project_id.into(),
        path,
        hash: hash.into(),
        start: start
            .parse()
            .map_err(|_| MemoryError::InvalidRequest("invalid project citation start".into()))?,
        end: end
            .parse()
            .map_err(|_| MemoryError::InvalidRequest("invalid project citation end".into()))?,
    })
}

fn existing_file_hashes(connection: &Connection) -> Result<BTreeMap<String, String>> {
    let mut statement = connection.prepare("SELECT path, content_hash FROM project_files")?;
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

fn set_meta(connection: &Connection, key: &str, value: &str) -> Result<()> {
    connection.execute(
        "INSERT INTO project_meta(key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value],
    )?;
    Ok(())
}

fn get_meta(connection: &Connection, key: &str) -> Result<Option<String>> {
    Ok(connection
        .query_row(
            "SELECT value FROM project_meta WHERE key = ?1",
            [key],
            |row| row.get(0),
        )
        .optional()?)
}

fn git_output(root: &Path, arguments: &[&str]) -> Result<Vec<u8>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(arguments)
        .output()?;
    if !output.status.success() {
        return Err(MemoryError::Config(format!(
            "git {} failed in {}: {}",
            arguments.join(" "),
            root.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(output.stdout)
}

fn nul_paths(output: &[u8]) -> Vec<String> {
    output
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .filter_map(|path| std::str::from_utf8(path).ok().map(str::to_owned))
        .collect()
}

fn chunk_spans(text: &str) -> Vec<(usize, usize)> {
    if text.is_empty() {
        return vec![];
    }
    let mut spans = Vec::new();
    let mut start = 0usize;
    while start < text.len() {
        let mut end = (start + CHUNK_BYTES).min(text.len());
        while end > start && !text.is_char_boundary(end) {
            end -= 1;
        }
        if end < text.len()
            && let Some(newline) = text[start..end].rfind('\n')
            && newline + 1 >= CHUNK_BYTES / 2
        {
            end = start + newline + 1;
        }
        if end <= start {
            break;
        }
        spans.push((start, end));
        if end == text.len() {
            break;
        }
        let mut next = end.saturating_sub(CHUNK_OVERLAP_BYTES);
        while next < end && !text.is_char_boundary(next) {
            next += 1;
        }
        start = next.max(start + 1);
        while start < text.len() && !text.is_char_boundary(start) {
            start += 1;
        }
    }
    spans
}

fn fts_expression(query: &str) -> Result<String> {
    let mut tokens = query
        .split(|character: char| {
            !character.is_alphanumeric() && character != '_' && character != '-'
        })
        .filter(|token| !token.is_empty())
        .map(|token| token.to_lowercase())
        .collect::<Vec<_>>();
    tokens.sort();
    tokens.dedup();
    if tokens.is_empty() || tokens.len() > 32 {
        return Err(MemoryError::InvalidRequest(
            "project search requires between 1 and 32 words or identifiers".into(),
        ));
    }
    Ok(tokens
        .iter()
        .map(|token| format!("\"{}\"", token.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" OR "))
}

fn project_map_terms(query: &str) -> Vec<String> {
    const STOP_WORDS: &[&str] = &[
        "a",
        "affected",
        "an",
        "and",
        "are",
        "call",
        "called",
        "caller",
        "callers",
        "calls",
        "class",
        "code",
        "cover",
        "covered",
        "coverage",
        "defined",
        "definition",
        "does",
        "find",
        "for",
        "from",
        "function",
        "how",
        "impact",
        "import",
        "imports",
        "in",
        "is",
        "it",
        "me",
        "method",
        "of",
        "on",
        "show",
        "spec",
        "test",
        "tests",
        "the",
        "this",
        "to",
        "used",
        "uses",
        "what",
        "where",
        "which",
        "who",
        "with",
    ];
    let raw_tokens = query
        .split(|character: char| {
            !character.is_alphanumeric() && character != '_' && character != '-'
        })
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();
    let mut tokens = raw_tokens
        .iter()
        .flat_map(|token| {
            let mut expanded = identifier_words(token);
            expanded.push(token.to_lowercase());
            let aliases = expanded
                .iter()
                .flat_map(|word| project_query_aliases(word).iter().copied())
                .map(str::to_owned)
                .collect::<Vec<_>>();
            expanded.extend(aliases);
            expanded
        })
        .filter(|token| !STOP_WORDS.contains(&token.as_str()))
        .collect::<Vec<_>>();
    tokens.sort();
    tokens.dedup();
    tokens
}

fn project_map_explicit_terms(query: &str) -> Vec<String> {
    let mut terms = query
        .split(|character: char| {
            !character.is_alphanumeric() && character != '_' && character != '-'
        })
        .filter(|token| !token.is_empty())
        .map(str::to_lowercase)
        .filter(|token| !project_map_explicit_stop_word(token))
        .collect::<Vec<_>>();
    terms.sort();
    terms.dedup();
    terms
}

fn project_map_explicit_stop_word(token: &str) -> bool {
    matches!(
        token,
        "a" | "all"
            | "an"
            | "and"
            | "are"
            | "call"
            | "called"
            | "caller"
            | "callers"
            | "calls"
            | "change"
            | "changed"
            | "code"
            | "cover"
            | "coverage"
            | "direct"
            | "does"
            | "every"
            | "explain"
            | "find"
            | "for"
            | "from"
            | "function"
            | "how"
            | "impact"
            | "in"
            | "is"
            | "it"
            | "list"
            | "method"
            | "of"
            | "on"
            | "show"
            | "state"
            | "test"
            | "tests"
            | "the"
            | "then"
            | "to"
            | "trace"
            | "used"
            | "uses"
            | "what"
            | "where"
            | "which"
            | "who"
            | "with"
            | "would"
    )
}

fn project_query_aliases(word: &str) -> &'static [&'static str] {
    match word {
        "authentication" | "authenticated" => &["auth", "authenticate"],
        "compilation" | "compiled" => &["compile"],
        "configuration" | "configured" => &["config", "configure"],
        "creation" | "created" => &["create"],
        "deletion" | "deleted" => &["delete"],
        "implementation" | "implemented" => &["implement"],
        "indexing" | "indexed" => &["index"],
        "initialization" | "initialized" => &["init", "initialize"],
        "migration" | "migrated" => &["migrate"],
        "reconciliation" | "reconciled" => &["reconcile"],
        "resolution" | "resolved" => &["resolve"],
        "retrieval" | "retrieved" | "retrieving" => &["retrieve"],
        "serialization" | "serialized" => &["serialize"],
        "synchronization" | "synchronized" => &["sync", "synchronize"],
        "updating" | "updated" => &["update"],
        "validation" | "validated" => &["validate"],
        "withdrawal" | "withdrawn" => &["withdraw"],
        _ => &[],
    }
}

fn symbol_search_text(name: &str, qualified_name: &str, kind: &str, path: &str) -> String {
    let mut values = vec![
        name.to_owned(),
        qualified_name.to_owned(),
        kind.to_owned(),
        path.to_owned(),
    ];
    for value in [name, qualified_name, path] {
        values.extend(identifier_words(value));
    }
    values.join(" ")
}

fn identifier_words(value: &str) -> Vec<String> {
    let characters = value.chars().collect::<Vec<_>>();
    let mut words = Vec::new();
    let mut start = 0usize;
    let push = |words: &mut Vec<String>, slice: &[char]| {
        let word = slice.iter().collect::<String>().to_lowercase();
        if !word.is_empty() {
            words.push(word);
        }
    };
    for index in 0..characters.len() {
        let character = characters[index];
        if !character.is_alphanumeric() {
            push(&mut words, &characters[start..index]);
            start = index + 1;
            continue;
        }
        if index > start {
            let previous = characters[index - 1];
            let next = characters.get(index + 1).copied();
            let lower_to_upper = previous.is_lowercase() && character.is_uppercase();
            let acronym_boundary = previous.is_uppercase()
                && character.is_uppercase()
                && next.is_some_and(char::is_lowercase);
            let alpha_numeric_boundary = previous.is_alphabetic() != character.is_alphabetic();
            if lower_to_upper || acronym_boundary || alpha_numeric_boundary {
                push(&mut words, &characters[start..index]);
                start = index;
            }
        }
    }
    push(&mut words, &characters[start..]);
    words.sort();
    words.dedup();
    words
}

fn classify_call_form(expression: &str, target_name: &str) -> Option<TraversalCallForm> {
    let (target_start, _) = expression
        .match_indices(target_name)
        .find(|(start, _)| exact_identifier_occurrence(expression, *start, target_name.len()))?;
    let before = expression[..target_start].trim_end();
    if before.is_empty() {
        return Some(TraversalCallForm::Free);
    }
    if let Some(prefix) = before.strip_suffix("::") {
        let qualifier = trailing_identifier(prefix.trim_end())?.to_lowercase();
        return Some(TraversalCallForm::Static { qualifier });
    }
    if let Some(prefix) = before.strip_suffix('.') {
        let prefix = prefix
            .trim_end()
            .strip_suffix('?')
            .unwrap_or(prefix.trim_end());
        let receiver = trailing_identifier(prefix)?.to_lowercase();
        let receiver_start = prefix.len().checked_sub(receiver.len())?;
        if !prefix[..receiver_start].trim().is_empty() {
            return None;
        }
        return Some(TraversalCallForm::Receiver { receiver });
    }
    None
}

fn trailing_identifier(value: &str) -> Option<String> {
    let end = value.len();
    let start = value
        .char_indices()
        .rev()
        .find_map(|(index, character)| {
            (!character.is_alphanumeric() && character != '_')
                .then_some(index + character.len_utf8())
        })
        .unwrap_or(0);
    let identifier = value.get(start..end)?;
    (!identifier.is_empty()).then(|| identifier.to_owned())
}

fn receiver_matches_owner(receiver: &str, owner_words: &[String]) -> bool {
    let receiver_words = identifier_words(receiver);
    !receiver_words.is_empty()
        && receiver_words
            .iter()
            .all(|word| owner_words.iter().any(|owner| owner == word))
}

fn rust_impl_owner(prefix: &str) -> Option<String> {
    let mut search_end = prefix.len();
    while let Some(relative) = prefix[..search_end].rfind("impl") {
        let before_ok = relative == 0
            || !prefix[..relative]
                .chars()
                .next_back()
                .is_some_and(|character| character.is_alphanumeric() || character == '_');
        let after = relative + "impl".len();
        let after_ok = prefix[after..]
            .chars()
            .next()
            .is_some_and(|character| character.is_whitespace() || character == '<');
        if before_ok && after_ok {
            let rest = &prefix[after..];
            let header_end = rest.find('{')?;
            let mut header = rest[..header_end].trim();
            if header.starts_with('<') {
                let mut depth = 0usize;
                let mut generic_end = None;
                for (index, character) in header.char_indices() {
                    match character {
                        '<' => depth += 1,
                        '>' => {
                            depth = depth.saturating_sub(1);
                            if depth == 0 {
                                generic_end = Some(index + 1);
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                header = header.get(generic_end?..)?.trim_start();
            }
            if let Some((_, concrete)) = header.rsplit_once(" for ") {
                header = concrete.trim_start();
            }
            header = header.split(" where ").next().unwrap_or(header).trim();
            let type_expression = header.split_whitespace().next()?;
            let before_generics = type_expression.split('<').next().unwrap_or(type_expression);
            let owner = trailing_identifier(before_generics.trim_end_matches(':'))?;
            return Some(owner);
        }
        search_end = relative;
    }
    None
}

fn project_map_expression(query: &str) -> Result<String> {
    let mut tokens = project_map_terms(query);
    if tokens.is_empty() {
        return fts_expression(query);
    }
    if tokens.len() > 32 {
        tokens.truncate(32);
    }
    Ok(tokens
        .iter()
        .map(|token| format!("\"{}\"", token.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(" OR "))
}

fn project_map_mode(query: &str) -> String {
    let lower = query.to_ascii_lowercase();
    let query_words = identifier_words(&lower)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let wants_tests = ["test", "tests", "spec", "coverage"]
        .iter()
        .any(|token| query_words.contains(*token));
    let wants_callers = [
        "caller",
        "called by",
        "used by",
        "dependents",
        "call site",
        "call-site",
        "what calls",
        "which calls",
        "directly call",
        "symbols call",
        "functions call",
        "methods call",
    ]
    .iter()
    .any(|token| lower.contains(token));
    let wants_callees = [
        "callee",
        "calls made",
        "functions it calls",
        "it calls",
        "invokes",
        "invoked by",
        "depends on",
        "dependencies",
        "direct path",
        "immediate path",
        "helpers it uses",
    ]
    .iter()
    .any(|token| lower.contains(token));
    let wants_calls = wants_callers
        || wants_callees
        || ["call", "invoke"].iter().any(|token| lower.contains(token));
    let wants_impact = ["impact", "affected", "dependents", "used by", "references"]
        .iter()
        .any(|token| lower.contains(token));
    let wants_imports = ["import", "module", "package"]
        .iter()
        .any(|token| lower.contains(token));
    let wants_definition = [
        "define",
        "definition",
        "implement",
        "class",
        "function",
        "method",
    ]
    .iter()
    .any(|token| lower.contains(token));
    let mut modes = Vec::new();
    if wants_callers {
        modes.push("callers");
    }
    if wants_callees {
        modes.push("callees");
    }
    if wants_calls && !wants_callers && !wants_callees {
        modes.push("calls");
    }
    if wants_tests {
        modes.push("tests");
    }
    if wants_impact && !wants_callers {
        modes.push("impact");
    }
    if wants_imports {
        modes.push("imports");
    }
    if wants_definition && modes.is_empty() {
        modes.push("definition");
    }
    if modes.is_empty() {
        modes.push("mixed");
    }
    modes.join("+")
}

fn project_map_limits() -> Vec<String> {
    [
        "dynamic_dispatch",
        "macros",
        "runtime_registration",
        "non_structural_test_surfaces",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn mark_indexed_facet_incomplete(
    completeness: &mut ProjectMapFacetCompleteness,
    edge: &IndexedMapEdge,
) {
    if edge.kind == StructuralEdgeKind::TestedBy.as_str() {
        completeness.direct_tests = false;
    } else if edge.kind == "behavioral_test_lead" {
        completeness.behavioral_test_leads = false;
    } else if edge.kind == StructuralEdgeKind::Calls.as_str() && edge.direction == "incoming" {
        if edge.related_is_test {
            completeness.direct_tests = false;
        } else {
            completeness.direct_callers = false;
        }
    } else if edge.kind == StructuralEdgeKind::Calls.as_str() && edge.direction == "outgoing" {
        completeness.direct_callees = false;
    }
}

fn project_test_surface(path: &str) -> &'static str {
    let lower = path.to_ascii_lowercase();
    if lower.starts_with("scripts/") || lower.ends_with(".sh") {
        "script"
    } else if lower.contains("eval") || lower.starts_with("src/bin/") {
        "harness"
    } else if lower.starts_with("tests/") || lower.contains("/tests/") {
        "integration"
    } else {
        "unit"
    }
}

fn project_map_facets(
    query_mode: &str,
    edges: &[ProjectMapEdge],
    completeness: &ProjectMapFacetCompleteness,
) -> ProjectMapFacets {
    let intents = query_mode.split('+').collect::<BTreeSet<_>>();
    let mixed = intents.contains("mixed");
    let calls = intents.contains("calls");
    let callers_requested = mixed || calls || intents.contains("callers");
    let callees_requested = mixed || calls || intents.contains("callees");
    let tests_requested = mixed || intents.contains("tests");
    let count = |predicate: fn(&ProjectMapEdge) -> bool| {
        edges.iter().filter(|edge| predicate(edge)).count()
    };
    let summary = |requested: bool, complete: bool, returned: usize| ProjectMapFacetSummary {
        state: if !requested {
            "not_requested"
        } else if complete {
            "complete_in_projection"
        } else {
            "incomplete"
        }
        .into(),
        returned,
    };
    ProjectMapFacets {
        definition: summary(true, true, 1),
        direct_callers: summary(
            callers_requested,
            completeness.direct_callers,
            count(|edge| {
                edge.kind == StructuralEdgeKind::Calls.as_str()
                    && edge.direction == "incoming"
                    && !edge.related_is_test
            }),
        ),
        direct_callees: summary(
            callees_requested,
            completeness.direct_callees,
            count(|edge| {
                edge.kind == StructuralEdgeKind::Calls.as_str()
                    && edge.direction == "outgoing"
                    && !edge.related_is_test
            }),
        ),
        direct_tests: summary(
            tests_requested,
            completeness.direct_tests,
            count(|edge| edge.kind == StructuralEdgeKind::TestedBy.as_str()),
        ),
        behavioral_test_leads: summary(
            tests_requested,
            completeness.behavioral_test_leads,
            count(|edge| edge.kind == "behavioral_test_lead"),
        ),
    }
}

fn mark_public_facet_incomplete(facets: &mut ProjectMapFacets, edge: &ProjectMapEdge) {
    let summary = if edge.kind == StructuralEdgeKind::TestedBy.as_str() {
        &mut facets.direct_tests
    } else if edge.kind == "behavioral_test_lead" {
        &mut facets.behavioral_test_leads
    } else if edge.kind == StructuralEdgeKind::Calls.as_str() && edge.direction == "incoming" {
        if edge.related_is_test {
            &mut facets.direct_tests
        } else {
            &mut facets.direct_callers
        }
    } else if edge.kind == StructuralEdgeKind::Calls.as_str() && edge.direction == "outgoing" {
        &mut facets.direct_callees
    } else {
        return;
    };
    if summary.state != "not_requested" {
        summary.state = "incomplete".into();
    }
}

fn refresh_public_facet_counts(facets: &mut ProjectMapFacets, edges: &[ProjectMapEdge]) {
    facets.direct_callers.returned = edges
        .iter()
        .filter(|edge| {
            edge.kind == StructuralEdgeKind::Calls.as_str()
                && edge.direction == "incoming"
                && !edge.related_is_test
        })
        .count();
    facets.direct_callees.returned = edges
        .iter()
        .filter(|edge| {
            edge.kind == StructuralEdgeKind::Calls.as_str()
                && edge.direction == "outgoing"
                && !edge.related_is_test
        })
        .count();
    facets.direct_tests.returned = edges
        .iter()
        .filter(|edge| edge.kind == StructuralEdgeKind::TestedBy.as_str())
        .count();
    facets.behavioral_test_leads.returned = edges
        .iter()
        .filter(|edge| edge.kind == "behavioral_test_lead")
        .count();
}

fn project_map_is_relation_focused(mode: &str) -> bool {
    ["callers", "callees", "calls", "tests", "impact", "imports"]
        .iter()
        .any(|intent| mode.split('+').any(|part| part == *intent))
}

fn exact_identifier_occurrence(text: &str, start: usize, length: usize) -> bool {
    let identifier_character = |character: char| character.is_alphanumeric() || character == '_';
    let before = text[..start].chars().next_back();
    let after = text[start + length..].chars().next();
    !before.is_some_and(identifier_character) && !after.is_some_and(identifier_character)
}

fn project_map_edge_priority(mode: &str, edge: &IndexedMapEdge) -> Option<(u8, u8)> {
    let intents = mode.split('+').collect::<BTreeSet<_>>();
    let priority = if intents.contains("callers")
        && edge.kind == StructuralEdgeKind::Calls.as_str()
        && edge.direction == "incoming"
        && !edge.related_is_test
    {
        0
    } else if intents.contains("tests") && edge.kind == StructuralEdgeKind::TestedBy.as_str() {
        1
    } else if intents.contains("callers")
        && edge.kind == StructuralEdgeKind::Calls.as_str()
        && edge.direction == "incoming"
    {
        2
    } else if intents.contains("tests")
        && edge.kind == StructuralEdgeKind::Calls.as_str()
        && edge.direction == "incoming"
        && edge.related_is_test
    {
        3
    } else if intents.contains("tests") && edge.kind == "behavioral_test_lead" {
        4
    } else if intents.contains("callees")
        && edge.kind == StructuralEdgeKind::Calls.as_str()
        && edge.direction == "outgoing"
    {
        0
    } else if intents.contains("calls") && edge.kind == StructuralEdgeKind::Calls.as_str() {
        if edge.direction == "incoming" { 0 } else { 1 }
    } else if (intents.contains("impact")
        && edge.direction == "incoming"
        && matches!(edge.kind.as_str(), "calls" | "imports" | "inherits"))
        || (intents.contains("imports") && edge.kind == StructuralEdgeKind::Imports.as_str())
        || (intents.contains("definition") && edge.kind == StructuralEdgeKind::Contains.as_str())
    {
        0
    } else if intents.contains("mixed") {
        10
    } else {
        return None;
    };
    let confidence = match edge.confidence.as_str() {
        "extracted" => 0,
        "inferred" => 1,
        _ => 2,
    };
    Some((priority, confidence))
}

fn enforce_project_map_budget(report: &mut ProjectMapReport) -> Result<()> {
    let serialized_len = |report: &ProjectMapReport| -> Result<usize> {
        Ok(serde_json::to_vec(report)
            .map_err(|error| MemoryError::Integrity(error.to_string()))?
            .len())
    };
    while serialized_len(report)? > report.max_bytes && !report.text_fallback.is_empty() {
        report.text_fallback.pop();
        report.truncated = true;
    }
    // Keep a small set of strong anchors and preserve their structural edges;
    // a wide list of isolated definitions recreates grep fan-out.
    while serialized_len(report)? > report.max_bytes && report.leads.len() > 5 {
        report.leads.pop();
        report.truncated = true;
    }
    if project_map_is_relation_focused(&report.query_mode) {
        while serialized_len(report)? > report.max_bytes && !report.alternatives.is_empty() {
            report.alternatives.pop();
            report.truncated = true;
        }
        while serialized_len(report)? > report.max_bytes && shrink_project_map_lead_excerpt(report)?
        {
        }
    }
    while serialized_len(report)? > report.max_bytes && !report.lexical_residue.is_empty() {
        report.lexical_residue.pop();
        report.mentions_truncated = true;
        report.truncated = true;
    }
    while serialized_len(report)? > report.max_bytes {
        let mut removed = false;
        let has_test_intent = report.query_mode.split('+').any(|intent| intent == "tests");
        for lead in report.leads.iter_mut().rev() {
            if let Some(edge) = lead.edges.pop() {
                lead.edges_truncated = true;
                lead.test_leads_truncated |= has_test_intent;
                mark_public_facet_incomplete(&mut lead.facets, &edge);
                refresh_public_facet_counts(&mut lead.facets, &lead.edges);
                report.truncated = true;
                removed = true;
                break;
            }
        }
        if !removed {
            break;
        }
    }
    while serialized_len(report)? > report.max_bytes && !report.alternatives.is_empty() {
        report.alternatives.pop();
        report.truncated = true;
    }
    while serialized_len(report)? > report.max_bytes && report.leads.len() > 1 {
        report.leads.pop();
        report.truncated = true;
    }
    while serialized_len(report)? > report.max_bytes && shrink_project_map_lead_excerpt(report)? {}
    if serialized_len(report)? > report.max_bytes {
        return Err(MemoryError::InvalidRequest(format!(
            "project map metadata cannot fit within max_bytes {}; increase the budget",
            report.max_bytes
        )));
    }
    Ok(())
}

fn retryable_watch_reindex_error(error: &MemoryError) -> bool {
    matches!(error, MemoryError::Io(_) | MemoryError::Database(_))
        || matches!(
            error,
            MemoryError::InvalidRequest(message)
                if message.starts_with("another project index operation holds ")
        )
}

fn shrink_project_map_lead_excerpt(report: &mut ProjectMapReport) -> Result<bool> {
    let Some(lead) = report
        .leads
        .iter_mut()
        .rev()
        .find(|lead| lead.excerpt.len() > 128)
    else {
        return Ok(false);
    };
    let target = (lead.excerpt.len() / 2).max(128);
    let mut end = target.min(lead.excerpt.len());
    while end > 0 && !lead.excerpt.is_char_boundary(end) {
        end -= 1;
    }
    lead.excerpt.truncate(end);
    lead.excerpt_truncated = true;
    let parsed = parse_project_citation(&lead.citation)?;
    lead.citation = project_citation(
        &parsed.project_id,
        &parsed.path,
        &parsed.hash,
        parsed.start,
        parsed.start + end,
    );
    report.truncated = true;
    Ok(true)
}

fn safe_project_path(path: &str) -> bool {
    let path = Path::new(path);
    !path.is_absolute()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn excluded_path(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    let components = lower.split('/').collect::<Vec<_>>();
    components.iter().any(|component| {
        matches!(
            *component,
            ".git"
                | ".env"
                | ".aws"
                | ".ssh"
                | "node_modules"
                | "target"
                | "vendor"
                | "dist"
                | "build"
                | "coverage"
                | "credentials"
                | "secrets"
        ) || component.ends_with(".pem")
            || component.ends_with(".key")
            || component.ends_with(".p12")
            || component.ends_with(".pfx")
            || component.ends_with(".keystore")
    }) || sensitive_data_filename(&lower)
        || lower == MARKER_FILE
        || lower.ends_with(".min.js")
        || lower.ends_with(".min.css")
        || lower.ends_with(".map")
}

fn sensitive_data_filename(path: &str) -> bool {
    let file_name = Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    if file_name == ".env"
        || file_name.starts_with(".env.")
        || matches!(
            file_name,
            ".npmrc"
                | ".pypirc"
                | ".netrc"
                | ".authinfo"
                | ".git-credentials"
                | "id_rsa"
                | "id_ed25519"
                | "kubeconfig"
        )
    {
        return true;
    }
    let Some(extension) = Path::new(file_name)
        .extension()
        .and_then(|value| value.to_str())
    else {
        return false;
    };
    if !matches!(
        extension,
        "json"
            | "json5"
            | "yaml"
            | "yml"
            | "toml"
            | "ini"
            | "cfg"
            | "conf"
            | "config"
            | "properties"
            | "xml"
    ) {
        return false;
    }
    let stem = Path::new(file_name)
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect::<String>();
    matches!(
        stem.as_str(),
        "credential"
            | "credentials"
            | "secret"
            | "secrets"
            | "clientsecret"
            | "awskeys"
            | "apikey"
            | "apikeys"
            | "privatekey"
            | "serviceaccount"
    )
}

fn indexable_extension(path: &str) -> bool {
    let file_name = Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if matches!(
        file_name.as_str(),
        "dockerfile"
            | ".dockerignore"
            | ".gitignore"
            | "makefile"
            | "justfile"
            | "procfile"
            | "gemfile"
            | "rakefile"
            | "license"
            | "readme"
    ) {
        return true;
    }
    matches!(
        Path::new(path)
            .extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or_default()
            .to_ascii_lowercase()
            .as_str(),
        "rs" | "toml"
            | "md"
            | "mdx"
            | "txt"
            | "json"
            | "jsonl"
            | "yaml"
            | "yml"
            | "xml"
            | "html"
            | "astro"
            | "css"
            | "scss"
            | "sass"
            | "less"
            | "js"
            | "jsx"
            | "ts"
            | "tsx"
            | "mjs"
            | "cjs"
            | "vue"
            | "svelte"
            | "py"
            | "pyi"
            | "rb"
            | "go"
            | "java"
            | "kt"
            | "kts"
            | "swift"
            | "c"
            | "h"
            | "cc"
            | "cpp"
            | "cxx"
            | "hpp"
            | "cs"
            | "fs"
            | "fsx"
            | "php"
            | "sh"
            | "bash"
            | "zsh"
            | "fish"
            | "ps1"
            | "sql"
            | "graphql"
            | "gql"
            | "proto"
            | "conf"
            | "lock"
            | "tf"
            | "hcl"
            | "nix"
            | "ex"
            | "exs"
            | "erl"
            | "hrl"
            | "clj"
            | "cljs"
            | "scala"
            | "lua"
            | "r"
            | "dart"
    )
}

fn project_path_extension(path: &str) -> String {
    Path::new(path)
        .extension()
        .and_then(|extension| extension.to_str())
        .filter(|extension| !extension.is_empty())
        .map(|extension| extension.to_ascii_lowercase())
        .unwrap_or_else(|| "<none>".into())
}

fn read_memoree_ignore(root: &Path) -> Result<Vec<String>> {
    let path = root.join(".memoreeignore");
    let source = match fs::read_to_string(path) {
        Ok(source) => source,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
        Err(error) => return Err(error.into()),
    };
    let mut patterns = Vec::new();
    for line in source.lines() {
        let pattern = line.trim();
        if pattern.is_empty() || pattern.starts_with('#') {
            continue;
        }
        if pattern.starts_with('!') {
            return Err(MemoryError::Config(
                ".memoreeignore negation is not supported; use positive exclusion patterns only"
                    .into(),
            ));
        }
        patterns.push(pattern.trim_start_matches('/').to_owned());
    }
    Ok(patterns)
}

fn ignored_by_memoree(path: &str, patterns: &[String]) -> bool {
    patterns.iter().any(|pattern| {
        let pattern = pattern.trim_end_matches('/');
        if let Some(suffix) = pattern.strip_prefix("*.") {
            path.ends_with(&format!(".{suffix}"))
        } else if pattern.contains('*') {
            let parts = pattern
                .split('*')
                .filter(|part| !part.is_empty())
                .collect::<Vec<_>>();
            let mut remainder = path;
            parts.iter().all(|part| {
                if let Some(index) = remainder.find(part) {
                    remainder = &remainder[index + part.len()..];
                    true
                } else {
                    false
                }
            })
        } else {
            path == pattern
                || path.starts_with(&format!("{pattern}/"))
                || path.split('/').any(|component| component == pattern)
        }
    })
}

fn create_private_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    fs::set_permissions(path, std::os::unix::fs::PermissionsExt::from_mode(0o700))?;
    Ok(())
}

fn private_lock_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    Ok(options.open(path)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::init_marker;

    fn git(root: &Path, args: &[&str]) {
        let status = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .status()
            .unwrap();
        assert!(status.success());
    }

    #[test]
    fn project_index_search_get_and_stale_detection_are_exact() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("repo");
        let data = temporary.path().join("data");
        fs::create_dir(&root).unwrap();
        git(&root, &["init", "-q"]);
        git(&root, &["config", "user.email", "test@example.invalid"]);
        git(&root, &["config", "user.name", "Test"]);
        init_marker(&root, "fixture", None).unwrap();
        fs::write(
            root.join("lib.rs"),
            "pub fn historical_packet() { /* PROJECT_INDEX_SENTINEL_42 */ }\n",
        )
        .unwrap();
        fs::create_dir(root.join("credentials")).unwrap();
        fs::write(
            root.join("credentials/token.txt"),
            "SECRET_TOKEN_MUST_NOT_INDEX\n",
        )
        .unwrap();
        fs::write(
            root.join("credentials.json"),
            "{\"token\":\"SECRET_DATA_FILENAME_MUST_NOT_INDEX\"}\n",
        )
        .unwrap();
        git(
            &root,
            &["add", "lib.rs", "credentials/token.txt", "credentials.json"],
        );
        git(&root, &["commit", "-qm", "fixture"]);

        let mut index = ProjectIndex::discover(&root, &data).unwrap();
        let report = index.index().unwrap();
        assert_eq!(report.indexed_files, 1);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                fs::metadata(&data).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }
        let search = index.search("PROJECT_INDEX_SENTINEL_42", 5, false).unwrap();
        assert_eq!(search.hits.len(), 1);
        assert!(
            index
                .search("SECRET_DATA_FILENAME_MUST_NOT_INDEX", 5, false)
                .unwrap()
                .hits
                .is_empty()
        );
        let fetched = index.get(&search.hits[0].citation, 4096).unwrap();
        assert!(fetched.content.contains("PROJECT_INDEX_SENTINEL_42"));
        let secret_bytes = fs::read(root.join("credentials.json")).unwrap();
        let secret_citation = project_citation(
            &index.marker.project_id,
            "credentials.json",
            blake3::hash(&secret_bytes).to_hex().as_ref(),
            0,
            secret_bytes.len(),
        );
        assert!(index.get(&secret_citation, 4096).is_err());
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("lib.rs", root.join("linked.rs")).unwrap();
            let linked_citation = project_citation(
                &index.marker.project_id,
                "linked.rs",
                blake3::hash(fetched.content.as_bytes()).to_hex().as_ref(),
                0,
                fetched.content.len(),
            );
            assert!(index.get(&linked_citation, 4096).is_err());
        }

        fs::write(root.join("lib.rs"), "pub fn changed() {}\n").unwrap();
        assert!(index.status().unwrap().stale);
        assert!(index.get(&search.hits[0].citation, 4096).is_err());
        index.configure(AutoReindexMode::OnSearch, None).unwrap();
        let refreshed = index.search("changed", 5, true).unwrap();
        assert!(refreshed.reindex_attempted);
        assert!(!refreshed.stale);
        assert_eq!(refreshed.hits.len(), 1);

        // A second edit keeps the same porcelain ` M` status. Freshness must
        // still change because the indexed file bytes changed again.
        fs::write(root.join("lib.rs"), "pub fn changed_again() {}\n").unwrap();
        assert!(index.status().unwrap().stale);
        let remapped = index.map("changed again definition", 4096, true).unwrap();
        assert!(remapped.reindex_attempted);
        assert!(!remapped.stale);
        assert!(
            remapped
                .leads
                .iter()
                .any(|lead| lead.name == "changed_again")
        );
    }

    #[test]
    fn changed_byte_overflow_preserves_the_previous_index() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("repo");
        let data = temporary.path().join("data");
        fs::create_dir(&root).unwrap();
        git(&root, &["init", "-q"]);
        git(&root, &["config", "user.email", "test@example.invalid"]);
        git(&root, &["config", "user.name", "Test"]);
        init_marker(&root, "fixture", None).unwrap();
        fs::write(
            root.join("main.rs"),
            "const SAFE: &str = \"OLD_INDEX_VALUE\";\n",
        )
        .unwrap();
        git(&root, &["add", "main.rs"]);
        git(&root, &["commit", "-qm", "fixture"]);
        let mut index = ProjectIndex::discover(&root, &data).unwrap();
        index.index().unwrap();
        index.marker.project_index.max_changed_bytes = 8;
        fs::write(
            root.join("main.rs"),
            "const NEW: &str = \"NEW_INDEX_VALUE_IS_TOO_LARGE\";\n",
        )
        .unwrap();
        assert!(index.index().is_err());
        let old = index.search("OLD_INDEX_VALUE", 5, false).unwrap();
        assert_eq!(old.hits.len(), 1);
        assert!(old.stale);
    }

    #[test]
    fn foreground_watcher_requires_opt_in_and_is_bounded_for_automation() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("repo");
        let data = temporary.path().join("data");
        fs::create_dir(&root).unwrap();
        git(&root, &["init", "-q"]);
        init_marker(&root, "fixture", None).unwrap();
        fs::write(root.join("lib.rs"), "pub fn watched() {}\n").unwrap();
        git(&root, &["add", "lib.rs"]);

        let mut index = ProjectIndex::discover(&root, &data).unwrap();
        let marker_before = fs::read(root.join(MARKER_FILE)).unwrap();
        assert!(index.watch(250, 500, 100, Some(1)).is_err());
        let config = index.configure(AutoReindexMode::Watch, None).unwrap();
        assert_eq!(config.auto_reindex, AutoReindexMode::Watch);
        assert_eq!(fs::read(root.join(MARKER_FILE)).unwrap(), marker_before);
        assert_eq!(
            ProjectIndex::discover(&root, &data)
                .unwrap()
                .config()
                .auto_reindex,
            AutoReindexMode::Watch
        );
        let report = index.watch(250, 500, 100, Some(1)).unwrap();
        assert_eq!(report.polls, 1);
        assert_eq!(report.reindexes, 0);
        assert_eq!(report.failed_reindexes, 0);
        assert_eq!(report.failed_snapshot_reads, 0);

        let changed_root = root.clone();
        let changer = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            fs::write(changed_root.join("lib.rs"), "pub fn observed() {}\n").unwrap();
        });
        let mut observations = Vec::new();
        let report = index
            .watch_observed(250, 500, 100, Some(1), |event| observations.push(event))
            .unwrap();
        changer.join().unwrap();
        assert_eq!(report.reindexes, 1);
        assert!(matches!(
            observations.as_slice(),
            [ProjectWatchObservation::Reindexed { .. }]
        ));

        let rediscovered = ProjectIndex::discover(&root, &data).unwrap();
        assert_eq!(rediscovered.config().auto_reindex, AutoReindexMode::Watch);
    }

    #[test]
    fn foreground_watcher_survives_a_transient_git_snapshot_failure() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("repo");
        let data = temporary.path().join("data");
        fs::create_dir(&root).unwrap();
        git(&root, &["init", "-q"]);
        init_marker(&root, "fixture", None).unwrap();
        fs::write(root.join("lib.rs"), "pub fn watched() {}\n").unwrap();
        git(&root, &["add", "lib.rs"]);

        let mut index = ProjectIndex::discover(&root, &data).unwrap();
        index.configure(AutoReindexMode::Watch, None).unwrap();
        let git_dir = root.join(".git");
        let paused_git_dir = root.join(".git-paused");
        fs::rename(&git_dir, &paused_git_dir).unwrap();
        let restorer = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            fs::rename(paused_git_dir, git_dir).unwrap();
        });
        let mut observations = Vec::new();
        let report = index
            .watch_observed(250, 500, 100, Some(1), |event| observations.push(event))
            .unwrap();
        restorer.join().unwrap();

        assert_eq!(report.failed_snapshot_reads, 1);
        assert_ne!(report.last_snapshot, "unavailable");
        assert!(matches!(
            observations.as_slice(),
            [ProjectWatchObservation::SnapshotFailed { .. }]
        ));
    }

    #[test]
    fn foreground_watcher_retries_transient_index_lock_contention() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("repo");
        let data = temporary.path().join("data");
        fs::create_dir(&root).unwrap();
        git(&root, &["init", "-q"]);
        init_marker(&root, "fixture", None).unwrap();
        fs::write(root.join("lib.rs"), "pub fn watched() {}\n").unwrap();
        git(&root, &["add", "lib.rs"]);

        let mut index = ProjectIndex::discover(&root, &data).unwrap();
        index.index().unwrap();
        index.configure(AutoReindexMode::Watch, None).unwrap();
        let lock_path = index.index_dir.join("index.lock");
        let changed_root = root.clone();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let holder = thread::spawn(move || {
            let lock = private_lock_file(&lock_path).unwrap();
            lock.try_lock_exclusive().unwrap();
            ready_tx.send(()).unwrap();
            thread::sleep(Duration::from_millis(100));
            fs::write(changed_root.join("lib.rs"), "pub fn observed() {}\n").unwrap();
            thread::sleep(Duration::from_millis(600));
        });
        ready_rx.recv().unwrap();
        let mut observations = Vec::new();
        let report = index
            .watch_observed(250, 500, 100, Some(2), |event| observations.push(event))
            .unwrap();
        holder.join().unwrap();

        assert_eq!(report.failed_reindexes, 1);
        assert_eq!(report.reindexes, 1);
        assert!(matches!(
            observations.as_slice(),
            [
                ProjectWatchObservation::ReindexFailed { .. },
                ProjectWatchObservation::Reindexed { .. }
            ]
        ));
        assert_eq!(index.search("observed", 5, false).unwrap().hits.len(), 1);
    }

    #[test]
    fn secret_shaped_data_filenames_are_excluded_without_hiding_source_modules() {
        for path in [
            "credentials.json",
            "config/secrets.yaml",
            "config/aws_keys.yml",
            "config/api-keys.toml",
            ".env.production",
            ".git-credentials",
        ] {
            assert!(excluded_path(path), "expected {path} to be excluded");
        }
        for path in [
            "src/credentials.rs",
            "src/secrets.ts",
            "docs/credential-handling.md",
        ] {
            assert!(!excluded_path(path), "expected {path} to remain indexable");
        }
    }

    fn projection_rows(index: &ProjectIndex) -> Vec<String> {
        let connection = index.open_database().unwrap();
        let mut rows = Vec::new();
        for (label, sql) in [
            (
                "file",
                "SELECT path || '|' || content_hash || '|' || byte_count || '|' ||
                        COALESCE(structural_language, '') || '|' || structural_state
                   FROM project_files ORDER BY path",
            ),
            (
                "symbol",
                "SELECT symbol_key || '|' || path || '|' || name || '|' || kind || '|' ||
                        qualified_name || '|' || COALESCE(parent_key, '') || '|' ||
                        start_byte || '|' || end_byte || '|' || start_line || '|' ||
                        end_line || '|' || is_test
                   FROM project_symbols ORDER BY symbol_key",
            ),
            (
                "reference",
                "SELECT source_path || '|' || source_key || '|' || target_name || '|' ||
                        kind || '|' || start_byte || '|' || end_byte
                   FROM project_references
                  ORDER BY source_path, source_key, target_name, kind, start_byte, end_byte",
            ),
            (
                "edge",
                "SELECT source_key || '|' || target_key || '|' || target_name || '|' ||
                        kind || '|' || confidence || '|' || source_path || '|' ||
                        start_byte || '|' || end_byte
                   FROM project_edges
                  ORDER BY source_key, target_key, kind, start_byte, end_byte",
            ),
            (
                "chunk",
                "SELECT path || '|' || start_byte || '|' || end_byte || '|' || content
                   FROM chunks_fts ORDER BY path, start_byte",
            ),
        ] {
            let mut statement = connection.prepare(sql).unwrap();
            let values = statement
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap();
            rows.extend(values.into_iter().map(|value| format!("{label}:{value}")));
        }
        rows
    }

    #[test]
    fn structural_map_is_bounded_verified_and_preserves_ambiguity() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("repo");
        let data = temporary.path().join("data");
        fs::create_dir(&root).unwrap();
        git(&root, &["init", "-q"]);
        git(&root, &["config", "user.email", "test@example.invalid"]);
        git(&root, &["config", "user.name", "Test"]);
        init_marker(&root, "fixture", None).unwrap();
        fs::create_dir_all(root.join("src")).unwrap();
        fs::create_dir_all(root.join("tests")).unwrap();
        fs::create_dir_all(root.join("docs")).unwrap();
        fs::create_dir_all(root.join("credentials")).unwrap();
        fs::write(
            root.join("src/lib.rs"),
            "pub fn dispatch() { process(); process(); helper(); }\n",
        )
        .unwrap();
        fs::write(
            root.join("src/service.rs"),
            "pub fn process() {}\npub fn get_route_handler() {}\n",
        )
        .unwrap();
        fs::write(
            root.join("src/qualified.rs"),
            "pub fn qualified() { crate::process(); }\n",
        )
        .unwrap();
        fs::write(
            root.join("src/string.rs"),
            "pub const NOTE: &str = \"process is mentioned, not called\";\n",
        )
        .unwrap();
        fs::write(
            root.join("docs/mention.md"),
            "The process symbol appears here as documentation only.\nA second process mention stays in the same file context.\n",
        )
        .unwrap();
        fs::write(root.join("src/a.rs"), "pub fn helper() {}\n").unwrap();
        fs::write(root.join("src/b.rs"), "pub fn helper() {}\n").unwrap();
        fs::write(
            root.join("tests/service_test.rs"),
            "#[test]\nfn process_test() { process(); }\n",
        )
        .unwrap();
        fs::write(
            root.join("src/transitive.rs"),
            r#"pub fn target_leaf() {}
pub fn layer_one() { target_leaf(); }
pub fn layer_two() { layer_one(); }
pub fn two_hop_leaf() {}
pub fn two_hop_wrapper() { two_hop_leaf(); }
pub fn depth_four_leaf() {}
pub fn depth_four_one() { depth_four_leaf(); }
pub fn depth_four_two() { depth_four_one(); }
pub fn depth_four_three() { depth_four_two(); }
pub fn ambiguous_leaf() {}
pub fn ambiguous_bridge() { ambiguous_leaf(); }
pub fn fan_leaf() {}
pub fn fan_caller_00() { fan_leaf(); }
pub fn fan_caller_01() { fan_leaf(); }
pub fn fan_caller_02() { fan_leaf(); }
pub fn fan_caller_03() { fan_leaf(); }
pub fn fan_caller_04() { fan_leaf(); }
pub fn fan_caller_05() { fan_leaf(); }
pub fn fan_caller_06() { fan_leaf(); }
pub fn fan_caller_07() { fan_leaf(); }
pub fn fan_caller_08() { fan_leaf(); }
pub fn fan_caller_09() { fan_leaf(); }
pub fn fan_caller_10() { fan_leaf(); }
pub fn fan_caller_11() { fan_leaf(); }
pub fn fan_caller_12() { fan_leaf(); }
pub fn fan_caller_13() { fan_leaf(); }
pub fn fan_caller_14() { fan_leaf(); }
pub fn fan_caller_15() { fan_leaf(); }
pub fn fan_caller_16() { fan_leaf(); }
pub fn cycle_a() { cycle_b(); }
pub fn cycle_b() { cycle_a(); }
pub fn map() {}
pub struct FixtureIndex;
impl FixtureIndex {
    pub fn mapped(&self) {}
}
"#,
        )
        .unwrap();
        fs::write(
            root.join("src/ambiguous_bridge.rs"),
            "pub fn ambiguous_bridge() { ambiguous_leaf(); }\n",
        )
        .unwrap();
        fs::write(
            root.join("tests/transitive_test.rs"),
            r#"#[test]
fn indirect_leaf_test() { layer_two(); }
#[test]
fn two_hop_test() { two_hop_wrapper(); }
#[test]
fn depth_four_test() { depth_four_three(); }
#[test]
fn ambiguous_test() { ambiguous_bridge(); }
#[test]
fn cycle_test() { cycle_a(); }
#[test]
fn iterator_map_is_not_project_map() { [1].iter().map(|value| value); }
#[test]
fn receiver_method_test() { let index = FixtureIndex; index.mapped(); }
"#,
        )
        .unwrap();
        fs::write(
            root.join("credentials/secret.rs"),
            "pub fn NEVER_LEAK_SECRET_SYMBOL() {}\n",
        )
        .unwrap();
        git(&root, &["add", "."]);
        git(&root, &["commit", "-qm", "fixture"]);

        let index = ProjectIndex::discover(&root, &data).unwrap();
        let indexed = index.index().unwrap();
        assert_eq!(indexed.structural_files, 10);
        assert_eq!(indexed.parse_error_files, 0);
        assert!(indexed.symbol_count >= 10);
        assert!(indexed.edge_count >= 4);
        fs::write(
            root.join("tests/untracked_process.rs"),
            "#[test]\nfn untracked_process_test() { process(); }\n",
        )
        .unwrap();

        let report = index
            .map("what calls process and tests it", 4096, false)
            .unwrap();
        assert!(report.content_is_untrusted);
        assert_eq!(report.presence, "symbols");
        assert_eq!(report.query_mode, "callers+tests");
        assert!(serde_json::to_vec(&report).unwrap().len() <= 4096);
        assert!(report.leads.iter().any(|lead| lead.name == "process"));
        assert!(report.leads.iter().all(|lead| lead.name == "process"));
        assert!(report.limits.contains(&"dynamic_dispatch".to_owned()));
        let process_facets = &report.leads[0].facets;
        assert!(process_facets.direct_callers.returned >= 2);
        assert_eq!(process_facets.direct_callees.state, "not_requested");
        assert!(process_facets.direct_tests.returned >= 1);
        assert!(report.leads.iter().all(|lead| {
            index.get(&lead.citation, 4096).is_ok()
                && !lead.excerpt.contains("NEVER_LEAK_SECRET_SYMBOL")
        }));
        assert!(
            report
                .leads
                .iter()
                .flat_map(|lead| &lead.edges)
                .any(|edge| edge.kind == "tested_by")
        );
        assert!(
            report
                .leads
                .iter()
                .flat_map(|lead| &lead.edges)
                .any(|edge| edge.kind == "tested_by"
                    && edge.test_surface.as_deref() == Some("integration"))
        );
        let process_edges = report
            .leads
            .iter()
            .find(|lead| lead.name == "process")
            .unwrap()
            .edges
            .iter()
            .collect::<Vec<_>>();
        assert!(process_edges.iter().any(|edge| edge.name == "dispatch"));
        assert!(process_edges.iter().any(|edge| edge.name == "qualified"));
        let dispatch_sites = process_edges
            .iter()
            .filter(|edge| edge.name == "dispatch")
            .collect::<Vec<_>>();
        assert_eq!(dispatch_sites.len(), 2);
        assert!(dispatch_sites.iter().all(|edge| edge.site_count == 2));
        assert_eq!(
            dispatch_sites
                .iter()
                .map(|edge| edge.site_index)
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([1, 2])
        );
        assert!(process_edges.iter().all(|edge| {
            edge.kind == "tested_by" || (edge.kind == "calls" && edge.direction == "incoming")
        }));
        assert!(process_edges.iter().all(|edge| {
            edge.excerpt.contains("process")
                && edge.reference_start_line > 0
                && index.get(&edge.citation, 4096).is_ok()
        }));

        let three_hop = index
            .map(
                "which tests cover target_leaf",
                MAX_PROJECT_MAP_BYTES,
                false,
            )
            .unwrap();
        let three_hop_lead = three_hop
            .leads
            .iter()
            .flat_map(|lead| &lead.edges)
            .find(|edge| edge.kind == "behavioral_test_lead" && edge.name == "indirect_leaf_test")
            .expect("three-hop behavioral test lead");
        assert_eq!(three_hop_lead.hops, 3);
        assert_eq!(
            three_hop_lead
                .via
                .iter()
                .map(|via| via.name.as_str())
                .collect::<Vec<_>>(),
            vec!["layer_two", "layer_one"]
        );
        assert!(three_hop_lead.excerpt.contains("layer_two"));
        assert_eq!(three_hop_lead.confidence, "inferred");

        let two_hop = index
            .map(
                "which tests cover two_hop_leaf",
                MAX_PROJECT_MAP_BYTES,
                false,
            )
            .unwrap();
        let two_hop_lead = two_hop
            .leads
            .iter()
            .flat_map(|lead| &lead.edges)
            .find(|edge| edge.kind == "behavioral_test_lead" && edge.name == "two_hop_test")
            .expect("two-hop behavioral test lead");
        assert_eq!(two_hop_lead.hops, 2);
        assert_eq!(two_hop_lead.via[0].name, "two_hop_wrapper");

        let depth_four = index
            .map(
                "which tests cover depth_four_leaf",
                MAX_PROJECT_MAP_BYTES,
                false,
            )
            .unwrap();
        assert!(
            !depth_four
                .leads
                .iter()
                .flat_map(|lead| &lead.edges)
                .any(|edge| edge.kind == "behavioral_test_lead" && edge.name == "depth_four_test")
        );

        let ambiguous_test = index
            .map(
                "which tests cover ambiguous_leaf",
                MAX_PROJECT_MAP_BYTES,
                false,
            )
            .unwrap();
        assert!(
            !ambiguous_test
                .leads
                .iter()
                .flat_map(|lead| &lead.edges)
                .any(|edge| edge.kind == "behavioral_test_lead" && edge.name == "ambiguous_test")
        );
        assert!(
            ambiguous_test
                .leads
                .iter()
                .find(|lead| lead.name == "ambiguous_leaf")
                .unwrap()
                .edges_truncated
        );

        let fan_in = index
            .map("which tests cover fan_leaf", MAX_PROJECT_MAP_BYTES, false)
            .unwrap();
        assert!(
            fan_in
                .leads
                .iter()
                .find(|lead| lead.name == "fan_leaf")
                .unwrap()
                .edges_truncated
        );

        let cycle = index
            .map("which tests cover cycle_b", MAX_PROJECT_MAP_BYTES, false)
            .unwrap();
        let cycle_lead = cycle
            .leads
            .iter()
            .flat_map(|lead| &lead.edges)
            .find(|edge| edge.kind == "behavioral_test_lead" && edge.name == "cycle_test")
            .expect("bounded traversal should cross and terminate on a cycle");
        assert_eq!(cycle_lead.hops, 2);
        assert_eq!(cycle_lead.via[0].name, "cycle_a");
        assert_eq!(
            serde_json::to_vec(&cycle).unwrap(),
            serde_json::to_vec(
                &index
                    .map("which tests cover cycle_b", MAX_PROJECT_MAP_BYTES, false)
                    .unwrap()
            )
            .unwrap(),
            "bounded traversal output must be deterministic"
        );

        let dynamic_receiver = index
            .map("which tests cover map", MAX_PROJECT_MAP_BYTES, false)
            .unwrap();
        assert!(
            !dynamic_receiver
                .leads
                .iter()
                .flat_map(|lead| &lead.edges)
                .any(|edge| edge.name == "iterator_map_is_not_project_map")
        );
        assert!(
            dynamic_receiver
                .leads
                .iter()
                .find(|lead| lead.name == "map")
                .unwrap()
                .test_leads_truncated
        );

        let compatible_receiver = index
            .map("which tests cover mapped", MAX_PROJECT_MAP_BYTES, false)
            .unwrap();
        assert!(
            compatible_receiver
                .leads
                .iter()
                .flat_map(|lead| &lead.edges)
                .any(|edge| edge.kind == "tested_by" && edge.name == "receiver_method_test")
        );

        let closure = index
            .map(
                "what calls process and tests it",
                MAX_PROJECT_MAP_BYTES,
                false,
            )
            .unwrap();
        assert_eq!(
            report
                .leads
                .iter()
                .map(|lead| lead.edges.len())
                .sum::<usize>(),
            closure
                .leads
                .iter()
                .map(|lead| lead.edges.len())
                .sum::<usize>(),
            "lexical residue must be evicted before structural edges"
        );
        if report.lexical_residue.len() < closure.lexical_residue.len() {
            assert!(report.mentions_truncated);
        }
        assert!(!closure.mentions_truncated);
        assert_eq!(closure.coverage.untracked_excluded, 1);
        assert!(closure.coverage.excluded_paths >= 1);
        assert!(closure.lexical_residue.iter().all(|mention| {
            mention.classification == "unresolved_mention"
                && mention.excerpt.contains("process")
                && index.get(&mention.citation, 4096).is_ok()
        }));
        assert!(
            closure
                .lexical_residue
                .iter()
                .any(|mention| mention.path == "docs/mention.md")
        );
        let documentation_mentions = closure
            .lexical_residue
            .iter()
            .find(|mention| mention.path == "docs/mention.md")
            .unwrap();
        assert_eq!(documentation_mentions.occurrence_count, 2);
        assert_eq!(documentation_mentions.occurrence_lines, vec![1, 2]);
        assert!(
            closure
                .lexical_residue
                .iter()
                .any(|mention| mention.path == "src/string.rs")
        );
        let connection = index.open_database().unwrap();
        let mut statement = connection
            .prepare("SELECT path FROM project_files ORDER BY path")
            .unwrap();
        let indexed_paths = statement
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        let covered =
            |path: &str, start: usize, end: usize| {
                closure.leads.iter().any(|lead| {
                    lead.path == path && lead.start_byte <= start && lead.end_byte >= end
                }) || closure
                    .leads
                    .iter()
                    .flat_map(|lead| &lead.edges)
                    .any(|edge| {
                        edge.reference_path_internal == path
                            && edge.reference_start_byte <= start
                            && edge.reference_end_byte >= end
                    })
                    || closure.lexical_residue.iter().any(|mention| {
                        mention.path == path
                            && mention.occurrence_lines.contains(
                                &(1 + fs::read_to_string(root.join(path)).unwrap()[..start]
                                    .bytes()
                                    .filter(|byte| *byte == b'\n')
                                    .count()),
                            )
                    })
            };
        for path in indexed_paths {
            let text = fs::read_to_string(root.join(&path)).unwrap();
            for (start, _) in text.match_indices("process") {
                if exact_identifier_occurrence(&text, start, "process".len()) {
                    assert!(
                        covered(&path, start, start + "process".len()),
                        "uncovered exact process occurrence at {path}:{start}"
                    );
                }
            }
        }

        let long_path = "src/long_excerpt.rs";
        let long_source = format!(
            "pub fn sample() {{ process(\"{}🚀\"); }}\n",
            "x".repeat(900)
        );
        fs::write(root.join(long_path), &long_source).unwrap();
        let long_start = long_source.find("process").unwrap();
        let long_end = long_source.find("); }").unwrap() + 1;
        let long_hash = blake3::hash(long_source.as_bytes()).to_hex().to_string();
        let (_, long_excerpt, long_truncated, _, _) = index
            .verified_reference_excerpt(
                long_path,
                &long_hash,
                long_start,
                long_end,
                MAX_PROJECT_MAP_REFERENCE_EXCERPT_BYTES,
                false,
            )
            .unwrap()
            .unwrap();
        assert!(long_truncated);
        assert!(long_excerpt.len() <= MAX_PROJECT_MAP_REFERENCE_EXCERPT_BYTES);
        assert!(long_excerpt.starts_with("process("));

        let context_path = "src/residue_context.rs";
        let context_source = "pub const SAMPLE: &str = r#\"\nprocess();\n\"#;\n";
        fs::write(root.join(context_path), context_source).unwrap();
        let context_start = context_source.find("process").unwrap();
        let context_hash = blake3::hash(context_source.as_bytes()).to_hex().to_string();
        let (_, context_excerpt, context_truncated, _, _) = index
            .verified_reference_excerpt(
                context_path,
                &context_hash,
                context_start,
                context_start + "process".len(),
                MAX_PROJECT_MAP_MENTION_EXCERPT_BYTES,
                true,
            )
            .unwrap()
            .unwrap();
        assert!(!context_truncated);
        assert!(context_excerpt.contains("r#\""));
        assert!(context_excerpt.contains("\"#;"));

        let ambiguous = index.map("helper callers", 4096, false).unwrap();
        let ambiguous_edges = ambiguous
            .leads
            .iter()
            .flat_map(|lead| &lead.edges)
            .filter(|edge| {
                edge.kind == "calls" && edge.confidence == StructuralConfidence::Ambiguous.as_str()
            })
            .count();
        assert!(
            ambiguous_edges >= 2,
            "ambiguous candidates must not collapse"
        );
        assert!(
            index
                .map("NEVER_LEAK_SECRET_SYMBOL", 4096, false)
                .unwrap()
                .leads
                .is_empty()
        );
        let natural_identifier = index
            .map("where is the route handler defined", 4096, false)
            .unwrap();
        assert!(
            natural_identifier
                .leads
                .iter()
                .any(|lead| lead.name == "get_route_handler")
        );

        fs::write(root.join("src/lib.rs"), "pub fn dispatch() { helper(); }\n").unwrap();
        let stale = index.map("process callers", 4096, false).unwrap();
        assert!(stale.stale);
        assert!(stale.mentions_truncated);
        assert!(stale.leads.iter().flat_map(|lead| &lead.edges).all(|edge| {
            edge.path != "src/lib.rs" && !edge.citation.contains("c3JjL2xpYi5ycw")
        }));
    }

    #[test]
    fn structural_query_intents_are_composable_and_directional() {
        assert_eq!(
            project_map_mode("who are the direct callers and which tests cover parse_config"),
            "callers+tests"
        );
        assert_eq!(project_map_mode("what calls parse_config"), "callers");
        assert_eq!(
            project_map_mode("which symbols directly call parse_config"),
            "callers"
        );
        assert_eq!(
            project_map_mode("show the callees invoked by parse_config"),
            "callees"
        );
        assert_eq!(
            project_map_mode("show calls made by parse_config"),
            "callees"
        );
        assert_eq!(
            project_map_mode("find callers, tests, and dependencies of parse_config"),
            "callers+callees+tests"
        );
        assert_eq!(
            project_map_mode("where is ParseConfig defined"),
            "definition"
        );
        assert_eq!(project_map_mode("which modules import config"), "imports");
        assert_eq!(project_map_mode("explain parse_config"), "mixed");
        assert_eq!(project_map_mode("show the latest parser"), "mixed");
    }

    #[test]
    fn identifier_expansion_preserves_original_and_adds_natural_words() {
        assert_eq!(
            identifier_words("getHTTPResponse_v2"),
            ["2", "get", "http", "response", "v"]
        );
        let search = symbol_search_text(
            "get_route_handler",
            "src/api_routes.rs::get_route_handler",
            "function",
            "src/api_routes.rs",
        );
        assert!(search.contains("get_route_handler"));
        assert!(search.contains("route"));
        assert!(search.contains("handler"));
        let query = project_map_terms("how does source withdrawal affect retrieval");
        assert!(query.contains(&"withdraw".into()));
        assert!(query.contains(&"retrieve".into()));
    }

    #[test]
    fn behavioral_call_gate_rejects_chains_and_matches_only_owner_words() {
        assert_eq!(
            classify_call_form("run()", "run"),
            Some(TraversalCallForm::Free)
        );
        assert_eq!(
            classify_call_form("crate::eval::run()", "run"),
            Some(TraversalCallForm::Static {
                qualifier: "eval".into()
            })
        );
        assert_eq!(
            classify_call_form("project_index.map()", "map"),
            Some(TraversalCallForm::Receiver {
                receiver: "project_index".into()
            })
        );
        assert_eq!(
            classify_call_form("values.iter().map(|value| value)", "map"),
            None
        );
        assert_eq!(classify_call_form("client.transport.send()", "send"), None);
        let owner = identifier_words("ProjectIndex");
        assert!(receiver_matches_owner("index", &owner));
        assert!(receiver_matches_owner("project_index", &owner));
        assert!(!receiver_matches_owner("other_index", &owner));
        assert_eq!(
            rust_impl_owner("impl<T> ProjectIndex<T> where T: Send {\n    pub fn map"),
            Some("ProjectIndex".into())
        );
        assert_eq!(
            rust_impl_owner("impl SomeTrait for MemoryStore {\n    fn retrieve"),
            Some("MemoryStore".into())
        );
    }

    #[test]
    fn incremental_projection_equals_clean_rebuild() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("repo");
        fs::create_dir(&root).unwrap();
        git(&root, &["init", "-q"]);
        git(&root, &["config", "user.email", "test@example.invalid"]);
        git(&root, &["config", "user.name", "Test"]);
        init_marker(&root, "fixture", None).unwrap();
        fs::write(root.join("a.rs"), "pub fn alpha() {}\n").unwrap();
        fs::write(root.join("b.rs"), "pub fn beta() { alpha(); }\n").unwrap();
        git(&root, &["add", "."]);
        git(&root, &["commit", "-qm", "fixture"]);

        let incremental =
            ProjectIndex::discover(&root, &temporary.path().join("incremental")).unwrap();
        incremental.index().unwrap();
        fs::write(
            root.join("b.rs"),
            "pub fn beta() { alpha(); gamma(); }\npub fn gamma() {}\n",
        )
        .unwrap();
        fs::write(root.join("c.py"), "def delta():\n    return 4\n").unwrap();
        git(&root, &["add", "."]);
        incremental.index().unwrap();

        let clean = ProjectIndex::discover(&root, &temporary.path().join("clean")).unwrap();
        clean.index().unwrap();
        assert_eq!(projection_rows(&incremental), projection_rows(&clean));
    }

    #[test]
    fn disposable_schema_two_projection_is_invalidated_automatically() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("repo");
        let data = temporary.path().join("data");
        fs::create_dir(&root).unwrap();
        git(&root, &["init", "-q"]);
        init_marker(&root, "fixture", None).unwrap();
        fs::write(root.join("lib.rs"), "pub fn current() {}\n").unwrap();
        git(&root, &["add", "."]);
        let index = ProjectIndex::discover(&root, &data).unwrap();
        fs::create_dir_all(&index.index_dir).unwrap();
        let legacy = Connection::open(&index.database_path).unwrap();
        legacy
            .execute_batch(
                "CREATE TABLE project_meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                 INSERT INTO project_meta VALUES('schema_version', '2');
                 INSERT INTO project_meta VALUES('snapshot', 'legacy');
                 CREATE TABLE project_files(
                    path TEXT PRIMARY KEY, content_hash TEXT NOT NULL, byte_count INTEGER NOT NULL
                 );
                 INSERT INTO project_files VALUES('legacy.rs', 'bad', 3);",
            )
            .unwrap();
        drop(legacy);

        let status = index.status().unwrap();
        assert_eq!(status.schema_version, 3);
        assert!(!status.ready);
        assert_eq!(status.indexed_files, 0);
        let rebuilt = index.index().unwrap();
        assert_eq!(rebuilt.indexed_files, 1);
        assert_eq!(rebuilt.structural_files, 1);
    }

    #[test]
    fn not_ready_map_is_fast_machine_readable_and_does_not_start_indexing() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("repo");
        let data = temporary.path().join("data");
        fs::create_dir(&root).unwrap();
        git(&root, &["init", "-q"]);
        init_marker(&root, "fixture", None).unwrap();
        fs::write(root.join("lib.rs"), "pub fn untouched() {}\n").unwrap();
        git(&root, &["add", "."]);
        let index = ProjectIndex::discover(&root, &data).unwrap();
        let started = Instant::now();
        let report = index.map("untouched callers", 4096, true).unwrap();
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(report.structural_state, "not_ready");
        assert_eq!(report.presence, "none");
        assert!(!report.reindex_attempted);
        assert!(report.stale);
        assert!(!index.database_path.exists());
    }

    #[test]
    fn unsupported_language_query_returns_verified_fts_fallback() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("repo");
        let data = temporary.path().join("data");
        fs::create_dir(&root).unwrap();
        git(&root, &["init", "-q"]);
        init_marker(&root, "fixture", None).unwrap();
        fs::write(root.join("lib.rs"), "pub fn unrelated() {}\n").unwrap();
        fs::write(
            root.join("notes.md"),
            "The cobalt migration requires a reversible shadow cutover.\n",
        )
        .unwrap();
        git(&root, &["add", "."]);
        let index = ProjectIndex::discover(&root, &data).unwrap();
        let indexed = index.index().unwrap();
        assert_eq!(indexed.structural_files, 1);
        assert_eq!(indexed.fallback_files, 1);

        let report = index
            .map("cobalt reversible shadow cutover", 4096, false)
            .unwrap();
        assert_eq!(report.structural_state, "fts_fallback");
        assert_eq!(report.presence, "text_only");
        assert!(report.leads.is_empty());
        assert_eq!(report.text_fallback.len(), 1);
        let exact = index.get(&report.text_fallback[0].citation, 4096).unwrap();
        assert!(exact.content.contains("cobalt migration"));
    }
}
