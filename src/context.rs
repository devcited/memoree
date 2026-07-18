//! Ambient context and local application settings.
//!
//! Context resolution deliberately has a narrow, deterministic precedence:
//! `MEMOREE_CONTEXT`, then the nearest ancestor `.memoree.toml`, then an explicitly
//! configured personal fallback. Resolution never broadens a search horizon.

use std::{
    env,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    error::{MemoryError, Result},
    protocol::{
        AmbientContext, ContextSource, Horizon, MAX_CONTEXT_ID_BYTES, MAX_CONTEXT_PINS,
        MAX_PIN_BYTES, ResolvedContext,
    },
};

pub const MARKER_FILE: &str = ".memoree.toml";
pub const MEMOREE_CONTEXT_ENV: &str = "MEMOREE_CONTEXT";
pub const MEMOREE_HOME_ENV: &str = "MEMOREE_HOME";
pub const MEMOREE_CONFIG_ENV: &str = "MEMOREE_CONFIG";
pub const MEMOREE_DATA_DIR_ENV: &str = "MEMOREE_DATA_DIR";
pub const MEMOREE_RUNTIME_DIR_ENV: &str = "MEMOREE_RUNTIME_DIR";
pub const MEMOREE_SOCKET_ENV: &str = "MEMOREE_SOCKET";

const SETTINGS_FILE: &str = "config.toml";
const SOCKET_FILE: &str = "memoree.sock";
const SETTINGS_SCHEMA: u32 = 1;
const MARKER_SCHEMA: u32 = 1;

/// Platform-appropriate paths used by the local daemon and CLI.
///
/// `MEMOREE_HOME` provides a convenient relocatable layout. Individual path
/// variables can override any part of that layout. No directory is created by
/// discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppPaths {
    pub data_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub socket_path: PathBuf,
    pub config_path: PathBuf,
}

impl AppPaths {
    pub fn discover() -> Result<Self> {
        let home = env_path(MEMOREE_HOME_ENV);
        let (default_data, default_runtime, default_config) = match home {
            Some(home) => (
                home.join("data"),
                home.join("run"),
                home.join(SETTINGS_FILE),
            ),
            None => {
                let project = ProjectDirs::from("dev", "memoree", "memoree").ok_or_else(|| {
                    MemoryError::Config(
                        "could not determine platform application directories; set MEMOREE_HOME"
                            .into(),
                    )
                })?;
                (
                    project.data_dir().to_path_buf(),
                    project
                        .runtime_dir()
                        .map(Path::to_path_buf)
                        .unwrap_or_else(|| project.data_local_dir().join("run")),
                    project.config_dir().join(SETTINGS_FILE),
                )
            }
        };

        let data_dir = env_path(MEMOREE_DATA_DIR_ENV).unwrap_or(default_data);
        let runtime_dir = env_path(MEMOREE_RUNTIME_DIR_ENV).unwrap_or(default_runtime);
        let config_path = env_path(MEMOREE_CONFIG_ENV).unwrap_or(default_config);
        let socket_path =
            env_path(MEMOREE_SOCKET_ENV).unwrap_or_else(|| runtime_dir.join(SOCKET_FILE));

        Ok(Self {
            data_dir,
            runtime_dir,
            socket_path,
            config_path,
        })
    }
}

fn env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    #[serde(default = "settings_schema")]
    pub schema: u32,
    /// This fallback is used only when neither a session nor a project marker
    /// exists. Merely having a config file never makes retrieval broader.
    #[serde(default, alias = "personal_fallback")]
    pub personal: Option<PersonalFallback>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            schema: SETTINGS_SCHEMA,
            personal: None,
        }
    }
}

const fn settings_schema() -> u32 {
    SETTINGS_SCHEMA
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PersonalFallback {
    pub workspace_id: String,
    pub project_id: String,
    #[serde(default)]
    pub pins: Vec<String>,
}

impl PersonalFallback {
    pub fn into_context(self) -> AmbientContext {
        AmbientContext {
            workspace_id: self.workspace_id,
            project_id: self.project_id,
            task_id: None,
            component: None,
            pins: self.pins,
        }
    }
}

/// Load settings from the platform default (or environment-overridden) path.
pub fn load_settings() -> Result<Settings> {
    let paths = AppPaths::discover()?;
    load_settings_from(&paths.config_path)
}

/// Load settings from a specified path. A missing file means default settings;
/// malformed or unsupported settings are errors rather than silent fallbacks.
pub fn load_settings_from(path: impl AsRef<Path>) -> Result<Settings> {
    let path = path.as_ref();
    let source = match fs::read_to_string(path) {
        Ok(source) => source,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Settings::default());
        }
        Err(error) => return Err(error.into()),
    };

    let settings: Settings = toml::from_str(&source).map_err(|error| {
        MemoryError::Config(format!(
            "could not parse settings {}: {error}",
            path.display()
        ))
    })?;
    validate_settings(&settings, path)?;
    Ok(settings)
}

fn validate_settings(settings: &Settings, path: &Path) -> Result<()> {
    if settings.schema != SETTINGS_SCHEMA {
        return Err(MemoryError::Config(format!(
            "unsupported settings schema {} in {}; expected {SETTINGS_SCHEMA}",
            settings.schema,
            path.display()
        )));
    }
    if let Some(personal) = &settings.personal {
        validate_required("personal.workspace_id", &personal.workspace_id)?;
        validate_required("personal.project_id", &personal.project_id)?;
        validate_pins(&personal.pins)?;
    }
    Ok(())
}

/// On-disk project marker. Its IDs are generated once and remain stable even
/// if the directory is moved or renamed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Marker {
    #[serde(default = "marker_schema")]
    pub schema: u32,
    pub workspace_id: String,
    pub project_id: String,
    pub name: String,
    #[serde(default)]
    pub pins: Vec<String>,
}

const fn marker_schema() -> u32 {
    MARKER_SCHEMA
}

impl Marker {
    pub fn context(&self) -> AmbientContext {
        AmbientContext {
            workspace_id: self.workspace_id.clone(),
            project_id: self.project_id.clone(),
            task_id: None,
            component: None,
            pins: self.pins.clone(),
        }
    }
}

/// Create `.memoree.toml` without ever replacing an existing marker.
///
/// Pass an existing stable workspace ID to group projects. Passing `None`
/// creates a new workspace ID. The project ID is always generated once here.
pub fn init_marker(
    dir: impl AsRef<Path>,
    name: impl Into<String>,
    workspace_id: Option<&str>,
) -> Result<Marker> {
    let dir = dir.as_ref();
    if !dir.is_dir() {
        return Err(MemoryError::Config(format!(
            "marker directory does not exist or is not a directory: {}",
            dir.display()
        )));
    }

    let name = name.into();
    validate_required("marker.name", &name)?;
    if let Some(workspace_id) = workspace_id {
        validate_required("marker.workspace_id", workspace_id)?;
    }

    let marker = Marker {
        schema: MARKER_SCHEMA,
        workspace_id: workspace_id
            .map(str::to_owned)
            .unwrap_or_else(|| format!("wsp_{}", Ulid::r#gen())),
        project_id: format!("prj_{}", Ulid::r#gen()),
        name,
        pins: Vec::new(),
    };
    let mut serialized = toml::to_string_pretty(&marker).map_err(|error| {
        MemoryError::Config(format!("could not serialize project marker: {error}"))
    })?;
    serialized.push('\n');

    // Linking a fully synced temporary file gives us both atomic publication and
    // create-new semantics: hard_link fails if `.memoree.toml` already exists.
    let marker_path = dir.join(MARKER_FILE);
    let temporary_path = dir.join(format!("{MARKER_FILE}.tmp-{}", Ulid::r#gen()));
    let mut temporary = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary_path)?;
    let publication = (|| -> std::io::Result<()> {
        temporary.write_all(serialized.as_bytes())?;
        temporary.sync_all()?;
        fs::hard_link(&temporary_path, &marker_path)
    })();
    drop(temporary);
    let _ = fs::remove_file(&temporary_path);

    match publication {
        Ok(()) => Ok(marker),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            Err(MemoryError::Config(format!(
                "refusing to overwrite existing marker {}",
                marker_path.display()
            )))
        }
        Err(error) => Err(error.into()),
    }
}

pub fn read_marker(path: impl AsRef<Path>) -> Result<Marker> {
    let path = path.as_ref();
    let source = fs::read_to_string(path)?;
    let marker: Marker = toml::from_str(&source).map_err(|error| {
        MemoryError::Config(format!(
            "could not parse marker {}: {error}",
            path.display()
        ))
    })?;
    validate_marker(&marker, path)?;
    Ok(marker)
}

fn validate_marker(marker: &Marker, path: &Path) -> Result<()> {
    if marker.schema != MARKER_SCHEMA {
        return Err(MemoryError::Config(format!(
            "unsupported marker schema {} in {}; expected {MARKER_SCHEMA}",
            marker.schema,
            path.display()
        )));
    }
    validate_required("marker.workspace_id", &marker.workspace_id)?;
    validate_required("marker.project_id", &marker.project_id)?;
    validate_required("marker.name", &marker.name)?;
    validate_pins(&marker.pins)
}

fn validate_context(context: &AmbientContext) -> Result<()> {
    validate_required("context.workspace_id", &context.workspace_id)?;
    validate_required("context.project_id", &context.project_id)?;
    if let Some(task_id) = &context.task_id {
        validate_required("context.task_id", task_id)?;
    }
    if let Some(component) = &context.component {
        validate_required("context.component", component)?;
    }
    validate_pins(&context.pins)
}

fn validate_required(field: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(MemoryError::Config(format!("{field} must not be empty")));
    }
    if value.len() > MAX_CONTEXT_ID_BYTES {
        return Err(MemoryError::Config(format!(
            "{field} must not exceed {MAX_CONTEXT_ID_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_pins(pins: &[String]) -> Result<()> {
    if pins.len() > MAX_CONTEXT_PINS {
        return Err(MemoryError::Config(format!(
            "context pins must not contain more than {MAX_CONTEXT_PINS} values"
        )));
    }
    if pins
        .iter()
        .any(|pin| pin.trim().is_empty() || pin.len() > MAX_PIN_BYTES)
    {
        return Err(MemoryError::Config(format!(
            "context pins must contain 1..={MAX_PIN_BYTES} bytes"
        )));
    }
    Ok(())
}

/// Encode a complete inherited session context for `MEMOREE_CONTEXT`.
pub fn encode_memory_context(context: &AmbientContext) -> Result<String> {
    validate_context(context)?;
    serde_json::to_string(context).map_err(Into::into)
}

/// Decode and validate `MEMOREE_CONTEXT`.
pub fn decode_memory_context(value: &str) -> Result<AmbientContext> {
    let context: AmbientContext = serde_json::from_str(value).map_err(|error| {
        MemoryError::Config(format!("invalid {MEMOREE_CONTEXT_ENV} JSON: {error}"))
    })?;
    validate_context(&context)?;
    Ok(context)
}

/// Return a context suitable for a child task process while preserving the
/// stable project identity and project pins.
pub fn task_context(base: &AmbientContext, task_id: impl Into<String>) -> Result<AmbientContext> {
    validate_context(base)?;
    let task_id = task_id.into();
    validate_required("context.task_id", &task_id)?;
    let mut context = base.clone();
    context.task_id = Some(task_id);
    Ok(context)
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ContextExplanation {
    pub selected: ContextSource,
    pub summary: String,
    pub cwd: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_path: Option<PathBuf>,
    pub precedence: Vec<ContextSource>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ResolvedAmbient {
    pub context: AmbientContext,
    pub source: ContextSource,
    pub explanation: ContextExplanation,
}

impl ResolvedAmbient {
    /// Convert into the protocol response form. Ambient resolution cannot opt
    /// into a broader horizon; broadening must be explicit on each search.
    pub fn protocol_context(&self) -> ResolvedContext {
        ResolvedContext {
            ambient: self.context.clone(),
            resolved_from: self.source.clone(),
            horizon: Horizon::Ambient,
            broadened: false,
        }
    }
}

/// Deterministic ambient-context resolver with injectable sources for tests and
/// embedding. `new` uses process environment and platform settings.
#[derive(Debug, Clone)]
pub struct ContextResolver {
    cwd: PathBuf,
    config_path: PathBuf,
    session_json: Option<String>,
}

impl ContextResolver {
    pub fn new(cwd: impl AsRef<Path>) -> Result<Self> {
        let paths = AppPaths::discover()?;
        let session_json = match env::var(MEMOREE_CONTEXT_ENV) {
            Ok(value) if value.trim().is_empty() => None,
            Ok(value) => Some(value),
            Err(env::VarError::NotPresent) => None,
            Err(env::VarError::NotUnicode(_)) => {
                return Err(MemoryError::Config(format!(
                    "{MEMOREE_CONTEXT_ENV} is not valid UTF-8"
                )));
            }
        };
        Self::from_sources(cwd, paths.config_path, session_json)
    }

    /// Construct a resolver without reading process environment. This is also
    /// useful for callers that receive session context through another launcher.
    pub fn from_sources(
        cwd: impl AsRef<Path>,
        config_path: impl Into<PathBuf>,
        session_json: Option<String>,
    ) -> Result<Self> {
        Ok(Self {
            cwd: normalize_search_dir(cwd.as_ref())?,
            config_path: config_path.into(),
            session_json,
        })
    }

    pub fn resolve(&self) -> Result<ResolvedAmbient> {
        let precedence = resolution_precedence();

        if let Some(session_json) = &self.session_json {
            let context = decode_memory_context(session_json)?;
            return Ok(ResolvedAmbient {
                context,
                source: ContextSource::Session,
                explanation: ContextExplanation {
                    selected: ContextSource::Session,
                    summary: format!(
                        "selected {MEMOREE_CONTEXT_ENV}; inherited session context takes precedence"
                    ),
                    cwd: self.cwd.clone(),
                    source_path: None,
                    precedence,
                },
            });
        }

        if let Some(marker_path) = find_marker(&self.cwd)? {
            let marker = read_marker(&marker_path)?;
            return Ok(ResolvedAmbient {
                context: marker.context(),
                source: ContextSource::Marker,
                explanation: ContextExplanation {
                    selected: ContextSource::Marker,
                    summary: "selected the nearest ancestor project marker".into(),
                    cwd: self.cwd.clone(),
                    source_path: Some(marker_path),
                    precedence,
                },
            });
        }

        let settings = load_settings_from(&self.config_path)?;
        if let Some(personal) = settings.personal {
            return Ok(ResolvedAmbient {
                context: personal.into_context(),
                source: ContextSource::Personal,
                explanation: ContextExplanation {
                    selected: ContextSource::Personal,
                    summary:
                        "no session or project marker was present; selected configured personal fallback"
                            .into(),
                    cwd: self.cwd.clone(),
                    source_path: Some(self.config_path.clone()),
                    precedence,
                },
            });
        }

        Err(MemoryError::NoAmbientContext)
    }
}

pub fn resolve(cwd: impl AsRef<Path>) -> Result<ResolvedAmbient> {
    ContextResolver::new(cwd)?.resolve()
}

/// Find the nearest marker without parsing it. An unreadable or broken nearest
/// marker is an error; it is never skipped in favor of a broader context.
pub fn find_marker(start: impl AsRef<Path>) -> Result<Option<PathBuf>> {
    for ancestor in start.as_ref().ancestors() {
        let candidate = ancestor.join(MARKER_FILE);
        match fs::symlink_metadata(&candidate) {
            Ok(_) => return Ok(Some(candidate)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(None)
}

fn normalize_search_dir(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        env::current_dir()?.join(path)
    };
    let canonical = fs::canonicalize(&absolute)?;
    if canonical.is_dir() {
        Ok(canonical)
    } else if canonical.is_file() {
        canonical.parent().map(Path::to_path_buf).ok_or_else(|| {
            MemoryError::Config(format!("cannot search for context from {}", path.display()))
        })
    } else {
        Err(MemoryError::Config(format!(
            "context path is not a directory or regular file: {}",
            path.display()
        )))
    }
}

fn resolution_precedence() -> Vec<ContextSource> {
    vec![
        ContextSource::Session,
        ContextSource::Marker,
        ContextSource::Personal,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample_context() -> AmbientContext {
        AmbientContext {
            workspace_id: "wsp_session".into(),
            project_id: "prj_session".into(),
            task_id: Some("tsk_one".into()),
            component: Some("api".into()),
            pins: vec!["memoree://artifact/art_one@rev_one".into()],
        }
    }

    fn resolver(
        cwd: &Path,
        config_path: &Path,
        session: Option<&AmbientContext>,
    ) -> ContextResolver {
        ContextResolver::from_sources(
            cwd,
            config_path.to_path_buf(),
            session.map(|context| encode_memory_context(context).unwrap()),
        )
        .unwrap()
    }

    #[test]
    fn session_wins_without_touching_a_broken_marker_or_config() {
        let temp = TempDir::new().unwrap();
        fs::write(temp.path().join(MARKER_FILE), "not = [valid").unwrap();
        let config = temp.path().join("broken-config.toml");
        fs::write(&config, "also = [broken").unwrap();

        let resolved = resolver(temp.path(), &config, Some(&sample_context()))
            .resolve()
            .unwrap();

        assert_eq!(resolved.source, ContextSource::Session);
        assert_eq!(resolved.context, sample_context());
        assert_eq!(resolved.explanation.source_path, None);
    }

    #[test]
    fn nearest_marker_wins_and_ids_survive_directory_nesting() {
        let temp = TempDir::new().unwrap();
        let project = temp.path().join("project");
        let nested = project.join("src/deep");
        fs::create_dir_all(&nested).unwrap();
        let outer = init_marker(temp.path(), "outer", Some("wsp_outer")).unwrap();
        let inner = init_marker(&project, "inner", Some("wsp_inner")).unwrap();

        let resolved = resolver(&nested, &temp.path().join("missing.toml"), None)
            .resolve()
            .unwrap();

        assert_eq!(resolved.source, ContextSource::Marker);
        assert_eq!(resolved.context.workspace_id, inner.workspace_id);
        assert_eq!(resolved.context.project_id, inner.project_id);
        assert_ne!(resolved.context.project_id, outer.project_id);
        assert_eq!(
            resolved.explanation.source_path,
            Some(fs::canonicalize(&project).unwrap().join(MARKER_FILE))
        );
    }

    #[test]
    fn marker_wins_without_parsing_broken_lower_priority_settings() {
        let temp = TempDir::new().unwrap();
        let marker = init_marker(temp.path(), "project", None).unwrap();
        let config = temp.path().join("config.toml");
        fs::write(&config, "invalid = [").unwrap();

        let resolved = resolver(temp.path(), &config, None).resolve().unwrap();

        assert_eq!(resolved.source, ContextSource::Marker);
        assert_eq!(resolved.context.project_id, marker.project_id);
    }

    #[test]
    fn broken_nearest_marker_blocks_a_broader_marker() {
        let temp = TempDir::new().unwrap();
        init_marker(temp.path(), "outer", Some("wsp_outer")).unwrap();
        let inner = temp.path().join("inner");
        fs::create_dir(&inner).unwrap();
        fs::write(inner.join(MARKER_FILE), "broken = [").unwrap();

        let error = resolver(&inner, &temp.path().join("missing.toml"), None)
            .resolve()
            .unwrap_err();

        assert!(matches!(error, MemoryError::Config(_)));
    }

    #[test]
    fn configured_personal_context_is_last_resort() {
        let temp = TempDir::new().unwrap();
        let config = temp.path().join("config.toml");
        fs::write(
            &config,
            r#"
schema = 1

[personal]
workspace_id = "wsp_personal"
project_id = "prj_inbox"
pins = ["memoree://artifact/art_pin@rev_one"]
"#,
        )
        .unwrap();

        let resolved = resolver(temp.path(), &config, None).resolve().unwrap();

        assert_eq!(resolved.source, ContextSource::Personal);
        assert_eq!(resolved.context.workspace_id, "wsp_personal");
        assert_eq!(resolved.context.project_id, "prj_inbox");
        assert_eq!(resolved.context.task_id, None);
    }

    #[test]
    fn absent_sources_return_no_ambient_context() {
        let temp = TempDir::new().unwrap();
        let error = resolver(temp.path(), &temp.path().join("missing.toml"), None)
            .resolve()
            .unwrap_err();
        assert!(matches!(error, MemoryError::NoAmbientContext));
    }

    #[test]
    fn init_refuses_overwrite_and_preserves_original_marker() {
        let temp = TempDir::new().unwrap();
        let first = init_marker(temp.path(), "first", Some("wsp_existing")).unwrap();
        let before = fs::read(temp.path().join(MARKER_FILE)).unwrap();

        let error = init_marker(temp.path(), "second", Some("wsp_other")).unwrap_err();
        let after = fs::read(temp.path().join(MARKER_FILE)).unwrap();

        assert!(matches!(error, MemoryError::Config(_)));
        assert_eq!(before, after);
        assert_eq!(read_marker(temp.path().join(MARKER_FILE)).unwrap(), first);
    }

    #[test]
    fn session_encoding_and_task_context_are_lossless() {
        let base = sample_context();
        let child = task_context(&base, "tsk_child").unwrap();
        let encoded = encode_memory_context(&child).unwrap();

        assert_eq!(decode_memory_context(&encoded).unwrap(), child);
        assert_eq!(child.task_id.as_deref(), Some("tsk_child"));
        assert_eq!(child.workspace_id, base.workspace_id);
        assert_eq!(child.project_id, base.project_id);
        assert_eq!(child.pins, base.pins);
        assert!(task_context(&base, "  ").is_err());
    }

    #[test]
    fn resolved_protocol_context_is_never_implicitly_broadened() {
        let temp = TempDir::new().unwrap();
        let resolved = resolver(
            temp.path(),
            &temp.path().join("missing.toml"),
            Some(&sample_context()),
        )
        .resolve()
        .unwrap();

        let protocol = resolved.protocol_context();
        assert_eq!(protocol.horizon, Horizon::Ambient);
        assert!(!protocol.broadened);
    }

    #[test]
    fn invalid_session_is_not_silently_ignored() {
        let temp = TempDir::new().unwrap();
        let resolver = ContextResolver::from_sources(
            temp.path(),
            temp.path().join("missing.toml"),
            Some(r#"{"workspace_id":"","project_id":"prj"}"#.into()),
        )
        .unwrap();

        let error = resolver.resolve().unwrap_err();
        assert!(matches!(error, MemoryError::Config(_)));
    }

    #[test]
    fn unsupported_settings_and_marker_schemas_are_rejected() {
        let temp = TempDir::new().unwrap();
        let config = temp.path().join("config.toml");
        fs::write(&config, "schema = 9").unwrap();
        assert!(matches!(
            load_settings_from(&config),
            Err(MemoryError::Config(_))
        ));

        let marker = temp.path().join(MARKER_FILE);
        fs::write(
            &marker,
            "schema = 9\nworkspace_id = \"w\"\nproject_id = \"p\"\nname = \"n\"\n",
        )
        .unwrap();
        assert!(matches!(read_marker(marker), Err(MemoryError::Config(_))));
    }
}
