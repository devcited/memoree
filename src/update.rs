//! Signed, confirmation-based self-update discovery and installation.
//!
//! The unsigned latest pointer is discovery only. Every executable URL and
//! digest comes from an Ed25519-signed, versioned release manifest.

use std::{
    env,
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, IsTerminal, Write},
    os::unix::{fs::OpenOptionsExt, io::AsRawFd, process::CommandExt},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{DateTime, Utc};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use fs2::FileExt as _;
use schemars::JsonSchema;
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    context::AppPaths,
    error::{MemoryError, Result},
};

pub const AUTO_UPDATE_ENV: &str = "MEMOREE_AUTO_UPDATE";
pub const UPDATE_FEED_ENV: &str = "MEMOREE_UPDATE_FEED_URL";
pub const UPDATE_REEXEC_ENV: &str = "MEMOREE_UPDATE_REEXEC";
pub const MANAGED_INSTALL_ENV: &str = "MEMOREE_MANAGED_INSTALL";
pub const INSTALL_PREFIX_ENV: &str = "MEMOREE_INSTALL_PREFIX";
const ALLOW_INSECURE_UPDATE_ENV: &str = "MEMOREE_UPDATE_ALLOW_INSECURE";
const UPDATE_STATE_SCHEMA: u32 = 1;
const UPDATE_STATE_FILE: &str = "update-state.json";
pub const UPDATE_LOCK_FILE: &str = "auto-update.lock";
const DEFAULT_FEED_URL: &str = "https://memoree.dev/releases/latest.json";
const SIGNING_PUBLIC_KEY_B64: &str = "m64qvSA8wHiltREGcb/XvIqSSBfGb36JRvW9EOKnisA=";
#[cfg(debug_assertions)]
const TEST_SIGNING_PUBLIC_KEY_ENV: &str = "MEMOREE_TEST_UPDATE_PUBLIC_KEY";
const CHECK_INTERVAL_HOURS: i64 = 6;
const FAILURE_RETRY_MINUTES: i64 = 60;
const PROMPT_TIMEOUT_SECONDS: i32 = 10;
const MAX_POINTER_BYTES: usize = 64 * 1024;
const MAX_MANIFEST_BYTES: usize = 256 * 1024;
const MAX_SIGNATURE_BYTES: usize = 1024;
const MAX_INSTALLER_BYTES: usize = 512 * 1024;

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UpdateState {
    #[serde(default = "update_state_schema")]
    pub schema: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub managed_binary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_attempted_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_successful_check_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub available_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub declined_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

const fn update_state_schema() -> u32 {
    UPDATE_STATE_SCHEMA
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct LatestPointer {
    pub schema: u32,
    pub name: String,
    pub version: String,
    pub tag: String,
    pub signed_manifest_url: String,
    pub signature_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReleaseManifest {
    pub schema: u32,
    pub name: String,
    pub version: String,
    pub tag: String,
    pub store_schema_version: i64,
    pub published_at: DateTime<Utc>,
    pub installer: ReleaseInstaller,
    pub targets: Vec<ReleaseTarget>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReleaseInstaller {
    pub url: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ReleaseTarget {
    pub triple: String,
    pub archive_url: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct UpdateStatus {
    pub current_version: String,
    pub target: String,
    pub automatic_checks_enabled: bool,
    pub interactive: bool,
    pub managed_install: bool,
    pub state: UpdateState,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct UpdateCheckReport {
    pub current_version: String,
    pub latest_version: String,
    pub update_available: bool,
    pub target: String,
    pub signature_verified: bool,
    pub store_schema_version: i64,
}

#[derive(Debug, Clone)]
pub struct AvailableUpdate {
    pub manifest: ReleaseManifest,
    pub target: ReleaseTarget,
}

pub struct AutoUpdateLock(File);

impl AutoUpdateLock {
    pub fn acquire(paths: &AppPaths) -> Result<Self> {
        ensure_private_directory(&paths.data_dir)?;
        let path = paths.data_dir.join(UPDATE_LOCK_FILE);
        let file = open_private_file(&path, true)?;
        file.try_lock_exclusive().map_err(|error| {
            MemoryError::Config(format!(
                "another Memoree update owns {}: {error}",
                path.display()
            ))
        })?;
        Ok(Self(file))
    }
}

impl Drop for AutoUpdateLock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutoUpdateOutcome {
    Continue,
    Reexec(PathBuf),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PromptDecision {
    Accept,
    Decline,
    Timeout,
}

pub fn current_version() -> &'static str {
    option_env!("MEMOREE_BUILD_VERSION_OVERRIDE").unwrap_or(env!("CARGO_PKG_VERSION"))
}

pub fn target_triple() -> &'static str {
    if cfg!(all(target_os = "macos", target_arch = "aarch64")) {
        "aarch64-apple-darwin"
    } else if cfg!(all(target_os = "macos", target_arch = "x86_64")) {
        "x86_64-apple-darwin"
    } else if cfg!(all(target_os = "linux", target_arch = "aarch64")) {
        "aarch64-unknown-linux-musl"
    } else if cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        "x86_64-unknown-linux-musl"
    } else {
        "unsupported"
    }
}

pub fn automatic_update_enabled() -> bool {
    !matches!(
        env::var(AUTO_UPDATE_ENV).ok().as_deref(),
        Some("0" | "off" | "false" | "never")
    )
}

pub fn interactive_update_available() -> bool {
    io::stdin().is_terminal()
        && io::stderr().is_terminal()
        && env::var_os("CI").is_none()
        && env::var_os(UPDATE_REEXEC_ENV).is_none()
}

pub fn load_update_state(paths: &AppPaths) -> Result<UpdateState> {
    let path = paths.data_dir.join(UPDATE_STATE_FILE);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(UpdateState {
                schema: UPDATE_STATE_SCHEMA,
                ..UpdateState::default()
            });
        }
        Err(error) => return Err(error.into()),
    };
    let state: UpdateState = serde_json::from_slice(&bytes)?;
    if state.schema != UPDATE_STATE_SCHEMA {
        return Err(MemoryError::Config(format!(
            "unsupported update state schema {} in {}",
            state.schema,
            path.display()
        )));
    }
    Ok(state)
}

pub fn update_status(paths: &AppPaths) -> Result<UpdateStatus> {
    let state = load_update_state(paths)?;
    Ok(UpdateStatus {
        current_version: current_version().into(),
        target: target_triple().into(),
        automatic_checks_enabled: automatic_update_enabled(),
        interactive: interactive_update_available(),
        managed_install: managed_binary_matches(&state)?,
        state,
    })
}

pub fn record_managed_install(paths: &AppPaths) -> Result<()> {
    if env::var_os(MANAGED_INSTALL_ENV).is_none() {
        return Ok(());
    }
    let prefix = env::var_os(INSTALL_PREFIX_ENV)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| MemoryError::Config(format!("{INSTALL_PREFIX_ENV} is required")))?;
    let expected = fs::canonicalize(PathBuf::from(prefix).join("memoree"))?;
    let current = fs::canonicalize(env::current_exe()?)?;
    if current != expected {
        return Err(MemoryError::Config(format!(
            "installer receipt path {} does not match running binary {}",
            expected.display(),
            current.display()
        )));
    }
    let mut state = load_update_state(paths)?;
    state.managed_binary = Some(current.display().to_string());
    state.declined_version = None;
    state.last_error = None;
    write_update_state(paths, &state)
}

pub fn check_for_update(paths: &AppPaths, force: bool) -> Result<Option<AvailableUpdate>> {
    let mut state = load_update_state(paths)?;
    let now = Utc::now();
    if !force && !check_is_due(&state, now) {
        return Ok(None);
    }
    state.last_attempted_at = Some(now);
    write_update_state(paths, &state)?;
    match fetch_available_update(current_version()) {
        Ok(available) => {
            state.last_successful_check_at = Some(now);
            state.available_version = available
                .as_ref()
                .map(|update| update.manifest.version.clone());
            state.last_error = None;
            write_update_state(paths, &state)?;
            Ok(available)
        }
        Err(error) => {
            state.last_error = Some(error.to_string());
            write_update_state(paths, &state)?;
            Err(error)
        }
    }
}

pub fn check_report(paths: &AppPaths) -> Result<UpdateCheckReport> {
    let available = check_for_update(paths, true)?;
    let (latest_version, update_available, store_schema_version) = match available {
        Some(update) => (
            update.manifest.version,
            true,
            update.manifest.store_schema_version,
        ),
        None => (
            current_version().into(),
            false,
            crate::store::SCHEMA_VERSION,
        ),
    };
    Ok(UpdateCheckReport {
        current_version: current_version().into(),
        latest_version,
        update_available,
        target: target_triple().into(),
        signature_verified: true,
        store_schema_version,
    })
}

pub fn apply_available_update(paths: &AppPaths, update: &AvailableUpdate) -> Result<PathBuf> {
    let _lock = AutoUpdateLock::acquire(paths)?;
    let state = load_update_state(paths)?;
    let current_executable = managed_executable(&state)?;
    let install_directory = current_executable.parent().ok_or_else(|| {
        MemoryError::Config("managed Memoree binary has no install directory".into())
    })?;
    verify_install_directory(install_directory)?;

    let installer = fetch_bytes(&update.manifest.installer.url, MAX_INSTALLER_BYTES)?;
    verify_sha256(&installer, &update.manifest.installer.sha256, "installer")?;
    let mut script = tempfile::NamedTempFile::new()?;
    script.write_all(&installer)?;
    script.as_file_mut().sync_all()?;
    let mut permissions = script.as_file().metadata()?.permissions();
    use std::os::unix::fs::PermissionsExt as _;
    permissions.set_mode(0o700);
    script.as_file().set_permissions(permissions)?;

    let status = Command::new("sh")
        .arg(script.path())
        .env("MEMOREE_VERSION", &update.manifest.tag)
        .env("MEMOREE_INSTALL_DIR", install_directory)
        .env("MEMOREE_ARCHIVE_URL", &update.target.archive_url)
        .env("MEMOREE_EXPECTED_ARCHIVE_SHA256", &update.target.sha256)
        .env(MANAGED_INSTALL_ENV, "1")
        .env(INSTALL_PREFIX_ENV, install_directory)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()?;
    if !status.success() {
        return Err(MemoryError::Config(format!(
            "signed Memoree installer exited with {}; the original command was not run",
            status
        )));
    }
    Ok(current_executable)
}

pub fn maybe_auto_update(paths: &AppPaths, command_is_eligible: bool) -> Result<AutoUpdateOutcome> {
    if !command_is_eligible
        || !automatic_update_enabled()
        || !interactive_update_available()
        || target_triple() == "unsupported"
    {
        return Ok(AutoUpdateOutcome::Continue);
    }
    let mut state = match load_update_state(paths) {
        Ok(state) => state,
        Err(error) => {
            eprintln!("memoree: automatic update state is unreadable; continuing safely: {error}");
            return Ok(AutoUpdateOutcome::Continue);
        }
    };
    let managed = match managed_binary_matches(&state) {
        Ok(managed) => managed,
        Err(error) => {
            eprintln!("memoree: automatic update eligibility failed; continuing safely: {error}");
            return Ok(AutoUpdateOutcome::Continue);
        }
    };
    if !managed || !check_is_due(&state, Utc::now()) {
        return Ok(AutoUpdateOutcome::Continue);
    }
    let update = match check_for_update(paths, false) {
        Ok(Some(update)) => update,
        Ok(None) => return Ok(AutoUpdateOutcome::Continue),
        Err(error) => {
            eprintln!("memoree: automatic update check failed safely: {error}");
            return Ok(AutoUpdateOutcome::Continue);
        }
    };
    if state.declined_version.as_deref() == Some(update.manifest.version.as_str()) {
        return Ok(AutoUpdateOutcome::Continue);
    }
    eprintln!(
        "Memoree {} is available (installed {}). Update and reconcile memory now? [y/N]",
        update.manifest.version,
        current_version()
    );
    match prompt_with_timeout()? {
        PromptDecision::Accept => {
            let executable = apply_available_update(paths, &update)?;
            Ok(AutoUpdateOutcome::Reexec(executable))
        }
        PromptDecision::Decline => {
            state = match load_update_state(paths) {
                Ok(state) => state,
                Err(error) => {
                    eprintln!(
                        "memoree: could not persist the deferred update; continuing safely: {error}"
                    );
                    return Ok(AutoUpdateOutcome::Continue);
                }
            };
            state.declined_version = Some(update.manifest.version);
            if let Err(error) = write_update_state(paths, &state) {
                eprintln!(
                    "memoree: could not persist the deferred update; continuing safely: {error}"
                );
                return Ok(AutoUpdateOutcome::Continue);
            }
            eprintln!("memoree: update deferred; you will be asked again for the next version");
            Ok(AutoUpdateOutcome::Continue)
        }
        PromptDecision::Timeout => {
            eprintln!("memoree: update prompt timed out; continuing without changes");
            Ok(AutoUpdateOutcome::Continue)
        }
    }
}

pub fn reexec_current_process(executable: &Path) -> Result<i32> {
    let error = Command::new(executable)
        .args(env::args_os().skip(1))
        .env(UPDATE_REEXEC_ENV, "1")
        .exec();
    Err(MemoryError::Io(error))
}

fn fetch_available_update(current: &str) -> Result<Option<AvailableUpdate>> {
    let pointer_bytes = fetch_bytes(
        &env::var(UPDATE_FEED_ENV).unwrap_or_else(|_| DEFAULT_FEED_URL.into()),
        MAX_POINTER_BYTES,
    )?;
    let pointer: LatestPointer = serde_json::from_slice(&pointer_bytes)?;
    if pointer.schema != 2
        || pointer.name != "memoree"
        || pointer.tag != format!("v{}", pointer.version)
    {
        return Err(MemoryError::Integrity(
            "the release pointer has an unsupported or inconsistent identity".into(),
        ));
    }
    let current_version = Version::parse(current)
        .map_err(|error| MemoryError::Config(format!("invalid current version: {error}")))?;
    let pointer_version = Version::parse(&pointer.version)
        .map_err(|error| MemoryError::Integrity(format!("invalid release version: {error}")))?;
    let manifest_bytes = fetch_bytes(&pointer.signed_manifest_url, MAX_MANIFEST_BYTES)?;
    let signature_bytes = fetch_bytes(&pointer.signature_url, MAX_SIGNATURE_BYTES)?;
    let public_key = update_public_key();
    verify_release_signature(&manifest_bytes, &signature_bytes, &public_key)?;
    let manifest: ReleaseManifest = serde_json::from_slice(&manifest_bytes)?;
    validate_manifest(&manifest, &pointer)?;
    if pointer_version <= current_version {
        return Ok(None);
    }
    let target = manifest
        .targets
        .iter()
        .find(|target| target.triple == target_triple())
        .cloned()
        .ok_or_else(|| {
            MemoryError::Config(format!(
                "release {} has no signed artifact for {}",
                manifest.version,
                target_triple()
            ))
        })?;
    Ok(Some(AvailableUpdate { manifest, target }))
}

fn validate_manifest(manifest: &ReleaseManifest, pointer: &LatestPointer) -> Result<()> {
    if manifest.schema != 1
        || manifest.name != "memoree"
        || manifest.version != pointer.version
        || manifest.tag != pointer.tag
        || manifest.tag != format!("v{}", manifest.version)
        || manifest.targets.is_empty()
    {
        return Err(MemoryError::Integrity(
            "signed release manifest identity does not match the latest pointer".into(),
        ));
    }
    Version::parse(&manifest.version).map_err(|error| {
        MemoryError::Integrity(format!("signed manifest has an invalid version: {error}"))
    })?;
    validate_update_url(&manifest.installer.url)?;
    validate_sha256(&manifest.installer.sha256)?;
    for target in &manifest.targets {
        if target.triple.is_empty() {
            return Err(MemoryError::Integrity(
                "signed manifest contains an empty target".into(),
            ));
        }
        validate_update_url(&target.archive_url)?;
        validate_sha256(&target.sha256)?;
    }
    Ok(())
}

fn verify_release_signature(bytes: &[u8], encoded_signature: &[u8], key_b64: &str) -> Result<()> {
    let key = BASE64
        .decode(key_b64)
        .map_err(|error| MemoryError::Integrity(format!("invalid embedded update key: {error}")))?;
    let key: [u8; 32] = key
        .try_into()
        .map_err(|_| MemoryError::Integrity("embedded update key is not 32 bytes".into()))?;
    let verifying_key = VerifyingKey::from_bytes(&key)
        .map_err(|error| MemoryError::Integrity(format!("invalid embedded update key: {error}")))?;
    let signature_text = std::str::from_utf8(encoded_signature)
        .map_err(|_| MemoryError::Integrity("release signature is not UTF-8".into()))?
        .trim();
    let signature = BASE64.decode(signature_text).map_err(|error| {
        MemoryError::Integrity(format!("release signature is not valid base64: {error}"))
    })?;
    let signature = Signature::from_slice(&signature).map_err(|error| {
        MemoryError::Integrity(format!("release signature has an invalid shape: {error}"))
    })?;
    verifying_key.verify(bytes, &signature).map_err(|_| {
        MemoryError::Integrity(
            "release signature verification failed; no update was executed".into(),
        )
    })
}

fn update_public_key() -> String {
    #[cfg(debug_assertions)]
    if let Ok(key) = env::var(TEST_SIGNING_PUBLIC_KEY_ENV)
        && !key.is_empty()
    {
        return key;
    }
    SIGNING_PUBLIC_KEY_B64.into()
}

fn fetch_bytes(url: &str, max_bytes: usize) -> Result<Vec<u8>> {
    validate_update_url(url)?;
    let config = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(8)))
        .build();
    let agent: ureq::Agent = config.into();
    let mut response = agent
        .get(url)
        .header("User-Agent", concat!("memoree/", env!("CARGO_PKG_VERSION")))
        .call()
        .map_err(|error| MemoryError::Transport(format!("update fetch failed: {error}")))?;
    response
        .body_mut()
        .with_config()
        .limit(u64::try_from(max_bytes).map_err(|_| MemoryError::ContentTooLarge)?)
        .read_to_vec()
        .map_err(|error| MemoryError::Transport(format!("update response failed: {error}")))
}

fn validate_update_url(url: &str) -> Result<()> {
    if url.starts_with("https://") {
        return Ok(());
    }
    if env::var_os(ALLOW_INSECURE_UPDATE_ENV).is_some()
        && (url.starts_with("http://127.0.0.1:") || url.starts_with("http://localhost:"))
    {
        return Ok(());
    }
    Err(MemoryError::Integrity(format!(
        "update URL must use HTTPS: {url}"
    )))
}

fn validate_sha256(value: &str) -> Result<()> {
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(MemoryError::Integrity(
            "signed release manifest contains an invalid SHA-256 digest".into(),
        ))
    }
}

fn verify_sha256(bytes: &[u8], expected: &str, label: &str) -> Result<()> {
    validate_sha256(expected)?;
    let actual = format!("{:x}", Sha256::digest(bytes));
    if actual.eq_ignore_ascii_case(expected) {
        Ok(())
    } else {
        Err(MemoryError::Integrity(format!(
            "signed {label} SHA-256 verification failed; no code was executed"
        )))
    }
}

fn check_is_due(state: &UpdateState, now: DateTime<Utc>) -> bool {
    if let Some(success) = state.last_successful_check_at
        && now.signed_duration_since(success).num_hours() < CHECK_INTERVAL_HOURS
    {
        return false;
    }
    if state.last_error.is_some()
        && let Some(attempt) = state.last_attempted_at
        && now.signed_duration_since(attempt).num_minutes() < FAILURE_RETRY_MINUTES
    {
        return false;
    }
    true
}

fn prompt_with_timeout() -> Result<PromptDecision> {
    let fd = io::stdin().as_raw_fd();
    let mut descriptor = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: `descriptor` is a valid one-element pollfd array and remains
    // alive for the duration of the call.
    let ready = unsafe {
        libc::poll(
            &mut descriptor,
            1,
            PROMPT_TIMEOUT_SECONDS.saturating_mul(1000),
        )
    };
    if ready < 0 {
        return Err(io::Error::last_os_error().into());
    }
    if ready == 0 {
        return Ok(PromptDecision::Timeout);
    }
    let mut answer = String::new();
    io::stdin().read_line(&mut answer)?;
    if matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes") {
        Ok(PromptDecision::Accept)
    } else {
        Ok(PromptDecision::Decline)
    }
}

fn managed_binary_matches(state: &UpdateState) -> Result<bool> {
    let Some(managed) = state.managed_binary.as_deref() else {
        return Ok(false);
    };
    let current = fs::canonicalize(env::current_exe()?)?;
    let managed = match fs::canonicalize(managed) {
        Ok(path) => path,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    Ok(current == managed)
}

fn managed_executable(state: &UpdateState) -> Result<PathBuf> {
    if !managed_binary_matches(state)? {
        return Err(MemoryError::Config(
            "this binary is not a Memoree-installer-managed copy; reinstall with https://memoree.dev/install.sh to enable self-updates"
                .into(),
        ));
    }
    fs::canonicalize(
        state
            .managed_binary
            .as_deref()
            .ok_or_else(|| MemoryError::Config("managed binary receipt is absent".into()))?,
    )
    .map_err(Into::into)
}

fn verify_install_directory(directory: &Path) -> Result<()> {
    let canonical = fs::canonicalize(directory)?;
    let forbidden = ["/usr/bin", "/bin", "/nix/store"];
    if forbidden.iter().any(|path| canonical == Path::new(path))
        || canonical.to_string_lossy().contains("/Cellar/")
    {
        return Err(MemoryError::Config(format!(
            "refusing to self-update a package-manager or system path: {}",
            canonical.display()
        )));
    }
    let probe = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(canonical.join(format!(".memoree-update-probe-{}", std::process::id())))?;
    let path = canonical.join(format!(".memoree-update-probe-{}", std::process::id()));
    drop(probe);
    fs::remove_file(path)?;
    Ok(())
}

fn write_update_state(paths: &AppPaths, state: &UpdateState) -> Result<()> {
    ensure_private_directory(&paths.data_dir)?;
    let mut temporary = tempfile::NamedTempFile::new_in(&paths.data_dir)?;
    temporary.write_all(&serde_json::to_vec_pretty(state)?)?;
    temporary.write_all(b"\n")?;
    temporary.as_file_mut().sync_all()?;
    set_private_permissions(temporary.path())?;
    temporary
        .persist(paths.data_dir.join(UPDATE_STATE_FILE))
        .map_err(|error| error.error)?;
    Ok(())
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    let mut permissions = fs::metadata(path)?.permissions();
    use std::os::unix::fs::PermissionsExt as _;
    permissions.set_mode(0o700);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

fn open_private_file(path: &Path, create: bool) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(create)
        .mode(0o600)
        .open(path)?;
    set_private_permissions(path)?;
    Ok(file)
}

fn set_private_permissions(path: &Path) -> Result<()> {
    let mut permissions = fs::metadata(path)?.permissions();
    use std::os::unix::fs::PermissionsExt as _;
    permissions.set_mode(0o600);
    fs::set_permissions(path, permissions)?;
    Ok(())
}

pub fn command_name_is_auto_update_eligible(command: &str) -> bool {
    !matches!(
        command,
        "call" | "serve" | "daemon" | "upgrade" | "update" | "eval" | "session"
    )
}

pub fn command_name(arguments: impl IntoIterator<Item = impl AsRef<OsStr>>) -> Option<String> {
    let mut skip_next = false;
    for argument in arguments {
        if skip_next {
            skip_next = false;
            continue;
        }
        let value = argument.as_ref().to_string_lossy();
        if value == "--endpoint" {
            skip_next = true;
            continue;
        }
        if value.starts_with("--endpoint=")
            || matches!(value.as_ref(), "--pretty" | "--no-autostart")
        {
            continue;
        }
        if value.starts_with('-') {
            continue;
        }
        return Some(value.into_owned());
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer as _, SigningKey};

    #[test]
    fn signed_manifest_verification_is_exact_and_rejects_tampering() {
        let signing = SigningKey::from_bytes(&[7_u8; 32]);
        let key = BASE64.encode(signing.verifying_key().to_bytes());
        let manifest = br#"{"schema":1,"name":"memoree"}"#;
        let signature = BASE64.encode(signing.sign(manifest).to_bytes());
        verify_release_signature(manifest, signature.as_bytes(), &key).unwrap();
        let error = verify_release_signature(
            br#"{"schema":2,"name":"memoree"}"#,
            signature.as_bytes(),
            &key,
        )
        .unwrap_err();
        assert!(matches!(error, MemoryError::Integrity(_)));
    }

    #[test]
    fn check_cadence_distinguishes_success_and_failure_backoff() {
        let now = Utc::now();
        let successful = UpdateState {
            schema: UPDATE_STATE_SCHEMA,
            last_successful_check_at: Some(now - chrono::Duration::hours(5)),
            ..UpdateState::default()
        };
        assert!(!check_is_due(&successful, now));
        let failed = UpdateState {
            schema: UPDATE_STATE_SCHEMA,
            last_attempted_at: Some(now - chrono::Duration::minutes(30)),
            last_error: Some("offline".into()),
            ..UpdateState::default()
        };
        assert!(!check_is_due(&failed, now));
        assert!(check_is_due(&failed, now + chrono::Duration::minutes(31)));
    }

    #[test]
    fn command_detection_suppresses_protocol_and_lifecycle_surfaces() {
        assert_eq!(
            command_name(["--pretty", "recall", "query"]),
            Some("recall".into())
        );
        assert!(command_name_is_auto_update_eligible("recall"));
        for command in [
            "call", "serve", "daemon", "upgrade", "update", "eval", "session",
        ] {
            assert!(!command_name_is_auto_update_eligible(command));
        }
    }

    #[test]
    fn signed_manifest_allows_additive_forward_compatible_fields() {
        let manifest: ReleaseManifest = serde_json::from_str(
            r#"{
                "schema": 1,
                "name": "memoree",
                "version": "0.4.1",
                "tag": "v0.4.1",
                "store_schema_version": 5,
                "published_at": "2026-07-20T00:00:00Z",
                "installer": {
                    "url": "https://memoree.dev/install.sh",
                    "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                    "future_installer_field": true
                },
                "targets": [{
                    "triple": "x86_64-unknown-linux-musl",
                    "archive_url": "https://example.com/memoree.tar.gz",
                    "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                    "future_target_field": "value"
                }],
                "future_manifest_field": {"value": 1}
            }"#,
        )
        .unwrap();
        assert_eq!(manifest.version, "0.4.1");
        assert_eq!(manifest.targets.len(), 1);
    }
}
