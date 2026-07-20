//! Local upgrade reconciliation and canonical agent-skill distribution.

use std::{
    env,
    fs::{self, File, OpenOptions},
    io::Write as _,
    path::{Path, PathBuf},
};

use chrono::{DateTime, Utc};
use fs2::FileExt as _;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::{
    context::AppPaths,
    error::{MemoryError, Result},
};

pub const SKILL_SYNC_OPT_OUT_ENV: &str = "MEMOREE_SKIP_SKILL_SYNC";
pub const UPGRADE_STATE_SCHEMA: u32 = 1;
const UPGRADE_STATE_FILE: &str = "upgrade-state.json";
const UPGRADE_LOCK_FILE: &str = "upgrade.lock";
const INTEGRATION_BACKUP_DIRECTORY: &str = "integration-backups";
const CANONICAL_SKILL: &str = include_str!("../skills/use-memoree/SKILL.md");

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct UpgradeState {
    pub schema: u32,
    pub target_version: String,
    pub phase: String,
    pub prior_daemon_running: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_daemon_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migration_backup: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store_schema_version: Option<i64>,
    pub skill_digest: String,
    pub updated_at: DateTime<Utc>,
}

impl UpgradeState {
    pub fn new(prior_daemon_running: bool, previous_daemon_version: Option<String>) -> Self {
        Self {
            schema: UPGRADE_STATE_SCHEMA,
            target_version: env!("CARGO_PKG_VERSION").into(),
            phase: "starting".into(),
            prior_daemon_running,
            previous_daemon_version,
            migration_backup: None,
            store_schema_version: None,
            skill_digest: canonical_skill_digest(),
            updated_at: Utc::now(),
        }
    }

    pub fn set_phase(&mut self, phase: &str) {
        self.phase = phase.into();
        self.updated_at = Utc::now();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SkillSyncItem {
    pub agent: String,
    pub action: String,
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_backup: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SkillSyncReport {
    pub version: String,
    pub digest: String,
    pub opted_out: bool,
    pub items: Vec<SkillSyncItem>,
}

pub struct UpgradeLock(File);

impl UpgradeLock {
    pub fn acquire(paths: &AppPaths) -> Result<Self> {
        ensure_private_directory(&paths.data_dir)?;
        let path = paths.data_dir.join(UPGRADE_LOCK_FILE);
        let file = open_private_file(&path, true)?;
        file.try_lock_exclusive().map_err(|error| {
            MemoryError::Config(format!(
                "another Memoree upgrade owns {}: {error}",
                path.display()
            ))
        })?;
        Ok(Self(file))
    }
}

impl Drop for UpgradeLock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

pub fn ensure_upgrade_not_in_progress(paths: &AppPaths) -> Result<()> {
    for (path, activity) in [
        (
            paths.data_dir.join(UPGRADE_LOCK_FILE),
            "upgrade reconciliation",
        ),
        (
            paths.data_dir.join(crate::update::UPDATE_LOCK_FILE),
            "signed automatic update",
        ),
    ] {
        if !path.exists() {
            continue;
        }
        let file = open_private_file(&path, false)?;
        file.try_lock_shared().map_err(|_| {
            MemoryError::Transport(format!(
                "a Memoree {activity} is in progress; retry after it completes"
            ))
        })?;
        file.unlock()?;
    }
    Ok(())
}

pub fn canonical_skill_digest() -> String {
    blake3::hash(CANONICAL_SKILL.as_bytes())
        .to_hex()
        .to_string()
}

pub fn load_upgrade_state(paths: &AppPaths) -> Result<Option<UpgradeState>> {
    let path = paths.data_dir.join(UPGRADE_STATE_FILE);
    if !path.is_file() {
        return Ok(None);
    }
    let state: UpgradeState = serde_json::from_slice(&fs::read(&path)?)?;
    if state.schema != UPGRADE_STATE_SCHEMA {
        return Err(MemoryError::Config(format!(
            "unsupported upgrade state schema {} in {}",
            state.schema,
            path.display()
        )));
    }
    Ok(Some(state))
}

pub fn write_upgrade_state(paths: &AppPaths, state: &UpgradeState) -> Result<()> {
    ensure_private_directory(&paths.data_dir)?;
    let path = paths.data_dir.join(UPGRADE_STATE_FILE);
    let mut temporary = tempfile::NamedTempFile::new_in(&paths.data_dir)?;
    temporary.write_all(&serde_json::to_vec_pretty(state)?)?;
    temporary.write_all(b"\n")?;
    temporary.as_file_mut().sync_all()?;
    set_private_permissions(temporary.path())?;
    temporary.persist(&path).map_err(|error| error.error)?;
    sync_directory_best_effort(&paths.data_dir);
    Ok(())
}

pub fn sync_skills(paths: &AppPaths) -> Result<SkillSyncReport> {
    let digest = canonical_skill_digest();
    if env_flag(SKILL_SYNC_OPT_OUT_ENV) {
        return Ok(SkillSyncReport {
            version: env!("CARGO_PKG_VERSION").into(),
            digest,
            opted_out: true,
            items: Vec::new(),
        });
    }

    sync_skills_to_roots(paths, agent_roots()?)
}

fn sync_skills_to_roots(
    paths: &AppPaths,
    roots: Vec<(&'static str, PathBuf)>,
) -> Result<SkillSyncReport> {
    let digest = canonical_skill_digest();

    let mut items = Vec::new();
    for (agent, root) in roots {
        if !root.exists() {
            items.push(SkillSyncItem {
                agent: agent.into(),
                action: "skipped".into(),
                path: root.display().to_string(),
                previous_backup: None,
                reason: Some("agent home does not exist".into()),
            });
            continue;
        }
        if !root.is_dir() {
            return Err(MemoryError::Config(format!(
                "{agent} agent home is not a directory: {}",
                root.display()
            )));
        }
        let canonical_root = fs::canonicalize(&root)?;
        let skills_root = canonical_root.join("skills");
        refuse_symlink_if_present(&skills_root)?;
        ensure_private_directory(&skills_root)?;
        let skill_dir = skills_root.join("use-memoree");
        refuse_symlink_if_present(&skill_dir)?;
        ensure_private_directory(&skill_dir)?;
        let skill_path = skill_dir.join("SKILL.md");
        refuse_symlink_if_present(&skill_path)?;

        let existing = if skill_path.is_file() {
            Some(fs::read(&skill_path)?)
        } else {
            None
        };
        if existing.as_deref() == Some(CANONICAL_SKILL.as_bytes()) {
            items.push(SkillSyncItem {
                agent: agent.into(),
                action: "unchanged".into(),
                path: skill_path.display().to_string(),
                previous_backup: None,
                reason: None,
            });
            continue;
        }

        let previous_backup = match existing.as_deref() {
            Some(bytes) => Some(backup_existing_skill(paths, agent, bytes)?),
            None => None,
        };
        atomic_write_skill(&skill_dir, &skill_path)?;
        items.push(SkillSyncItem {
            agent: agent.into(),
            action: if existing.is_some() {
                "updated".into()
            } else {
                "installed".into()
            },
            path: skill_path.display().to_string(),
            previous_backup,
            reason: None,
        });
    }

    Ok(SkillSyncReport {
        version: env!("CARGO_PKG_VERSION").into(),
        digest,
        opted_out: false,
        items,
    })
}

fn agent_roots() -> Result<Vec<(&'static str, PathBuf)>> {
    let home = env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| MemoryError::Config("HOME is required for agent skill sync".into()))?;
    let codex = env::var_os("CODEX_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".codex"));
    let claude = env::var_os("CLAUDE_CONFIG_DIR")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".claude"));
    Ok(vec![("codex", codex), ("claude", claude)])
}

fn backup_existing_skill(paths: &AppPaths, agent: &str, bytes: &[u8]) -> Result<String> {
    let root = paths.data_dir.join(INTEGRATION_BACKUP_DIRECTORY);
    ensure_private_directory(&root)?;
    let directory = root.join(format!(
        "{agent}-use-memoree-{}-{}",
        Utc::now().format("%Y%m%dT%H%M%SZ"),
        Ulid::r#gen()
    ));
    ensure_private_directory(&directory)?;
    let path = directory.join("SKILL.md");
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    set_private_permissions(&path)?;
    sync_directory_best_effort(&directory);
    Ok(path.display().to_string())
}

fn atomic_write_skill(directory: &Path, destination: &Path) -> Result<()> {
    let mut temporary = tempfile::NamedTempFile::new_in(directory)?;
    temporary.write_all(CANONICAL_SKILL.as_bytes())?;
    temporary.as_file_mut().sync_all()?;
    set_private_permissions(temporary.path())?;
    temporary
        .persist(destination)
        .map_err(|error| error.error)?;
    sync_directory_best_effort(directory);
    Ok(())
}

fn refuse_symlink_if_present(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(MemoryError::Config(format!(
            "refusing to sync through symlink: {}",
            path.display()
        ))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
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

fn open_private_file(path: &Path, create: bool) -> Result<File> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create(create)
        .truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    set_private_permissions(path)?;
    Ok(file)
}

fn set_private_permissions(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn sync_directory_best_effort(path: &Path) {
    if let Ok(directory) = File::open(path) {
        let _ = directory.sync_all();
    }
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_skill_has_valid_identity_and_stable_digest() {
        assert!(CANONICAL_SKILL.starts_with("---\nname: use-memoree\n"));
        assert_eq!(canonical_skill_digest().len(), 64);
    }

    #[test]
    fn skill_sync_is_atomic_idempotent_and_preserves_a_different_copy() {
        let temporary = tempfile::tempdir().unwrap();
        let paths = AppPaths {
            data_dir: temporary.path().join("memoree data"),
            runtime_dir: temporary.path().join("run"),
            socket_path: temporary.path().join("run/memoree.sock"),
            config_path: temporary.path().join("config.toml"),
        };
        let codex = temporary.path().join("agent homes/codex");
        let claude = temporary.path().join("agent homes/claude");
        fs::create_dir_all(codex.join("skills/use-memoree")).unwrap();
        fs::create_dir_all(&claude).unwrap();
        fs::write(
            codex.join("skills/use-memoree/SKILL.md"),
            b"user-modified skill",
        )
        .unwrap();

        let first = sync_skills_to_roots(
            &paths,
            vec![("codex", codex.clone()), ("claude", claude.clone())],
        )
        .unwrap();
        assert_eq!(first.items[0].action, "updated");
        assert_eq!(first.items[1].action, "installed");
        let backup = first.items[0].previous_backup.as_ref().unwrap();
        assert_eq!(fs::read(backup).unwrap(), b"user-modified skill");
        assert_eq!(
            fs::read(codex.join("skills/use-memoree/SKILL.md")).unwrap(),
            CANONICAL_SKILL.as_bytes()
        );

        let second =
            sync_skills_to_roots(&paths, vec![("codex", codex), ("claude", claude)]).unwrap();
        assert!(second.items.iter().all(|item| item.action == "unchanged"));
    }

    #[cfg(unix)]
    #[test]
    fn skill_sync_refuses_a_symlinked_skill_root() {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir().unwrap();
        let paths = AppPaths {
            data_dir: temporary.path().join("data"),
            runtime_dir: temporary.path().join("run"),
            socket_path: temporary.path().join("run/memoree.sock"),
            config_path: temporary.path().join("config.toml"),
        };
        let codex = temporary.path().join("codex");
        let redirected = temporary.path().join("redirected");
        fs::create_dir_all(&codex).unwrap();
        fs::create_dir_all(&redirected).unwrap();
        symlink(&redirected, codex.join("skills")).unwrap();
        let error = sync_skills_to_roots(&paths, vec![("codex", codex)])
            .expect_err("symlinked skill roots must be rejected");
        assert!(matches!(error, MemoryError::Config(message) if message.contains("symlink")));
    }
}
