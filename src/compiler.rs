//! Discovery, authentication, model selection, and persisted preference for
//! the caller-side `memoree remember` compiler.

use std::{
    collections::BTreeMap,
    env,
    ffi::{OsStr, OsString},
    fs::{self, OpenOptions},
    io::{self, IsTerminal, Write},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::{process::Command, time::timeout};
use ulid::Ulid;

use crate::{
    context::AppPaths,
    error::{MemoryError, Result},
    remember::{
        CLAUDE_REMEMBER_MODEL, CODEX_REMEMBER_MODEL, ClaudeCompiler, CodexCompiler,
        ValidatedCompilation,
    },
};

const COMPILER_SELECTION_FILE: &str = "compiler-selection.json";
const COMPILER_SELECTION_SCHEMA: u32 = 1;
const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_DISCOVERY_OUTPUT_BYTES: usize = 512 * 1024;
const DISCOVERY_ENV_ALLOWLIST: [&str; 10] = [
    "HOME",
    "CODEX_HOME",
    "CLAUDE_CONFIG_DIR",
    "PATH",
    "TMPDIR",
    "LANG",
    "LC_ALL",
    "TERM",
    "USER",
    "LOGNAME",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompilerProvider {
    Codex,
    Claude,
}

impl CompilerProvider {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
        }
    }

    const fn login_command(self) -> &'static str {
        match self {
            Self::Codex => "codex login",
            Self::Claude => "claude auth login",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderState {
    Missing,
    LoggedOut,
    UnsupportedAuth,
    UnsupportedCli,
    CatalogUnavailable,
    Ready,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompilerModel {
    pub id: String,
    pub display_name: String,
    pub recommended: bool,
    pub selectable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompilerProviderStatus {
    pub provider: CompilerProvider,
    pub state: ProviderState,
    pub installed: bool,
    pub logged_in: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cli_version: Option<String>,
    #[serde(default)]
    pub models: Vec<CompilerModel>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl CompilerProviderStatus {
    fn ready(&self) -> bool {
        self.state == ProviderState::Ready
    }

    fn recommended_model(&self) -> Option<&CompilerModel> {
        self.models
            .iter()
            .find(|model| model.recommended && model.selectable)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectionOrigin {
    Prompted,
    Explicit,
    AutoSingleProvider,
    LegacyCodexDefault,
    ExplicitApiKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompilerSelection {
    #[serde(default = "compiler_selection_schema")]
    pub schema: u32,
    pub provider: CompilerProvider,
    pub model: String,
    pub origin: SelectionOrigin,
    pub cli_version: String,
    pub selected_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolved_model_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_at: Option<DateTime<Utc>>,
}

const fn compiler_selection_schema() -> u32 {
    COMPILER_SELECTION_SCHEMA
}

#[derive(Debug, Clone, Serialize)]
pub struct CompilerStatus {
    pub selection_path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selection: Option<CompilerSelection>,
    pub configuration_required: bool,
    pub providers: Vec<CompilerProviderStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CompilerConfigureReport {
    pub selection_path: PathBuf,
    pub selection: CompilerSelection,
    pub providers: Vec<CompilerProviderStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CompilerUpgradeReport {
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selection: Option<CompilerSelection>,
    pub providers: Vec<CompilerProviderStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CompilerRunReport {
    pub mode: String,
    pub provider: CompilerProvider,
    pub model: String,
    pub cli_version: String,
    pub selection_origin: SelectionOrigin,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolved_model_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CompilerExecution {
    pub compilation: ValidatedCompilation,
    pub report: CompilerRunReport,
}

#[derive(Debug, Clone)]
pub struct CompilerRegistry {
    codex_binary: PathBuf,
    claude_binary: PathBuf,
    discovery_timeout: Duration,
}

impl Default for CompilerRegistry {
    fn default() -> Self {
        Self {
            codex_binary: PathBuf::from("codex"),
            claude_binary: PathBuf::from("claude"),
            discovery_timeout: DISCOVERY_TIMEOUT,
        }
    }
}

impl CompilerRegistry {
    #[cfg(test)]
    fn with_binaries(codex_binary: impl Into<PathBuf>, claude_binary: impl Into<PathBuf>) -> Self {
        Self {
            codex_binary: codex_binary.into(),
            claude_binary: claude_binary.into(),
            discovery_timeout: Duration::from_secs(5),
        }
    }

    pub async fn status(&self, paths: &AppPaths) -> Result<CompilerStatus> {
        let (codex, claude) = tokio::join!(self.discover_codex(), self.discover_claude());
        let providers = vec![codex, claude];
        let selection = load_selection(paths)?;
        let configuration_required =
            selection.is_none() && providers.iter().filter(|provider| provider.ready()).count() > 1;
        Ok(CompilerStatus {
            selection_path: selection_path(paths),
            selection,
            configuration_required,
            providers,
        })
    }

    pub async fn configure(
        &self,
        paths: &AppPaths,
        requested_provider: Option<CompilerProvider>,
        requested_model: Option<String>,
        interactive: bool,
    ) -> Result<CompilerConfigureReport> {
        let status = self.status(paths).await?;
        ensure_discovery_is_usable(&status.providers)?;
        let ready = status
            .providers
            .iter()
            .filter(|provider| provider.ready())
            .collect::<Vec<_>>();
        if ready.is_empty() {
            return Err(no_login_error(&status.providers));
        }

        let provider = match requested_provider {
            Some(provider) => provider,
            None if ready.len() == 1 => ready[0].provider,
            None if interactive => prompt_provider(&ready)?,
            None => {
                return Err(MemoryError::Config(
                    "both Codex and Claude are logged in; choose one with `memoree compiler configure --provider codex --model gpt-5.6-luna` or `memoree compiler configure --provider claude --model sonnet`"
                        .to_owned(),
                ));
            }
        };
        let provider_status = ready
            .iter()
            .find(|candidate| candidate.provider == provider)
            .copied()
            .ok_or_else(|| provider_unavailable_error(provider, &status.providers))?;
        let explicitly_requested = requested_provider.is_some() || requested_model.is_some();
        let model = match requested_model {
            Some(model) => model,
            None if interactive => prompt_model(provider_status)?,
            None => provider_status
                .recommended_model()
                .map(|model| model.id.clone())
                .ok_or_else(|| recommended_model_error(provider, provider_status))?,
        };
        validate_selected_model(provider, provider_status, &model)?;
        let origin = if explicitly_requested {
            SelectionOrigin::Explicit
        } else if ready.len() == 1 {
            SelectionOrigin::AutoSingleProvider
        } else {
            SelectionOrigin::Prompted
        };
        let selection = CompilerSelection {
            schema: COMPILER_SELECTION_SCHEMA,
            provider,
            model,
            origin,
            cli_version: provider_status.cli_version.clone().ok_or_else(|| {
                MemoryError::Config(format!(
                    "{} CLI did not report a version",
                    provider.as_str()
                ))
            })?,
            selected_at: Utc::now(),
            resolved_model_ids: Vec::new(),
            verified_at: None,
        };
        write_selection(paths, &selection)?;
        Ok(CompilerConfigureReport {
            selection_path: selection_path(paths),
            selection,
            providers: status.providers,
        })
    }

    pub async fn reconcile_upgrade(
        &self,
        paths: &AppPaths,
        previous_version: Option<&str>,
    ) -> Result<CompilerUpgradeReport> {
        let status = self.status(paths).await?;
        if let Some(selection) = status.selection {
            return Ok(CompilerUpgradeReport {
                state: "configured".to_owned(),
                selection: Some(selection),
                providers: status.providers,
            });
        }
        if status.providers.iter().any(|provider| {
            provider.logged_in && provider.state == ProviderState::CatalogUnavailable
        }) {
            return Ok(CompilerUpgradeReport {
                state: "catalog_unavailable".to_owned(),
                selection: None,
                providers: status.providers,
            });
        }
        let ready = status
            .providers
            .iter()
            .filter(|provider| provider.ready())
            .collect::<Vec<_>>();
        let chosen = if previous_version.is_some() {
            ready
                .iter()
                .find(|provider| {
                    provider.provider == CompilerProvider::Codex
                        && provider
                            .models
                            .iter()
                            .any(|model| model.id == CODEX_REMEMBER_MODEL && model.selectable)
                })
                .copied()
                .map(|provider| {
                    (
                        provider,
                        CODEX_REMEMBER_MODEL.to_owned(),
                        SelectionOrigin::LegacyCodexDefault,
                    )
                })
        } else {
            None
        }
        .or_else(|| {
            (ready.len() == 1).then(|| {
                let provider = ready[0];
                provider.recommended_model().map(|model| {
                    (
                        provider,
                        model.id.clone(),
                        SelectionOrigin::AutoSingleProvider,
                    )
                })
            })?
        });
        let Some((provider, model, origin)) = chosen else {
            let state = if ready.is_empty() {
                "login_required"
            } else {
                "configuration_required"
            };
            return Ok(CompilerUpgradeReport {
                state: state.to_owned(),
                selection: None,
                providers: status.providers,
            });
        };
        let selection = CompilerSelection {
            schema: COMPILER_SELECTION_SCHEMA,
            provider: provider.provider,
            model,
            origin,
            cli_version: provider
                .cli_version
                .clone()
                .unwrap_or_else(|| "unknown".to_owned()),
            selected_at: Utc::now(),
            resolved_model_ids: Vec::new(),
            verified_at: None,
        };
        write_selection(paths, &selection)?;
        Ok(CompilerUpgradeReport {
            state: "configured".to_owned(),
            selection: Some(selection),
            providers: status.providers,
        })
    }

    pub async fn compile(
        &self,
        paths: &AppPaths,
        source: &str,
        allow_api_key: bool,
        interactive: bool,
    ) -> Result<CompilerExecution> {
        let (mut selection, persist_selection) = if allow_api_key {
            let codex = self.discover_codex().await;
            if !codex.installed {
                return Err(MemoryError::Config(
                    "`--allow-api-key` was provided, but the Codex CLI is not installed".to_owned(),
                ));
            }
            (
                CompilerSelection {
                    schema: COMPILER_SELECTION_SCHEMA,
                    provider: CompilerProvider::Codex,
                    model: CODEX_REMEMBER_MODEL.to_owned(),
                    origin: SelectionOrigin::ExplicitApiKey,
                    cli_version: codex.cli_version.unwrap_or_else(|| "unknown".to_owned()),
                    selected_at: Utc::now(),
                    resolved_model_ids: Vec::new(),
                    verified_at: None,
                },
                false,
            )
        } else {
            (self.resolve_selection(paths, interactive).await?, true)
        };

        let output = match selection.provider {
            CompilerProvider::Codex => {
                CodexCompiler::with_binary(&self.codex_binary, &selection.model, allow_api_key)
                    .compile(source)
                    .await?
            }
            CompilerProvider::Claude => {
                if allow_api_key {
                    return Err(MemoryError::InvalidRequest(
                        "API-key fallback is supported only for an explicit Codex invocation"
                            .to_owned(),
                    ));
                }
                ClaudeCompiler::with_binary(&self.claude_binary, &selection.model)
                    .compile(source)
                    .await?
            }
        };
        selection.resolved_model_ids = output.resolved_model_ids.clone();
        selection.verified_at = Some(Utc::now());
        if persist_selection {
            write_selection(paths, &selection)?;
        }
        let mode = match (selection.provider, allow_api_key) {
            (CompilerProvider::Codex, true) => "codex_cli_api_key_permitted",
            (CompilerProvider::Codex, false) => "codex_cli_chatgpt",
            (CompilerProvider::Claude, false) => "claude_cli_subscription",
            (CompilerProvider::Claude, true) => unreachable!("guarded above"),
        };
        Ok(CompilerExecution {
            compilation: output.compilation,
            report: CompilerRunReport {
                mode: mode.to_owned(),
                provider: selection.provider,
                model: selection.model,
                cli_version: selection.cli_version,
                selection_origin: selection.origin,
                resolved_model_ids: output.resolved_model_ids,
            },
        })
    }

    async fn resolve_selection(
        &self,
        paths: &AppPaths,
        interactive: bool,
    ) -> Result<CompilerSelection> {
        let status = self.status(paths).await?;
        ensure_discovery_is_usable(&status.providers)?;
        if let Some(mut selection) = status.selection {
            validate_selection(&selection)?;
            let provider = status
                .providers
                .iter()
                .find(|provider| provider.provider == selection.provider)
                .ok_or_else(|| provider_unavailable_error(selection.provider, &status.providers))?;
            if !provider.ready() {
                return Err(provider_unavailable_error(
                    selection.provider,
                    &status.providers,
                ));
            }
            validate_selected_model(selection.provider, provider, &selection.model)?;
            selection.cli_version = provider.cli_version.clone().ok_or_else(|| {
                MemoryError::Config(format!(
                    "{} CLI did not report a version",
                    selection.provider.as_str()
                ))
            })?;
            return Ok(selection);
        }

        let ready = status
            .providers
            .iter()
            .filter(|provider| provider.ready())
            .collect::<Vec<_>>();
        if ready.is_empty() {
            return Err(no_login_error(&status.providers));
        }
        let (provider_status, origin) = if ready.len() == 1 {
            (ready[0], SelectionOrigin::AutoSingleProvider)
        } else if interactive {
            let provider = prompt_provider(&ready)?;
            (
                ready
                    .iter()
                    .find(|status| status.provider == provider)
                    .copied()
                    .expect("prompt selection came from ready providers"),
                SelectionOrigin::Prompted,
            )
        } else {
            return Err(MemoryError::Config(
                "both Codex and Claude are logged in and no compiler preference is configured; run `memoree compiler configure` in a terminal, or choose explicitly with `memoree compiler configure --provider codex --model gpt-5.6-luna` or `memoree compiler configure --provider claude --model sonnet`"
                    .to_owned(),
            ));
        };
        let model = if origin == SelectionOrigin::Prompted {
            prompt_model(provider_status)?
        } else {
            provider_status
                .recommended_model()
                .map(|model| model.id.clone())
                .ok_or_else(|| recommended_model_error(provider_status.provider, provider_status))?
        };
        let selection = CompilerSelection {
            schema: COMPILER_SELECTION_SCHEMA,
            provider: provider_status.provider,
            model,
            origin,
            cli_version: provider_status.cli_version.clone().ok_or_else(|| {
                MemoryError::Config(format!(
                    "{} CLI did not report a version",
                    provider_status.provider.as_str()
                ))
            })?,
            selected_at: Utc::now(),
            resolved_model_ids: Vec::new(),
            verified_at: None,
        };
        write_selection(paths, &selection)?;
        Ok(selection)
    }

    async fn discover_codex(&self) -> CompilerProviderStatus {
        let version = match self.run(&self.codex_binary, ["--version"]).await {
            Ok(output) if output.success => first_line(&output.stdout),
            Ok(output) => {
                return provider_error(
                    CompilerProvider::Codex,
                    ProviderState::UnsupportedCli,
                    true,
                    false,
                    None,
                    None,
                    format!("`codex --version` exited with {}", output.status),
                );
            }
            Err(ProbeError::Missing) => return missing_provider(CompilerProvider::Codex),
            Err(ProbeError::Failed(error)) => {
                return provider_error(
                    CompilerProvider::Codex,
                    ProviderState::UnsupportedCli,
                    true,
                    false,
                    None,
                    None,
                    error,
                );
            }
        };
        let auth = match self.run(&self.codex_binary, ["login", "status"]).await {
            Ok(output) if output.success => output,
            Ok(output) => {
                return CompilerProviderStatus {
                    provider: CompilerProvider::Codex,
                    state: ProviderState::LoggedOut,
                    installed: true,
                    logged_in: false,
                    auth_method: None,
                    cli_version: version,
                    models: Vec::new(),
                    error: Some(format!(
                        "Codex is not logged in with ChatGPT; run `{}` (status {})",
                        CompilerProvider::Codex.login_command(),
                        output.status
                    )),
                };
            }
            Err(error) => {
                return provider_error(
                    CompilerProvider::Codex,
                    ProviderState::UnsupportedCli,
                    true,
                    false,
                    None,
                    version,
                    error.to_string(),
                );
            }
        };
        let auth_output = auth.combined_output();
        let auth_text = String::from_utf8_lossy(&auth_output);
        if !auth_text.contains("Logged in using ChatGPT") {
            return CompilerProviderStatus {
                provider: CompilerProvider::Codex,
                state: ProviderState::UnsupportedAuth,
                installed: true,
                logged_in: false,
                auth_method: Some(
                    first_line(&auth_output).unwrap_or_else(|| "unknown".to_owned()),
                ),
                cli_version: version,
                models: Vec::new(),
                error: Some(
                    "Codex authentication is not a ChatGPT login; API-key auth is excluded from automatic discovery"
                        .to_owned(),
                ),
            };
        }
        let catalog = match self.run(&self.codex_binary, ["debug", "models"]).await {
            Ok(output) if output.success => output,
            Ok(output) => {
                return provider_error(
                    CompilerProvider::Codex,
                    ProviderState::CatalogUnavailable,
                    true,
                    true,
                    Some("chatgpt".to_owned()),
                    version,
                    format!("Codex model catalog exited with {}", output.status),
                );
            }
            Err(error) => {
                return provider_error(
                    CompilerProvider::Codex,
                    ProviderState::CatalogUnavailable,
                    true,
                    true,
                    Some("chatgpt".to_owned()),
                    version,
                    error.to_string(),
                );
            }
        };
        let models = match parse_codex_models(&catalog.stdout) {
            Ok(models) if !models.is_empty() => models,
            Ok(_) => {
                return provider_error(
                    CompilerProvider::Codex,
                    ProviderState::CatalogUnavailable,
                    true,
                    true,
                    Some("chatgpt".to_owned()),
                    version,
                    "Codex returned an empty selectable model catalog".to_owned(),
                );
            }
            Err(error) => {
                return provider_error(
                    CompilerProvider::Codex,
                    ProviderState::CatalogUnavailable,
                    true,
                    true,
                    Some("chatgpt".to_owned()),
                    version,
                    error,
                );
            }
        };
        CompilerProviderStatus {
            provider: CompilerProvider::Codex,
            state: ProviderState::Ready,
            installed: true,
            logged_in: true,
            auth_method: Some("chatgpt".to_owned()),
            cli_version: version,
            models,
            error: None,
        }
    }

    async fn discover_claude(&self) -> CompilerProviderStatus {
        let version = match self.run(&self.claude_binary, ["--version"]).await {
            Ok(output) if output.success => first_line(&output.stdout),
            Ok(output) => {
                return provider_error(
                    CompilerProvider::Claude,
                    ProviderState::UnsupportedCli,
                    true,
                    false,
                    None,
                    None,
                    format!("`claude --version` exited with {}", output.status),
                );
            }
            Err(ProbeError::Missing) => return missing_provider(CompilerProvider::Claude),
            Err(ProbeError::Failed(error)) => {
                return provider_error(
                    CompilerProvider::Claude,
                    ProviderState::UnsupportedCli,
                    true,
                    false,
                    None,
                    None,
                    error,
                );
            }
        };
        let auth = match self
            .run(&self.claude_binary, ["auth", "status", "--json"])
            .await
        {
            Ok(output) if output.success => output,
            Ok(output) => {
                return CompilerProviderStatus {
                    provider: CompilerProvider::Claude,
                    state: ProviderState::LoggedOut,
                    installed: true,
                    logged_in: false,
                    auth_method: None,
                    cli_version: version,
                    models: Vec::new(),
                    error: Some(format!(
                        "Claude is not logged in; run `{}` (status {})",
                        CompilerProvider::Claude.login_command(),
                        output.status
                    )),
                };
            }
            Err(error) => {
                return provider_error(
                    CompilerProvider::Claude,
                    ProviderState::UnsupportedCli,
                    true,
                    false,
                    None,
                    version,
                    error.to_string(),
                );
            }
        };
        let auth: ClaudeAuthStatus = match serde_json::from_slice(&auth.stdout) {
            Ok(auth) => auth,
            Err(error) => {
                return provider_error(
                    CompilerProvider::Claude,
                    ProviderState::UnsupportedCli,
                    true,
                    false,
                    None,
                    version,
                    format!("Claude auth status was not valid JSON: {error}"),
                );
            }
        };
        if !auth.logged_in {
            return CompilerProviderStatus {
                provider: CompilerProvider::Claude,
                state: ProviderState::LoggedOut,
                installed: true,
                logged_in: false,
                auth_method: auth.auth_method,
                cli_version: version,
                models: Vec::new(),
                error: Some(format!(
                    "Claude is not logged in; run `{}`",
                    CompilerProvider::Claude.login_command()
                )),
            };
        }
        let supported_auth = auth
            .auth_method
            .as_deref()
            .is_some_and(|method| matches!(method, "claude.ai" | "oauth" | "subscription"))
            && auth
                .api_provider
                .as_deref()
                .is_none_or(|provider| provider == "firstParty");
        if !supported_auth {
            return CompilerProviderStatus {
                provider: CompilerProvider::Claude,
                state: ProviderState::UnsupportedAuth,
                installed: true,
                logged_in: false,
                auth_method: auth.auth_method,
                cli_version: version,
                models: Vec::new(),
                error: Some(
                    "Claude authentication is not a claude.ai subscription login; API-key and third-party provider auth are excluded from automatic discovery"
                        .to_owned(),
                ),
            };
        }
        let catalog = match self
            .run(
                &self.claude_binary,
                [
                    "--print",
                    "/model",
                    "--output-format",
                    "json",
                    "--no-session-persistence",
                    "--safe-mode",
                    "--tools",
                    "",
                ],
            )
            .await
        {
            Ok(output) if output.success => output,
            Ok(output) => {
                return provider_error(
                    CompilerProvider::Claude,
                    ProviderState::CatalogUnavailable,
                    true,
                    true,
                    auth.auth_method,
                    version,
                    format!("Claude model catalog exited with {}", output.status),
                );
            }
            Err(error) => {
                return provider_error(
                    CompilerProvider::Claude,
                    ProviderState::CatalogUnavailable,
                    true,
                    true,
                    auth.auth_method,
                    version,
                    error.to_string(),
                );
            }
        };
        let models = match parse_claude_models(&catalog.stdout) {
            Ok(models) if !models.is_empty() => models,
            Ok(_) => {
                return provider_error(
                    CompilerProvider::Claude,
                    ProviderState::CatalogUnavailable,
                    true,
                    true,
                    auth.auth_method,
                    version,
                    "Claude returned an empty model catalog".to_owned(),
                );
            }
            Err(error) => {
                return provider_error(
                    CompilerProvider::Claude,
                    ProviderState::CatalogUnavailable,
                    true,
                    true,
                    auth.auth_method,
                    version,
                    error,
                );
            }
        };
        CompilerProviderStatus {
            provider: CompilerProvider::Claude,
            state: ProviderState::Ready,
            installed: true,
            logged_in: true,
            auth_method: auth.auth_method,
            cli_version: version,
            models,
            error: None,
        }
    }

    async fn run<I, S>(
        &self,
        binary: &Path,
        arguments: I,
    ) -> std::result::Result<Capture, ProbeError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut command = Command::new(binary);
        command
            .args(arguments)
            .env_clear()
            .envs(discovery_environment())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let child = command.spawn().map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                ProbeError::Missing
            } else {
                ProbeError::Failed(format!("could not start {}: {error}", binary.display()))
            }
        })?;
        let output = timeout(self.discovery_timeout, child.wait_with_output())
            .await
            .map_err(|_| {
                ProbeError::Failed(format!(
                    "{} exceeded the {} second discovery limit",
                    binary.display(),
                    self.discovery_timeout.as_secs()
                ))
            })?
            .map_err(|error| {
                ProbeError::Failed(format!(
                    "could not read {} output: {error}",
                    binary.display()
                ))
            })?;
        if output.stdout.len() > MAX_DISCOVERY_OUTPUT_BYTES
            || output.stderr.len() > MAX_DISCOVERY_OUTPUT_BYTES
        {
            return Err(ProbeError::Failed(format!(
                "{} discovery output exceeded the configured limit",
                binary.display()
            )));
        }
        Ok(Capture {
            success: output.status.success(),
            status: output.status,
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

#[derive(Debug)]
struct Capture {
    success: bool,
    status: std::process::ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl Capture {
    fn combined_output(&self) -> Vec<u8> {
        let mut combined = self.stdout.clone();
        if !combined.is_empty() && !self.stderr.is_empty() {
            combined.push(b'\n');
        }
        combined.extend_from_slice(&self.stderr);
        combined
    }
}

#[derive(Debug, thiserror::Error)]
enum ProbeError {
    #[error("CLI is not installed")]
    Missing,
    #[error("{0}")]
    Failed(String),
}

#[derive(Debug, Deserialize)]
struct CodexCatalog {
    models: Vec<CodexCatalogModel>,
}

#[derive(Debug, Deserialize)]
struct CodexCatalogModel {
    slug: String,
    display_name: String,
    visibility: String,
}

#[derive(Debug, Deserialize)]
struct ClaudeAuthStatus {
    #[serde(default, rename = "loggedIn")]
    logged_in: bool,
    #[serde(default, rename = "authMethod")]
    auth_method: Option<String>,
    #[serde(default, rename = "apiProvider")]
    api_provider: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ClaudeCatalogEnvelope {
    #[serde(default)]
    is_error: bool,
    result: String,
}

fn parse_codex_models(bytes: &[u8]) -> std::result::Result<Vec<CompilerModel>, String> {
    let catalog: CodexCatalog = serde_json::from_slice(bytes)
        .map_err(|error| format!("Codex model catalog was not valid JSON: {error}"))?;
    Ok(catalog
        .models
        .into_iter()
        .filter(|model| model.visibility == "list")
        .map(|model| CompilerModel {
            recommended: model.slug == CODEX_REMEMBER_MODEL,
            selectable: true,
            id: model.slug,
            display_name: model.display_name,
        })
        .collect())
}

fn parse_claude_models(bytes: &[u8]) -> std::result::Result<Vec<CompilerModel>, String> {
    let envelope: ClaudeCatalogEnvelope = serde_json::from_slice(bytes)
        .map_err(|error| format!("Claude model catalog envelope was not valid JSON: {error}"))?;
    if envelope.is_error {
        return Err("Claude model catalog command reported an error".to_owned());
    }
    let available = envelope
        .result
        .split_once("Available:")
        .map(|(_, available)| available)
        .ok_or_else(|| "Claude model catalog did not contain an Available list".to_owned())?;
    let mut models = Vec::new();
    for raw in available.split(',') {
        let id = raw
            .trim()
            .trim_end_matches('.')
            .trim_start_matches("or ")
            .trim();
        if id.is_empty() || id == "a full model ID" {
            continue;
        }
        if id.len() > 128
            || !id
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || "-_.[]".contains(character))
        {
            return Err(format!("Claude returned an invalid model alias `{id}`"));
        }
        models.push(CompilerModel {
            recommended: id == CLAUDE_REMEMBER_MODEL,
            selectable: !matches!(id, "best" | "default"),
            id: id.to_owned(),
            display_name: id.to_owned(),
        });
    }
    Ok(models)
}

fn validate_selection(selection: &CompilerSelection) -> Result<()> {
    if selection.schema != COMPILER_SELECTION_SCHEMA {
        return Err(MemoryError::Config(format!(
            "unsupported compiler selection schema {}; expected {COMPILER_SELECTION_SCHEMA}; run `memoree compiler configure`",
            selection.schema
        )));
    }
    if selection.model.trim().is_empty() || selection.model.len() > 128 {
        return Err(MemoryError::Config(
            "compiler selection has an invalid model; run `memoree compiler configure`".to_owned(),
        ));
    }
    Ok(())
}

fn validate_selected_model(
    provider: CompilerProvider,
    status: &CompilerProviderStatus,
    model: &str,
) -> Result<()> {
    let discovered = status.models.iter().find(|candidate| candidate.id == model);
    match discovered {
        Some(model) if model.selectable => Ok(()),
        Some(_) => Err(MemoryError::Config(format!(
            "{} model alias `{model}` is floating and is not accepted for durable compiler selection; choose an explicit alias with `memoree compiler configure`",
            provider.as_str()
        ))),
        None => Err(MemoryError::Config(format!(
            "configured {} model `{model}` is not present in the live CLI catalog; run `memoree compiler configure`",
            provider.as_str()
        ))),
    }
}

fn ensure_discovery_is_usable(providers: &[CompilerProviderStatus]) -> Result<()> {
    let catalog_errors = providers
        .iter()
        .filter(|provider| {
            provider.logged_in && provider.state == ProviderState::CatalogUnavailable
        })
        .collect::<Vec<_>>();
    if catalog_errors.is_empty() {
        return Ok(());
    }
    let details = catalog_errors
        .iter()
        .map(|provider| {
            format!(
                "{}: {}",
                provider.provider.as_str(),
                provider.error.as_deref().unwrap_or("catalog unavailable")
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    Err(MemoryError::Reasoner {
        message: format!(
            "compiler model discovery failed transiently ({details}); the stored preference was not changed"
        ),
        retryable: true,
    })
}

fn no_login_error(providers: &[CompilerProviderStatus]) -> MemoryError {
    let states = providers
        .iter()
        .map(|provider| format!("{}={:?}", provider.provider.as_str(), provider.state))
        .collect::<Vec<_>>()
        .join(", ");
    MemoryError::Config(format!(
        "no eligible compiler CLI login is available ({states}); run `codex login` or `claude auth login`, then run `memoree compiler configure`"
    ))
}

fn provider_unavailable_error(
    provider: CompilerProvider,
    providers: &[CompilerProviderStatus],
) -> MemoryError {
    let status = providers.iter().find(|status| status.provider == provider);
    let detail = status
        .and_then(|status| status.error.as_deref())
        .unwrap_or("provider is not ready");
    MemoryError::Config(format!(
        "configured {} compiler is unavailable: {detail}; run `{}` or `memoree compiler configure`",
        provider.as_str(),
        provider.login_command()
    ))
}

fn recommended_model_error(
    provider: CompilerProvider,
    status: &CompilerProviderStatus,
) -> MemoryError {
    MemoryError::Config(format!(
        "the live {} catalog does not contain the recommended model; available selectable models: {}; run `memoree compiler configure --provider {}` with one of them",
        provider.as_str(),
        status
            .models
            .iter()
            .filter(|model| model.selectable)
            .map(|model| model.id.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        provider.as_str()
    ))
}

fn prompt_provider(ready: &[&CompilerProviderStatus]) -> Result<CompilerProvider> {
    eprintln!("Memoree found multiple authenticated compiler CLIs:");
    for (index, provider) in ready.iter().enumerate() {
        let recommendation = provider
            .recommended_model()
            .map(|model| model.id.as_str())
            .unwrap_or("no recommended model");
        eprintln!(
            "  {}) {} ({recommendation})",
            index + 1,
            provider.provider.as_str()
        );
    }
    let index = prompt_index("Select compiler provider", ready.len(), 0)?;
    Ok(ready[index].provider)
}

fn prompt_model(provider: &CompilerProviderStatus) -> Result<String> {
    let models = provider
        .models
        .iter()
        .filter(|model| model.selectable)
        .collect::<Vec<_>>();
    if models.is_empty() {
        return Err(recommended_model_error(provider.provider, provider));
    }
    eprintln!("Available {} compiler models:", provider.provider.as_str());
    let default = models
        .iter()
        .position(|model| model.recommended)
        .unwrap_or(0);
    for (index, model) in models.iter().enumerate() {
        let suffix = if model.recommended {
            " (recommended)"
        } else {
            ""
        };
        eprintln!("  {}) {}{}", index + 1, model.display_name, suffix);
    }
    let index = prompt_index("Select compiler model", models.len(), default)?;
    Ok(models[index].id.clone())
}

fn prompt_index(label: &str, count: usize, default: usize) -> Result<usize> {
    eprint!("{label} [{}]: ", default + 1);
    io::stderr().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let input = input.trim();
    if input.is_empty() {
        return Ok(default);
    }
    let value = input.parse::<usize>().map_err(|_| {
        MemoryError::InvalidRequest(format!("selection must be a number between 1 and {count}"))
    })?;
    if !(1..=count).contains(&value) {
        return Err(MemoryError::InvalidRequest(format!(
            "selection must be a number between 1 and {count}"
        )));
    }
    Ok(value - 1)
}

pub fn interactive_selection_available() -> bool {
    io::stdin().is_terminal()
        && io::stderr().is_terminal()
        && env::var_os("CI").is_none()
        && env::var_os("MEMOREE_NONINTERACTIVE").is_none()
}

fn load_selection(paths: &AppPaths) -> Result<Option<CompilerSelection>> {
    let path = selection_path(paths);
    let source = match fs::read(&path) {
        Ok(source) => source,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if source.len() > 16 * 1024 {
        return Err(MemoryError::Config(format!(
            "compiler selection {} is oversized",
            path.display()
        )));
    }
    let selection: CompilerSelection = serde_json::from_slice(&source).map_err(|error| {
        MemoryError::Config(format!(
            "could not parse compiler selection {}: {error}",
            path.display()
        ))
    })?;
    validate_selection(&selection)?;
    Ok(Some(selection))
}

fn write_selection(paths: &AppPaths, selection: &CompilerSelection) -> Result<()> {
    validate_selection(selection)?;
    ensure_private_directory(&paths.data_dir)?;
    let destination = selection_path(paths);
    if fs::symlink_metadata(&destination).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err(MemoryError::Config(format!(
            "refusing to replace symlinked compiler selection {}",
            destination.display()
        )));
    }
    let temporary = paths
        .data_dir
        .join(format!(".{COMPILER_SELECTION_FILE}.tmp-{}", Ulid::r#gen()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    let mut serialized = serde_json::to_vec_pretty(selection)?;
    serialized.push(b'\n');
    let publication = (|| -> Result<()> {
        file.write_all(&serialized)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, &destination)?;
        sync_directory(&paths.data_dir)?;
        Ok(())
    })();
    if publication.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    publication
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    if fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err(MemoryError::Config(format!(
            "refusing to use symlinked compiler state directory {}",
            path.display()
        )));
    }
    fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

fn selection_path(paths: &AppPaths) -> PathBuf {
    paths.data_dir.join(COMPILER_SELECTION_FILE)
}

fn discovery_environment() -> BTreeMap<OsString, OsString> {
    let mut environment = BTreeMap::new();
    for key in DISCOVERY_ENV_ALLOWLIST {
        if let Some(value) = env::var_os(key) {
            environment.insert(OsString::from(key), value);
        }
    }
    environment
}

fn first_line(bytes: &[u8]) -> Option<String> {
    String::from_utf8_lossy(bytes)
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_owned)
}

fn missing_provider(provider: CompilerProvider) -> CompilerProviderStatus {
    CompilerProviderStatus {
        provider,
        state: ProviderState::Missing,
        installed: false,
        logged_in: false,
        auth_method: None,
        cli_version: None,
        models: Vec::new(),
        error: Some(format!("{} CLI is not installed", provider.as_str())),
    }
}

fn provider_error(
    provider: CompilerProvider,
    state: ProviderState,
    installed: bool,
    logged_in: bool,
    auth_method: Option<String>,
    cli_version: Option<String>,
    error: String,
) -> CompilerProviderStatus {
    CompilerProviderStatus {
        provider,
        state,
        installed,
        logged_in,
        auth_method,
        cli_version,
        models: Vec::new(),
        error: Some(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt as _;

    fn paths(root: &Path) -> AppPaths {
        AppPaths {
            data_dir: root.join("data"),
            runtime_dir: root.join("run"),
            socket_path: root.join("run/memoree.sock"),
            config_path: root.join("config.toml"),
        }
    }

    fn executable(path: &Path, source: &str) {
        fs::write(path, source).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }

    #[test]
    fn claude_catalog_is_queried_and_floating_aliases_are_not_selectable() {
        let envelope = serde_json::json!({
            "is_error": false,
            "result": "Current model: Opus. Usage: /model <name>. Available: sonnet, opus, haiku, fable, best, default, or a full model ID."
        });
        let models = parse_claude_models(&serde_json::to_vec(&envelope).unwrap()).unwrap();
        assert!(
            models
                .iter()
                .any(|model| model.id == "sonnet" && model.recommended)
        );
        assert!(
            models
                .iter()
                .any(|model| model.id == "best" && !model.selectable)
        );
        assert!(
            models
                .iter()
                .any(|model| model.id == "default" && !model.selectable)
        );
    }

    #[tokio::test]
    async fn single_subscription_provider_is_selected_and_persisted() {
        let directory = tempfile::tempdir().unwrap();
        let codex = directory.path().join("missing-codex");
        let claude = directory.path().join("claude");
        executable(
            &claude,
            r#"#!/bin/sh
if [ "$1" = "--version" ]; then printf '%s\n' '2.1.215 (Claude Code)'; exit 0; fi
if [ "$1" = "auth" ]; then printf '%s' '{"loggedIn":true,"authMethod":"claude.ai","apiProvider":"firstParty"}'; exit 0; fi
if [ "$1" = "--print" ] && [ "$2" = "/model" ]; then printf '%s' '{"is_error":false,"result":"Available: sonnet, opus, haiku, fable, best, default, or a full model ID."}'; exit 0; fi
cat >/dev/null
printf '%s' '{"is_error":false,"structured_output":{"claims":[{"claim_type":"decision","statement":"Use SQLite.","evidence_quotes":["Use SQLite."]}]},"modelUsage":{"claude-sonnet-5":{}}}'
"#,
        );
        let registry = CompilerRegistry::with_binaries(codex, claude);
        let app_paths = paths(directory.path());
        let execution = registry
            .compile(&app_paths, "Use SQLite.", false, false)
            .await
            .unwrap();
        assert_eq!(execution.report.provider, CompilerProvider::Claude);
        assert_eq!(execution.report.model, CLAUDE_REMEMBER_MODEL);
        assert_eq!(
            execution.report.selection_origin,
            SelectionOrigin::AutoSingleProvider
        );
        let stored = load_selection(&app_paths).unwrap().unwrap();
        assert_eq!(stored.provider, CompilerProvider::Claude);
        assert_eq!(stored.resolved_model_ids, ["claude-sonnet-5"]);
        assert_eq!(
            fs::metadata(selection_path(&app_paths))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[tokio::test]
    async fn two_ready_providers_require_explicit_noninteractive_selection() {
        let directory = tempfile::tempdir().unwrap();
        let codex = directory.path().join("codex");
        let claude = directory.path().join("claude");
        executable(
            &codex,
            r#"#!/bin/sh
if [ "$1" = "--version" ]; then printf '%s\n' 'codex-cli 0.144.5'; exit 0; fi
if [ "$1" = "login" ]; then printf '%s\n' 'Logged in using ChatGPT'; exit 0; fi
printf '%s' '{"models":[{"slug":"gpt-5.6-luna","display_name":"GPT-5.6-Luna","visibility":"list"}]}'
"#,
        );
        executable(
            &claude,
            r#"#!/bin/sh
if [ "$1" = "--version" ]; then printf '%s\n' '2.1.215 (Claude Code)'; exit 0; fi
if [ "$1" = "auth" ]; then printf '%s' '{"loggedIn":true,"authMethod":"claude.ai","apiProvider":"firstParty"}'; exit 0; fi
printf '%s' '{"is_error":false,"result":"Available: sonnet, opus, or a full model ID."}'
"#,
        );
        let registry = CompilerRegistry::with_binaries(codex, claude);
        let error = registry
            .compile(&paths(directory.path()), "Use SQLite.", false, false)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("both Codex and Claude"));
        assert!(error.to_string().contains("memoree compiler configure"));
    }

    #[tokio::test]
    async fn upgrade_preserves_the_legacy_codex_luna_default_when_both_are_ready() {
        let directory = tempfile::tempdir().unwrap();
        let codex = directory.path().join("codex");
        let claude = directory.path().join("claude");
        executable(
            &codex,
            r#"#!/bin/sh
if [ "$1" = "--version" ]; then printf '%s\n' 'codex-cli 0.144.5'; exit 0; fi
if [ "$1" = "login" ]; then printf '%s\n' 'Logged in using ChatGPT' >&2; exit 0; fi
printf '%s' '{"models":[{"slug":"gpt-5.6-luna","display_name":"GPT-5.6-Luna","visibility":"list"}]}'
"#,
        );
        executable(
            &claude,
            r#"#!/bin/sh
if [ "$1" = "--version" ]; then printf '%s\n' '2.1.215 (Claude Code)'; exit 0; fi
if [ "$1" = "auth" ]; then printf '%s' '{"loggedIn":true,"authMethod":"claude.ai","apiProvider":"firstParty"}'; exit 0; fi
printf '%s' '{"is_error":false,"result":"Available: sonnet, opus, or a full model ID."}'
"#,
        );
        let app_paths = paths(directory.path());
        let report = CompilerRegistry::with_binaries(codex, claude)
            .reconcile_upgrade(&app_paths, Some("0.2.0"))
            .await
            .unwrap();
        assert_eq!(report.state, "configured");
        let selection = report.selection.unwrap();
        assert_eq!(selection.provider, CompilerProvider::Codex);
        assert_eq!(selection.model, CODEX_REMEMBER_MODEL);
        assert_eq!(selection.origin, SelectionOrigin::LegacyCodexDefault);
        assert_eq!(
            load_selection(&app_paths).unwrap().unwrap().origin,
            SelectionOrigin::LegacyCodexDefault
        );
    }

    #[tokio::test]
    async fn api_key_auth_is_not_counted_as_an_automatic_login() {
        let directory = tempfile::tempdir().unwrap();
        let codex = directory.path().join("codex");
        let claude = directory.path().join("claude");
        executable(
            &codex,
            r#"#!/bin/sh
if [ "$1" = "--version" ]; then printf '%s\n' 'codex-cli test'; exit 0; fi
printf '%s\n' 'Logged in using an API key'
"#,
        );
        executable(
            &claude,
            r#"#!/bin/sh
if [ "$1" = "--version" ]; then printf '%s\n' 'claude test'; exit 0; fi
printf '%s' '{"loggedIn":true,"authMethod":"apiKey","apiProvider":"firstParty"}'
"#,
        );
        let status = CompilerRegistry::with_binaries(codex, claude)
            .status(&paths(directory.path()))
            .await
            .unwrap();
        assert!(status.providers.iter().all(|provider| !provider.logged_in));
        assert!(
            status
                .providers
                .iter()
                .all(|provider| provider.state == ProviderState::UnsupportedAuth)
        );
    }
}
