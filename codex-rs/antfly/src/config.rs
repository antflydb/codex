use std::path::PathBuf;

use serde::Deserialize;
use serde::Serialize;

use crate::error::AntflyError;

/// Where Codex state lives.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackendConfig {
    /// A local `.aflite` database opened in-process through `libantfly`.
    Embedded { path: PathBuf },
    /// A remote Antfly or Antfly Cloud instance.
    Remote {
        /// Base URL, for example `https://host/cloud/v1/<instance_id>`.
        url: String,
        /// Table that holds all Codex state.
        table: String,
        /// Environment variable that holds a bearer token, if any.
        api_key_env: Option<String>,
    },
    /// A local `.aflite` database whose writes are replicated to a remote
    /// instance through a durable outbox. Works offline; the remote catches
    /// up when reachable.
    Replicated {
        path: PathBuf,
        url: String,
        table: String,
        api_key_env: Option<String>,
        /// Search the remote replica (which can include other machines'
        /// threads) instead of the local copy.
        search_remote: bool,
    },
}

/// Embedding model used for semantic search over Codex state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbedderConfig {
    /// Model id as registered with Antfly inference.
    pub model: String,
    /// Output dimensions of `model`.
    pub dims: u32,
}

impl Default for EmbedderConfig {
    fn default() -> Self {
        Self {
            model: "BAAI/bge-small-en-v1.5".to_string(),
            dims: 384,
        }
    }
}

/// Complete Antfly configuration for one Codex process.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AntflyConfig {
    pub backend: BackendConfig,
    /// Models directory for embedded inference. Defaults to Antfly's own
    /// default (`~/.antfly/inference/models`) when unset.
    pub models_dir: Option<PathBuf>,
    /// Embedder for semantic search. `None` disables dense indexing and
    /// search falls back to full text only.
    pub embedder: Option<EmbedderConfig>,
    /// Typed-decision model used for approvals.
    pub decide_model: String,
    pub approvals: ApprovalSettings,
}

/// How approval requests are reviewed with typed decisions.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalMode {
    /// Approvals go to the user as usual.
    #[default]
    Off,
    /// Decisions are computed and logged; the user still decides.
    Shadow,
    /// Confident decisions are applied; everything else goes to the user.
    Enforce,
}

/// Approval review settings. Thresholds are in basis points (1/10000) so the
/// configuration stays `Eq`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApprovalSettings {
    pub mode: ApprovalMode,
    /// Allow when P(safe) is at least this.
    pub allow_threshold_bp: u32,
    /// Deny when P(destructive) is at least this.
    pub deny_threshold_bp: u32,
}

impl Default for ApprovalSettings {
    fn default() -> Self {
        Self {
            mode: ApprovalMode::Off,
            allow_threshold_bp: 9_000,
            deny_threshold_bp: 9_500,
        }
    }
}

/// Raw settings as read from `config.toml`, before defaults are applied.
#[derive(Clone, Debug, Default)]
pub struct AntflyTomlSettings {
    pub codex_home: PathBuf,
    pub path: Option<PathBuf>,
    pub url: Option<String>,
    pub table: Option<String>,
    pub api_key_env: Option<String>,
    pub models_dir: Option<PathBuf>,
    pub embedder_model: Option<String>,
    pub embedder_dims: Option<u32>,
    pub semantic_search: Option<bool>,
    pub search_remote: Option<bool>,
    pub decide_model: Option<String>,
    pub approvals_mode: Option<String>,
    pub allow_threshold: Option<f64>,
    pub deny_threshold: Option<f64>,
}

fn expand_home(path: PathBuf) -> PathBuf {
    let Ok(rest) = path.strip_prefix("~") else {
        return path;
    };
    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home).join(rest),
        None => path,
    }
}

fn basis_points(name: &str, value: Option<f64>, default: u32) -> Result<u32, AntflyError> {
    match value {
        None => Ok(default),
        Some(value) if (0.0..=1.0).contains(&value) => Ok((value * 10_000.0).round() as u32),
        Some(value) => Err(AntflyError::Config(format!(
            "{name} must be between 0 and 1, got {value}"
        ))),
    }
}

impl AntflyConfig {
    /// Embedded configuration with default models.
    pub fn embedded(path: impl Into<PathBuf>) -> Self {
        Self {
            backend: BackendConfig::Embedded { path: path.into() },
            models_dir: None,
            embedder: Some(EmbedderConfig::default()),
            decide_model: "laya".to_string(),
            approvals: ApprovalSettings::default(),
        }
    }

    /// Applies defaults to settings read from `config.toml`.
    pub fn from_toml(settings: AntflyTomlSettings) -> Result<Self, AntflyError> {
        let local_path = settings.path.map(expand_home);
        let table = settings.table.unwrap_or_else(|| "codex".to_string());
        let backend = match (settings.url, local_path) {
            (Some(url), Some(path)) => BackendConfig::Replicated {
                path,
                url,
                table,
                api_key_env: settings.api_key_env,
                search_remote: settings.search_remote.unwrap_or(false),
            },
            (Some(url), None) => BackendConfig::Remote {
                url,
                table,
                api_key_env: settings.api_key_env,
            },
            (None, path) => BackendConfig::Embedded {
                path: path.unwrap_or_else(|| settings.codex_home.join("antfly.aflite")),
            },
        };
        let embedder = if settings.semantic_search == Some(false) {
            None
        } else {
            let default = EmbedderConfig::default();
            Some(EmbedderConfig {
                model: settings.embedder_model.unwrap_or(default.model),
                dims: settings.embedder_dims.unwrap_or(default.dims),
            })
        };
        let defaults = ApprovalSettings::default();
        let mode = match settings.approvals_mode.as_deref() {
            None | Some("off") => ApprovalMode::Off,
            Some("shadow") => ApprovalMode::Shadow,
            Some("enforce") => ApprovalMode::Enforce,
            Some(other) => {
                return Err(AntflyError::Config(format!(
                    "approvals.mode must be off, shadow, or enforce; got {other}"
                )));
            }
        };
        Ok(Self {
            backend,
            models_dir: settings.models_dir.map(expand_home),
            embedder,
            decide_model: settings.decide_model.unwrap_or_else(|| "laya".to_string()),
            approvals: ApprovalSettings {
                mode,
                allow_threshold_bp: basis_points(
                    "approvals.allow_threshold",
                    settings.allow_threshold,
                    defaults.allow_threshold_bp,
                )?,
                deny_threshold_bp: basis_points(
                    "approvals.deny_threshold",
                    settings.deny_threshold,
                    defaults.deny_threshold_bp,
                )?,
            },
        })
    }
}
