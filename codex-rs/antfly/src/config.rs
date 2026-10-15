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
    /// A remote Antfly instance: document tables over HTTP and relational
    /// tables over its PostgreSQL wire listener.
    Remote {
        /// Base URL, for example `https://host/cloud/v1/<instance_id>`.
        url: String,
        /// PostgreSQL connection URL for the instance's SQL listener, for
        /// example `postgres://codex:secret@host:5432/default`.
        sql_url: Option<String>,
        /// Environment variable that holds a bearer token, if any.
        api_key_env: Option<String>,
    },
}

/// Embedding model used for semantic search over Codex state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbedderConfig {
    /// Model id as registered with Antfly inference.
    pub model: String,
    /// Matryoshka truncation of `model`'s output. `None` uses the model's
    /// native size, which Antfly detects by probing the embedder.
    pub dims: Option<u32>,
}

impl Default for EmbedderConfig {
    fn default() -> Self {
        Self {
            model: "google/embeddinggemma-2".to_string(),
            dims: None,
        }
    }
}

/// Default GLiNER extractor that links knowledge to concepts.
pub const DEFAULT_EXTRACT_MODEL: &str = "fastino/gliner2.5-base-v1";

/// The knowledge graph: claims and memory notes linked to the concepts
/// they are about (see `crate::knowledge`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeConfig {
    /// GLiNER model that extracts concepts, run by Antfly enrichments and
    /// at query time.
    pub extract_model: String,
    /// A new claim supersedes an earlier one when the decide model is at
    /// least this confident, in basis points.
    pub supersede_threshold_bp: u32,
    /// Per-turn claim extraction; `None` turns it off.
    pub claims: Option<ClaimPipelineConfig>,
    /// Concept names to merge, as `(alias, canonical name)`.
    pub concept_aliases: Vec<(String, String)>,
}

impl Default for KnowledgeConfig {
    fn default() -> Self {
        Self {
            extract_model: DEFAULT_EXTRACT_MODEL.to_string(),
            supersede_threshold_bp: 8_000,
            claims: Some(ClaimPipelineConfig::default()),
            concept_aliases: Vec::new(),
        }
    }
}

/// When the per-turn claim pipeline runs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimPipelineConfig {
    /// Quiet period after a turn ends before its thread is extracted.
    pub debounce_seconds: u64,
    /// A thread idle this long has its provisional claims settled.
    pub settle_minutes: u64,
}

impl Default for ClaimPipelineConfig {
    fn default() -> Self {
        Self {
            debounce_seconds: 120,
            settle_minutes: 30,
        }
    }
}

/// Default typed-decision model for approval review: a prepared Laya
/// checkpoint under `<models dir>/convaiinnovations/laya`.
pub const DEFAULT_DECIDE_MODEL: &str = "convaiinnovations/laya";

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
    /// Typed-decision model used for approvals and claim supersession.
    pub decide_model: String,
    pub approvals: ApprovalSettings,
    /// `None` disables concept extraction and the knowledge graph indexes.
    pub knowledge: Option<KnowledgeConfig>,
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
    /// Allow when the action is at least this likely to stay read-only or
    /// inside the project.
    pub allow_threshold_bp: u32,
    /// Deny when the action is at least this likely to be destructive.
    pub deny_threshold_bp: u32,
}

impl Default for ApprovalSettings {
    fn default() -> Self {
        Self {
            mode: ApprovalMode::Off,
            allow_threshold_bp: 7_000,
            deny_threshold_bp: 8_000,
        }
    }
}

/// Raw settings as read from `config.toml`, before defaults are applied.
#[derive(Clone, Debug, Default)]
pub struct AntflyTomlSettings {
    pub codex_home: PathBuf,
    pub path: Option<PathBuf>,
    pub url: Option<String>,
    pub sql_url: Option<String>,
    pub api_key_env: Option<String>,
    pub models_dir: Option<PathBuf>,
    pub embedder_model: Option<String>,
    pub embedder_dims: Option<u32>,
    pub semantic_search: Option<bool>,
    pub decide_model: Option<String>,
    pub approvals_mode: Option<String>,
    pub allow_threshold: Option<f64>,
    pub deny_threshold: Option<f64>,
    pub knowledge_graph: Option<bool>,
    pub extract_model: Option<String>,
    pub supersede_threshold: Option<f64>,
    pub claim_extraction: Option<bool>,
    pub claim_debounce_seconds: Option<u64>,
    pub claim_settle_minutes: Option<u64>,
    pub concept_aliases: Option<std::collections::BTreeMap<String, String>>,
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
            decide_model: DEFAULT_DECIDE_MODEL.to_string(),
            approvals: ApprovalSettings::default(),
            knowledge: Some(KnowledgeConfig::default()),
        }
    }

    /// Applies defaults to settings read from `config.toml`.
    pub fn from_toml(settings: AntflyTomlSettings) -> Result<Self, AntflyError> {
        let local_path = settings.path.map(expand_home);
        let backend = match (settings.url, local_path) {
            (Some(_), Some(_)) => {
                return Err(AntflyError::Config(
                    "set either antfly.path (embedded) or antfly.url (remote), not both; \
                     the replicated backend was removed with the move to SQL tables"
                        .to_string(),
                ));
            }
            (Some(url), None) => BackendConfig::Remote {
                url,
                sql_url: settings.sql_url,
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
                dims: settings.embedder_dims.or(default.dims),
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
        let knowledge = if settings.knowledge_graph == Some(false) {
            None
        } else {
            let default = KnowledgeConfig::default();
            Some(KnowledgeConfig {
                extract_model: settings.extract_model.unwrap_or(default.extract_model),
                supersede_threshold_bp: basis_points(
                    "supersede_threshold",
                    settings.supersede_threshold,
                    default.supersede_threshold_bp,
                )?,
                concept_aliases: settings
                    .concept_aliases
                    .clone()
                    .unwrap_or_default()
                    .into_iter()
                    .collect(),
                claims: (settings.claim_extraction != Some(false)).then(|| {
                    let default = ClaimPipelineConfig::default();
                    ClaimPipelineConfig {
                        debounce_seconds: settings
                            .claim_debounce_seconds
                            .unwrap_or(default.debounce_seconds),
                        settle_minutes: settings
                            .claim_settle_minutes
                            .unwrap_or(default.settle_minutes),
                    }
                }),
            })
        };
        Ok(Self {
            backend,
            knowledge,
            models_dir: settings.models_dir.map(expand_home),
            embedder,
            decide_model: settings
                .decide_model
                .unwrap_or_else(|| DEFAULT_DECIDE_MODEL.to_string()),
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
