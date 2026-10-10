//! Antfly storage and decision client shared by Codex persistence backends.
//!
//! Every Codex store that persists into Antfly goes through [`Antfly`], which
//! wraps one [`Backend`]: an embedded `.aflite` database driven through
//! `libantfly`, or a remote Antfly instance reached over HTTP. Both speak the
//! same JSON contracts, so stores are written once against the backend trait.

mod backend;
mod config;
mod embedded;
mod error;
pub mod keys;
mod remote;
mod runtime;
pub mod schema;
pub mod sql;

pub use backend::Backend;
pub use backend::BackendFuture;
pub use backend::DenseIndex;
pub use backend::Document;
pub use backend::LEGACY_TABLE;
pub use backend::ScanRequest;
pub use backend::SearchHit;
pub use backend::TableSpec;
pub use backend::Write;
pub use config::AntflyConfig;
pub use config::AntflyTomlSettings;
pub use config::ApprovalMode;
pub use config::ApprovalSettings;
pub use config::BackendConfig;
pub use config::DEFAULT_DECIDE_MODEL;
pub use config::EmbedderConfig;
pub use embedded::EmbeddedBackend;
pub use embedded::LocalDecider;
pub use error::AntflyError;
pub use error::AntflyResult;
pub use remote::RemoteBackend;
pub use runtime::Antfly;
pub use runtime::Documents;
pub use runtime::SEARCH_TEXT_FIELD;
pub use runtime::shared;
pub use runtime::strip_reserved;
