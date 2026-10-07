/// Errors returned by Antfly backends.
#[derive(Debug, thiserror::Error)]
pub enum AntflyError {
    /// The embedded `libantfly` call failed.
    #[error("antfly embedded call failed: {0}")]
    Embedded(String),
    /// The remote Antfly request failed.
    #[error("antfly request failed: {0}")]
    Remote(String),
    /// A response could not be decoded.
    #[error("malformed antfly response: {0}")]
    Malformed(String),
    /// The dedicated executor thread pool shut down.
    #[error("antfly executor is unavailable")]
    ExecutorUnavailable,
    /// The configuration is invalid.
    #[error("invalid antfly configuration: {0}")]
    Config(String),
    /// The backend does not support the requested operation.
    #[error("antfly backend does not support {0}")]
    Unsupported(&'static str),
}

pub type AntflyResult<T> = Result<T, AntflyError>;

impl From<serde_json::Error> for AntflyError {
    fn from(err: serde_json::Error) -> Self {
        AntflyError::Malformed(err.to_string())
    }
}
