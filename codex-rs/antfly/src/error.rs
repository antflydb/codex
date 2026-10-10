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
    /// A SQL statement failed; `code` is the SQLSTATE when the database
    /// reported one.
    #[error("antfly SQL failed ({}): {message}", code.as_deref().unwrap_or("no sqlstate"))]
    Sql {
        code: Option<String>,
        message: String,
    },
}

impl AntflyError {
    /// SQLSTATE of a failed SQL statement.
    pub fn sqlstate(&self) -> Option<&str> {
        match self {
            AntflyError::Sql { code, .. } => code.as_deref(),
            _ => None,
        }
    }

    /// A concurrent transaction committed first (40001); retry the whole
    /// transaction.
    pub fn is_conflict(&self) -> bool {
        self.sqlstate() == Some("40001")
    }

    /// A UNIQUE or PRIMARY KEY constraint rejected the write (23505).
    pub fn is_unique_violation(&self) -> bool {
        self.sqlstate() == Some("23505")
    }
}

pub type AntflyResult<T> = Result<T, AntflyError>;

impl From<serde_json::Error> for AntflyError {
    fn from(err: serde_json::Error) -> Self {
        AntflyError::Malformed(err.to_string())
    }
}
