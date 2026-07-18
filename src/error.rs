use crate::protocol::ErrorCode;

#[derive(Debug, thiserror::Error)]
pub enum MemoryError {
    #[error("no ambient memory context could be resolved")]
    NoAmbientContext,
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error(
        "revision conflict: {entity_type} {entity_id} is at revision {current_revision}, not {requested_revision}"
    )]
    RevisionConflict {
        entity_type: &'static str,
        entity_id: String,
        current_revision: String,
        requested_revision: String,
    },
    #[error("idempotency conflict for key {0}")]
    IdempotencyConflict(String),
    #[error(
        "index is not ready through commit sequence {requested}; current sequence is {current}"
    )]
    IndexNotReady { requested: i64, current: i64 },
    #[error("scope violation: {0}")]
    ScopeViolation(String),
    #[error("content exceeds the configured size limit")]
    ContentTooLarge,
    #[error("integrity check failed: {0}")]
    Integrity(String),
    #[error("unsupported protocol version {0}")]
    UnsupportedVersion(u32),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("configuration error: {0}")]
    Config(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("reasoning adapter error: {message}")]
    Reasoner { message: String, retryable: bool },
}

impl MemoryError {
    pub fn code(&self) -> ErrorCode {
        match self {
            Self::NoAmbientContext => ErrorCode::NoAmbientContext,
            Self::InvalidRequest(_) => ErrorCode::InvalidRequest,
            Self::NotFound(_) => ErrorCode::NotFound,
            Self::RevisionConflict { .. } => ErrorCode::RevisionConflict,
            Self::IdempotencyConflict(_) => ErrorCode::IdempotencyConflict,
            Self::IndexNotReady { .. } => ErrorCode::IndexNotReady,
            Self::ScopeViolation(_) => ErrorCode::ScopeViolation,
            Self::ContentTooLarge => ErrorCode::ContentTooLarge,
            Self::Integrity(_) => ErrorCode::IntegrityError,
            Self::UnsupportedVersion(_) => ErrorCode::UnsupportedVersion,
            Self::Config(_) => ErrorCode::ConfigError,
            Self::Io(_) | Self::Database(_) | Self::Json(_) => ErrorCode::InternalError,
            Self::Transport(_) => ErrorCode::TransportError,
            Self::Reasoner { .. } => ErrorCode::ReasonerError,
        }
    }

    pub fn retryable(&self) -> bool {
        matches!(self, Self::Transport(_) | Self::IndexNotReady { .. })
            || matches!(
                self,
                Self::Reasoner {
                    retryable: true,
                    ..
                }
            )
    }

    pub fn exit_code(&self) -> i32 {
        match self {
            Self::InvalidRequest(_)
            | Self::UnsupportedVersion(_)
            | Self::Config(_)
            | Self::ContentTooLarge => 2,
            Self::NotFound(_) => 3,
            Self::RevisionConflict { .. } | Self::IdempotencyConflict(_) => 4,
            Self::IndexNotReady { .. } => 6,
            Self::NoAmbientContext => 5,
            Self::ScopeViolation(_) => 7,
            _ => 1,
        }
    }
}

pub type Result<T> = std::result::Result<T, MemoryError>;
