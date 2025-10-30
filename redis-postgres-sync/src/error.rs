use thiserror::Error;

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),

    #[error("PostgreSQL error: {0}")]
    Sqlx(#[from] sqlx::Error),

    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Processing error: {0}")]
    Processing(String),

    #[error("Circuit breaker open")]
    CircuitBreakerOpen,

    #[error("Timeout error: operation exceeded {0}ms")]
    Timeout(u64),

    #[error("Batch processing error: {0}")]
    BatchProcessing(String),

    #[error("Parse error: {0}")]
    ParseError(String),
}

pub type Result<T> = std::result::Result<T, SyncError>;
