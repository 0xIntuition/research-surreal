use thiserror::Error;

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),
    
    #[error("SurrealDB error: {0}")]
    Surreal(Box<surrealdb::Error>),
    
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

impl From<surrealdb::Error> for SyncError {
    fn from(err: surrealdb::Error) -> Self {
        SyncError::Surreal(Box::new(err))
    }
}

pub type Result<T> = std::result::Result<T, SyncError>;