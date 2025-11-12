use crate::error::{Result, SyncError};
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    // RabbitMQ settings
    pub rabbitmq_url: String,
    pub exchanges: Vec<String>,
    pub queue_prefix: String,
    pub prefetch_count: u16,

    // SurrealDB settings
    pub surreal_url: String,
    pub surreal_user: String,
    pub surreal_pass: String,
    pub surreal_ns: String,
    pub surreal_db: String,

    // Processing settings
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    pub workers: usize,
    pub processing_timeout_ms: u64,
    pub max_retries: u32,

    // Circuit breaker settings
    pub circuit_breaker_threshold: u32,
    pub circuit_breaker_timeout_ms: u64,

    // HTTP server settings
    pub http_port: u16,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Config {
            // RabbitMQ configuration
            rabbitmq_url: env::var("RABBITMQ_URL")
                .unwrap_or_else(|_| "amqp://admin:admin@127.0.0.1:18101".to_string()),
            exchanges: env::var("RABBITMQ_EXCHANGES")
                .unwrap_or_else(|_| {
                    "atom_created,triple_created,deposited,redeemed,share_price_changed".to_string()
                })
                .split(',')
                .map(|s| s.trim().to_string())
                .collect(),
            queue_prefix: env::var("QUEUE_PREFIX").unwrap_or_else(|_| "surreal".to_string()),
            prefetch_count: env::var("PREFETCH_COUNT")
                .unwrap_or_else(|_| "20".to_string())
                .parse()
                .unwrap_or(20),

            // SurrealDB configuration
            surreal_url: env::var("SURREAL_URL")
                .map_err(|_| SyncError::Config("SURREAL_URL is required".to_string()))?,
            surreal_user: env::var("SURREAL_USER").unwrap_or_else(|_| "root".to_string()),
            surreal_pass: env::var("SURREAL_PASS").unwrap_or_else(|_| "root".to_string()),
            surreal_ns: env::var("SURREAL_NS").unwrap_or_else(|_| "namespace".to_string()),
            surreal_db: env::var("SURREAL_DB").unwrap_or_else(|_| "database".to_string()),

            // Processing configuration
            batch_size: env::var("BATCH_SIZE")
                .unwrap_or_else(|_| "100".to_string())
                .parse()
                .unwrap_or(100),
            batch_timeout_ms: env::var("BATCH_TIMEOUT_MS")
                .unwrap_or_else(|_| "5000".to_string())
                .parse()
                .unwrap_or(5000),
            workers: env::var("WORKERS")
                .unwrap_or_else(|_| "4".to_string())
                .parse()
                .unwrap_or(4),
            processing_timeout_ms: env::var("PROCESSING_TIMEOUT_MS")
                .unwrap_or_else(|_| "30000".to_string())
                .parse()
                .unwrap_or(30000),
            max_retries: env::var("MAX_RETRIES")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .unwrap_or(3),

            // Circuit breaker configuration
            circuit_breaker_threshold: env::var("CIRCUIT_BREAKER_THRESHOLD")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap_or(5),
            circuit_breaker_timeout_ms: env::var("CIRCUIT_BREAKER_TIMEOUT_MS")
                .unwrap_or_else(|_| "60000".to_string())
                .parse()
                .unwrap_or(60000),

            // HTTP server configuration
            http_port: env::var("HTTP_PORT")
                .unwrap_or_else(|_| "8080".to_string())
                .parse()
                .unwrap_or(8080),
        })
    }

    pub fn validate(&self) -> Result<()> {
        if self.exchanges.is_empty() {
            return Err(SyncError::Config(
                "At least one exchange is required".to_string(),
            ));
        }

        if self.prefetch_count == 0 {
            return Err(SyncError::Config(
                "Prefetch count must be greater than 0".to_string(),
            ));
        }

        if self.batch_size == 0 {
            return Err(SyncError::Config(
                "Batch size must be greater than 0".to_string(),
            ));
        }

        if self.workers == 0 {
            return Err(SyncError::Config(
                "Number of workers must be greater than 0".to_string(),
            ));
        }

        Ok(())
    }
}
