use crate::error::{Result, SyncError};
use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    // Redis settings
    pub redis_url: String,
    pub stream_names: Vec<String>,
    pub consumer_group: String,
    pub consumer_name: String,

    // PostgreSQL settings
    pub database_url: String,

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

    // Analytics worker settings
    pub consumer_group_suffix: Option<String>,
    pub analytics_stream_name: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        Ok(Config {
            // Redis configuration
            redis_url: env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()),
            stream_names: env::var("REDIS_STREAMS")
                .unwrap_or_else(|_| "rindexer_producer".to_string())
                .split(',')
                .map(|s| s.trim().to_string())
                .collect(),
            consumer_group: env::var("CONSUMER_GROUP")
                .unwrap_or_else(|_| "postgres-sync".to_string()),
            consumer_name: env::var("CONSUMER_NAME")
                .unwrap_or_else(|_| format!("consumer-{}", uuid::Uuid::new_v4())),

            // PostgreSQL configuration
            database_url: env::var("DATABASE_URL")
                .map_err(|_| SyncError::Config("DATABASE_URL is required".to_string()))?,

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

            // Analytics worker configuration
            consumer_group_suffix: env::var("CONSUMER_GROUP_SUFFIX").ok(),
            analytics_stream_name: {
                let base_name = env::var("ANALYTICS_STREAM_NAME")
                    .unwrap_or_else(|_| "term_updates".to_string());

                // If CONSUMER_GROUP_SUFFIX is provided and the stream name is the default,
                // append the suffix to create a unique stream per restart cycle
                if let Some(ref suffix) = env::var("CONSUMER_GROUP_SUFFIX").ok() {
                    if base_name == "term_updates" {
                        format!("{}-{}", base_name, suffix)
                    } else {
                        base_name
                    }
                } else {
                    base_name
                }
            },
        })
    }

    pub fn validate(&self) -> Result<()> {
        if self.stream_names.is_empty() {
            return Err(SyncError::Config(
                "At least one stream name is required".to_string(),
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
