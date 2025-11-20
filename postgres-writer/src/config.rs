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
    pub database_pool_size: usize,

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

    // Shutdown settings
    pub shutdown_timeout_secs: u64,

    // Analytics worker settings
    pub consumer_group_suffix: Option<String>,
    pub max_messages_per_second: u64,
    pub min_batch_interval_ms: u64,

    // Queue settings
    pub queue_poll_interval_ms: u64,
    pub queue_retention_hours: i32,
    pub max_retry_attempts: i32,
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
            database_pool_size: env::var("DATABASE_POOL_SIZE")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .unwrap_or(10),

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

            // Shutdown configuration
            shutdown_timeout_secs: env::var("SHUTDOWN_TIMEOUT_SECS")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .unwrap_or(30),

            // Analytics worker configuration
            consumer_group_suffix: env::var("CONSUMER_GROUP_SUFFIX").ok(),
            max_messages_per_second: env::var("MAX_MESSAGES_PER_SECOND")
                .unwrap_or_else(|_| "5000".to_string())
                .parse()
                .unwrap_or(5000),
            min_batch_interval_ms: env::var("MIN_BATCH_INTERVAL_MS")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .unwrap_or(10),

            // Queue configuration
            queue_poll_interval_ms: env::var("QUEUE_POLL_INTERVAL_MS")
                .unwrap_or_else(|_| "100".to_string())
                .parse()
                .unwrap_or(100),
            queue_retention_hours: env::var("QUEUE_RETENTION_HOURS")
                .unwrap_or_else(|_| "24".to_string())
                .parse()
                .unwrap_or(24),
            max_retry_attempts: env::var("MAX_RETRY_ATTEMPTS")
                .unwrap_or_else(|_| "3".to_string())
                .parse()
                .unwrap_or(3),
        })
    }

    pub fn validate(&self) -> Result<()> {
        // Stream validation
        if self.stream_names.is_empty() {
            return Err(SyncError::Config(
                "At least one stream name is required".to_string(),
            ));
        }

        // Processing validation
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

        // Timeout validation
        if self.batch_timeout_ms == 0 {
            return Err(SyncError::Config(
                "Batch timeout must be greater than 0".to_string(),
            ));
        }

        if self.processing_timeout_ms == 0 {
            return Err(SyncError::Config(
                "Processing timeout must be greater than 0".to_string(),
            ));
        }

        // Circuit breaker validation
        if self.circuit_breaker_timeout_ms == 0 {
            return Err(SyncError::Config(
                "Circuit breaker timeout must be greater than 0".to_string(),
            ));
        }

        // HTTP port validation (valid port range: 1-65535)
        if self.http_port == 0 {
            return Err(SyncError::Config(
                "HTTP port must be between 1 and 65535".to_string(),
            ));
        }

        // Database pool size validation
        if self.database_pool_size == 0 {
            return Err(SyncError::Config(
                "Database pool size must be greater than 0".to_string(),
            ));
        }

        // Shutdown timeout validation
        if self.shutdown_timeout_secs == 0 {
            return Err(SyncError::Config(
                "Shutdown timeout must be greater than 0".to_string(),
            ));
        }

        // Rate limiting validation
        if self.max_messages_per_second == 0 {
            return Err(SyncError::Config(
                "Max messages per second must be greater than 0".to_string(),
            ));
        }

        if self.min_batch_interval_ms == 0 {
            return Err(SyncError::Config(
                "Min batch interval must be greater than 0".to_string(),
            ));
        }

        Ok(())
    }
}
