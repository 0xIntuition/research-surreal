use serde::{Deserialize, Serialize};
use std::env;
use crate::error::{Result, SyncError};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    // Redis settings
    pub redis_url: String,
    pub stream_names: Vec<String>,
    pub consumer_group: String,
    pub consumer_name: String,
    
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
            // Redis configuration
            redis_url: env::var("REDIS_URL")
                .unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string()),
            stream_names: env::var("REDIS_STREAMS")
                .unwrap_or_else(|_| "rindexer_producer".to_string())
                .split(',')
                .map(|s| s.trim().to_string())
                .collect(),
            consumer_group: env::var("CONSUMER_GROUP")
                .unwrap_or_else(|_| "surreal-sync".to_string()),
            consumer_name: env::var("CONSUMER_NAME")
                .unwrap_or_else(|_| format!("consumer-{}", uuid::Uuid::new_v4())),
            
            // SurrealDB configuration
            surreal_url: env::var("SURREAL_URL")
                .map_err(|_| SyncError::Config("SURREAL_URL is required".to_string()))?,
            surreal_user: env::var("SURREAL_USER")
                .unwrap_or_else(|_| "root".to_string()),
            surreal_pass: env::var("SURREAL_PASS")
                .unwrap_or_else(|_| "root".to_string()),
            surreal_ns: env::var("SURREAL_NS")
                .unwrap_or_else(|_| "namespace".to_string()),
            surreal_db: env::var("SURREAL_DB")
                .unwrap_or_else(|_| "database".to_string()),
            
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
        if self.stream_names.is_empty() {
            return Err(SyncError::Config("At least one stream name is required".to_string()));
        }
        
        if self.batch_size == 0 {
            return Err(SyncError::Config("Batch size must be greater than 0".to_string()));
        }
        
        if self.workers == 0 {
            return Err(SyncError::Config("Number of workers must be greater than 0".to_string()));
        }
        
        Ok(())
    }
}