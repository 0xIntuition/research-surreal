use redis::{aio::MultiplexedConnection, Client};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info};

use crate::{
    error::{Result, SyncError},
    monitoring::metrics::Metrics,
};

/// Redis publisher for analytics updates
/// Used to notify the analytics worker of term updates
pub struct RedisPublisher {
    connection: MultiplexedConnection,
    stream_name: String,
    metrics: Arc<Metrics>,
}

/// Message published to Redis stream for analytics worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermUpdateMessage {
    pub term_id: String,
    pub timestamp: i64,
}

impl RedisPublisher {
    pub async fn new(redis_url: &str, stream_name: String, metrics: Arc<Metrics>) -> Result<Self> {
        let client = Client::open(redis_url).map_err(SyncError::Redis)?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(SyncError::Redis)?;

        info!(
            "Connected to Redis for publishing analytics updates to stream '{}'",
            stream_name
        );

        Ok(Self {
            connection,
            stream_name,
            metrics,
        })
    }

    /// Publish a term update to the analytics stream
    pub async fn publish_term_update(&mut self, term_id: &str) -> Result<()> {
        let start_time = std::time::Instant::now();

        let message = TermUpdateMessage {
            term_id: term_id.to_string(),
            timestamp: chrono::Utc::now().timestamp(),
        };

        let message_json = serde_json::to_string(&message).map_err(SyncError::Serde)?;

        // Publish to Redis stream
        let message_id: String = redis::cmd("XADD")
            .arg(&self.stream_name)
            .arg("*") // auto-generate ID
            .arg("data")
            .arg(&message_json)
            .query_async(&mut self.connection)
            .await
            .map_err(SyncError::Redis)?;

        // Record metrics
        self.metrics.record_term_update_published();
        self.metrics
            .record_term_updates_publish_duration(start_time.elapsed());

        debug!(
            "Published to stream '{}': term={}, message_id={}",
            self.stream_name, term_id, message_id
        );
        Ok(())
    }

    /// Publish multiple term updates in a single pipeline for efficiency
    pub async fn publish_term_updates_batch(&mut self, term_ids: &[String]) -> Result<()> {
        if term_ids.is_empty() {
            return Ok(());
        }

        let start_time = std::time::Instant::now();

        debug!(
            "Publishing batch of {} term updates to stream '{}'",
            term_ids.len(),
            self.stream_name
        );

        let mut pipe = redis::pipe();

        for term_id in term_ids {
            let message = TermUpdateMessage {
                term_id: term_id.clone(),
                timestamp: chrono::Utc::now().timestamp(),
            };

            let message_json = serde_json::to_string(&message).map_err(SyncError::Serde)?;

            pipe.cmd("XADD")
                .arg(&self.stream_name)
                .arg("*")
                .arg("data")
                .arg(&message_json);
        }

        let message_ids: Vec<String> = pipe
            .query_async(&mut self.connection)
            .await
            .map_err(SyncError::Redis)?;

        // Record metrics for batch publish (single pipeline operation)
        self.metrics.record_term_update_published();
        self.metrics
            .record_term_updates_publish_duration(start_time.elapsed());

        let first_id = message_ids.first().map(|s| s.as_str()).unwrap_or("none");
        let last_id = message_ids.last().map(|s| s.as_str()).unwrap_or("none");

        debug!(
            "Published batch of {} updates to stream '{}' (IDs: {} ... {})",
            term_ids.len(),
            self.stream_name,
            first_id,
            last_id
        );
        Ok(())
    }
}
