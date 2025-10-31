use redis::{aio::MultiplexedConnection, Client};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::error::{Result, SyncError};

/// Redis publisher for analytics updates
/// Used to notify the analytics worker of term updates
pub struct RedisPublisher {
    connection: MultiplexedConnection,
}

/// Message published to Redis stream for analytics worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermUpdateMessage {
    pub term_id: String,
    pub counter_term_id: Option<String>,
    pub timestamp: i64,
}

impl RedisPublisher {
    pub async fn new(redis_url: &str) -> Result<Self> {
        let client = Client::open(redis_url).map_err(SyncError::Redis)?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(SyncError::Redis)?;

        info!("Connected to Redis for publishing analytics updates");

        Ok(Self { connection })
    }

    /// Publish a term update to the analytics stream
    pub async fn publish_term_update(
        &mut self,
        term_id: &str,
        counter_term_id: Option<&str>,
    ) -> Result<()> {
        let message = TermUpdateMessage {
            term_id: term_id.to_string(),
            counter_term_id: counter_term_id.map(|s| s.to_string()),
            timestamp: chrono::Utc::now().timestamp(),
        };

        let message_json =
            serde_json::to_string(&message).map_err(|e| SyncError::Serde(e))?;

        debug!(
            "Publishing term update to analytics stream: term={}, counter_term={:?}",
            term_id, counter_term_id
        );

        // Publish to Redis stream
        let message_id: String = redis::cmd("XADD")
            .arg("term_updates") // stream name
            .arg("*") // auto-generate ID
            .arg("data")
            .arg(&message_json)
            .query_async(&mut self.connection)
            .await
            .map_err(SyncError::Redis)?;

        debug!(
            "Published term update: term={}, counter_term={:?}, message_id={}",
            term_id, counter_term_id, message_id
        );
        Ok(())
    }

    /// Publish multiple term updates in a single pipeline for efficiency
    pub async fn publish_term_updates_batch(
        &mut self,
        updates: &[(String, Option<String>)],
    ) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }

        debug!("Publishing batch of {} term updates to analytics stream", updates.len());

        let mut pipe = redis::pipe();

        for (term_id, counter_term_id) in updates {
            let message = TermUpdateMessage {
                term_id: term_id.clone(),
                counter_term_id: counter_term_id.clone(),
                timestamp: chrono::Utc::now().timestamp(),
            };

            let message_json =
                serde_json::to_string(&message).map_err(|e| SyncError::Serde(e))?;

            pipe.cmd("XADD")
                .arg("term_updates")
                .arg("*")
                .arg("data")
                .arg(&message_json);
        }

        let message_ids: Vec<String> = pipe
            .query_async(&mut self.connection)
            .await
            .map_err(SyncError::Redis)?;

        debug!(
            "Successfully published batch of {} term updates (first_id: {})",
            updates.len(),
            message_ids.first().unwrap_or(&"none".to_string())
        );
        Ok(())
    }
}
