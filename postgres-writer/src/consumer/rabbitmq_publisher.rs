use lapin::{
    options::*, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::{
    error::{Result, SyncError},
    monitoring::metrics::Metrics,
};

/// RabbitMQ publisher for analytics updates
/// Used to notify the analytics worker of term updates
pub struct RabbitMQPublisher {
    _connection: Arc<Connection>,
    channel: Arc<Mutex<Channel>>,
    exchange: String,
    routing_key: String,
    metrics: Arc<Metrics>,
}

/// Message published to RabbitMQ queue for analytics worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermUpdateMessage {
    pub term_id: String,
    pub timestamp: i64,
}

impl RabbitMQPublisher {
    pub async fn new(
        rabbitmq_url: &str,
        exchange: &str,
        routing_key: &str,
        metrics: Arc<Metrics>,
    ) -> Result<Self> {
        let connection = Connection::connect(rabbitmq_url, ConnectionProperties::default())
            .await
            .map_err(|e| SyncError::Connection(format!("Failed to connect to RabbitMQ: {e}")))?;

        let channel = connection
            .create_channel()
            .await
            .map_err(|e| SyncError::Connection(format!("Failed to create channel: {e}")))?;

        // Declare exchange (durable=false to match rindexer)
        channel
            .exchange_declare(
                exchange,
                lapin::ExchangeKind::Direct,
                ExchangeDeclareOptions {
                    durable: false,
                    auto_delete: false,
                    internal: false,
                    nowait: false,
                    passive: false,
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| {
                SyncError::Connection(format!("Failed to declare exchange {exchange}: {e}"))
            })?;

        info!(
            "Connected to RabbitMQ for publishing analytics updates to exchange '{}' with routing key '{}'",
            exchange, routing_key
        );

        Ok(Self {
            _connection: Arc::new(connection),
            channel: Arc::new(Mutex::new(channel)),
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
            metrics,
        })
    }

    /// Publish a term update to the analytics exchange
    pub async fn publish_term_update(&self, term_id: &str) -> Result<()> {
        let start_time = std::time::Instant::now();

        let message = TermUpdateMessage {
            term_id: term_id.to_string(),
            timestamp: chrono::Utc::now().timestamp(),
        };

        let payload = serde_json::to_vec(&message).map_err(SyncError::Serde)?;

        let channel = self.channel.lock().await;

        channel
            .basic_publish(
                &self.exchange,
                &self.routing_key,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default(),
            )
            .await
            .map_err(|e| SyncError::Connection(format!("Failed to publish message: {e}")))?;

        // Record metrics
        self.metrics.record_term_update_published();
        self.metrics
            .record_term_updates_publish_duration(start_time.elapsed());

        debug!(
            "Published to exchange '{}': term={}",
            self.exchange, term_id
        );
        Ok(())
    }

    /// Publish multiple term updates in a batch for efficiency
    pub async fn publish_term_updates_batch(&self, term_ids: &[String]) -> Result<()> {
        if term_ids.is_empty() {
            return Ok(());
        }

        let start_time = std::time::Instant::now();

        debug!(
            "Publishing batch of {} term updates to exchange '{}'",
            term_ids.len(),
            self.exchange
        );

        let channel = self.channel.lock().await;

        for term_id in term_ids {
            let message = TermUpdateMessage {
                term_id: term_id.clone(),
                timestamp: chrono::Utc::now().timestamp(),
            };

            let payload = serde_json::to_vec(&message).map_err(SyncError::Serde)?;

            channel
                .basic_publish(
                    &self.exchange,
                    &self.routing_key,
                    BasicPublishOptions::default(),
                    &payload,
                    BasicProperties::default(),
                )
                .await
                .map_err(|e| SyncError::Connection(format!("Failed to publish message: {e}")))?;
        }

        // Record metrics for batch publish
        self.metrics.record_term_update_published();
        self.metrics
            .record_term_updates_publish_duration(start_time.elapsed());

        debug!(
            "Published batch of {} updates to exchange '{}'",
            term_ids.len(),
            self.exchange
        );
        Ok(())
    }
}
