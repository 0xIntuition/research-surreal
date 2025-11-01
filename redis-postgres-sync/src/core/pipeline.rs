use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::time::{interval, Duration};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::consumer::redis_stream::RedisStreamConsumer;
use crate::sync::postgres_client::PostgresClient;
use crate::monitoring::metrics::Metrics;
use crate::error::{Result, SyncError};
use super::circuit_breaker::CircuitBreaker;
use super::types::PipelineHealth;

pub struct EventProcessingPipeline {
    config: Config,
    redis_consumer: Arc<RedisStreamConsumer>,
    postgres_client: Arc<PostgresClient>,
    circuit_breaker: Arc<CircuitBreaker>,
    pub metrics: Arc<Metrics>,
    is_running: Arc<AtomicBool>,
    shutdown_sender: broadcast::Sender<()>,
    cancellation_token: CancellationToken,
}

impl EventProcessingPipeline {
    pub async fn new(config: Config) -> Result<Self> {
        info!("Initializing event processing pipeline");

        let redis_consumer = Arc::new(
            RedisStreamConsumer::new(&config.redis_url, &config.stream_names, &config.consumer_group, &config.consumer_name)
                .await?
        );

        // Create metrics before PostgresClient so we can pass it
        let metrics = Arc::new(Metrics::new());

        let postgres_client = Arc::new(
            PostgresClient::new(
                &config.database_url,
                Some(&config.redis_url),
                config.analytics_stream_name.clone(),
                (*metrics).clone(),
            )
            .await?
        );

        let circuit_breaker = Arc::new(
            CircuitBreaker::new(config.circuit_breaker_threshold, config.circuit_breaker_timeout_ms)
        );

        let (shutdown_sender, _) = broadcast::channel(1);
        let cancellation_token = CancellationToken::new();

        Ok(Self {
            config,
            redis_consumer,
            postgres_client,
            circuit_breaker,
            metrics,
            is_running: Arc::new(AtomicBool::new(false)),
            shutdown_sender,
            cancellation_token,
        })
    }

    pub async fn start(&self) -> Result<()> {
        if self.is_running.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return Err(SyncError::Processing("Pipeline is already running".to_string()));
        }

        info!("Starting event processing pipeline");

        // Set initial health status
        self.metrics.set_redis_health(true).await;
        self.metrics.set_postgres_health(true).await;

        // Start processing loops
        let processing_tasks = self.spawn_processing_tasks().await?;
        let monitoring_task = self.spawn_monitoring_task();

        // Wait for shutdown signal
        let mut shutdown_rx = self.shutdown_sender.subscribe();
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Shutdown signal received");
            }
            _ = self.cancellation_token.cancelled() => {
                info!("Cancellation token triggered");
            }
        }

        // Stop all tasks
        for task in processing_tasks {
            task.abort();
        }
        monitoring_task.abort();

        self.is_running.store(false, Ordering::SeqCst);
        info!("Pipeline stopped");
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        info!("Stopping event processing pipeline");
        self.cancellation_token.cancel();
        let _ = self.shutdown_sender.send(());
        Ok(())
    }

    pub async fn health(&self) -> PipelineHealth {
        let snapshot = self.metrics.get_snapshot().await;
        PipelineHealth {
            healthy: snapshot.redis_healthy && snapshot.postgres_healthy && !self.circuit_breaker.is_open(),
            redis_consumer_healthy: snapshot.redis_healthy,
            surreal_sync_healthy: snapshot.postgres_healthy,
            circuit_breaker_closed: !self.circuit_breaker.is_open(),
            last_check: chrono::Utc::now(),
            metrics: crate::core::types::PipelineMetrics {
                total_events_processed: snapshot.total_events_processed,
                total_events_failed: snapshot.total_events_failed,
                circuit_breaker_state: self.circuit_breaker.get_state(),
                redis_consumer_health: snapshot.redis_healthy,
                surreal_sync_health: snapshot.postgres_healthy,
            },
        }
    }

    async fn spawn_processing_tasks(&self) -> Result<Vec<tokio::task::JoinHandle<()>>> {
        let mut tasks = Vec::new();

        for worker_id in 0..self.config.workers {
            let task = self.spawn_worker(worker_id).await;
            tasks.push(task);
        }

        Ok(tasks)
    }

    async fn spawn_worker(&self, worker_id: usize) -> tokio::task::JoinHandle<()> {
        let redis_consumer = self.redis_consumer.clone();
        let postgres_client = self.postgres_client.clone();
        let circuit_breaker = self.circuit_breaker.clone();
        let metrics = self.metrics.clone();
        let config = self.config.clone();
        let cancellation_token = self.cancellation_token.clone();

        tokio::spawn(async move {
            info!("Worker {} started", worker_id);
            let mut batch_interval = interval(Duration::from_millis(config.batch_timeout_ms));
            
            loop {
                tokio::select! {
                    _ = batch_interval.tick() => {
                        if let Err(e) = Self::process_batch(
                            &redis_consumer,
                            &postgres_client,
                            &circuit_breaker,
                            &metrics,
                            &config,
                        ).await {
                            error!("Worker {} batch processing error: {}", worker_id, e);
                            circuit_breaker.record_failure().await;
                            metrics.set_redis_health(false).await;
                        }
                    }
                    _ = cancellation_token.cancelled() => {
                        info!("Worker {} stopping", worker_id);
                        break;
                    }
                }
            }
        })
    }

    async fn process_batch(
        redis_consumer: &RedisStreamConsumer,
        postgres_client: &PostgresClient,
        circuit_breaker: &CircuitBreaker,
        metrics: &Metrics,
        config: &Config,
    ) -> Result<()> {
        // Check circuit breaker
        circuit_breaker.check().await?;

        // Consume messages from Redis
        debug!("About to call consume_batch with batch_size: {}", config.batch_size);
        let batch_result = redis_consumer.consume_batch(config.batch_size).await?;
        debug!("consume_batch returned {} messages ({} claimed)", batch_result.messages.len(), batch_result.claimed_count);
        if batch_result.messages.is_empty() {
            debug!("No messages received, returning early");
            return Ok(());
        }

        debug!("Processing batch of {} messages", batch_result.messages.len());

        // Track per-stream metrics
        let mut stream_message_counts: HashMap<String, usize> = HashMap::new();
        let mut stream_claimed_counts: HashMap<String, usize> = HashMap::new();

        for msg in &batch_result.messages {
            *stream_message_counts.entry(msg.source_stream.clone()).or_insert(0) += 1;
        }

        // Track claimed messages per stream if any were claimed
        if batch_result.claimed_count > 0 {
            for msg in &batch_result.messages {
                *stream_claimed_counts.entry(msg.source_stream.clone()).or_insert(0) += 1;
            }
        }

        for (stream_name, count) in &stream_message_counts {
            metrics.record_stream_batch_size(stream_name, *count);
            metrics.record_stream_messages_consumed(stream_name, *count);
            metrics.record_stream_last_message_timestamp(stream_name);
        }

        // Record claimed messages metric
        for (stream_name, count) in &stream_claimed_counts {
            metrics.record_stream_messages_claimed(stream_name, *count as u64);
        }

        // Process each message
        let mut successful = 0u64;
        let mut failed = 0u64;

        for message in batch_result.messages {
            let result = metrics.time_async_operation(|| {
                postgres_client.sync_event(&message.event)
            }).await;

            match result {
                Ok(_) => {
                    if let Err(e) = redis_consumer.ack_message(&message.source_stream, &message.redis_message_id).await {
                        warn!("Failed to acknowledge message {} from stream {}: {}", message.redis_message_id, message.source_stream, e);
                    }
                    successful += 1;
                }
                Err(e) => {
                    error!("Failed to sync event: {}", e);
                    error!("Event details - Name: {}, Data: {}",
                        message.event.event_name,
                        serde_json::to_string_pretty(&message.event.event_data).unwrap_or_else(|_| "Failed to serialize event_data for logging".to_string())
                    );
                    error!("Event network: {}, signature: {}", message.event.network, message.event.event_signature_hash);
                    failed += 1;
                }
            }
        }

        // Update metrics
        if successful > 0 {
            metrics.record_event_success(successful);
            circuit_breaker.record_success().await;
            metrics.set_postgres_health(true).await;
        }

        if failed > 0 {
            metrics.record_event_failure(failed);
            circuit_breaker.record_failure().await;
            metrics.set_postgres_health(false).await;
        }

        metrics.record_batch();
        metrics.update_last_event_time().await;

        Ok(())
    }

    fn spawn_monitoring_task(&self) -> tokio::task::JoinHandle<()> {
        let metrics = self.metrics.clone();
        let redis_consumer = self.redis_consumer.clone();
        let cancellation_token = self.cancellation_token.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(60));
            let start_time = std::time::Instant::now();
            let mut last_check_time = std::time::Instant::now();
            let mut last_event_count = 0u64;

            // Initialize claimed messages metric for all streams so it appears in Prometheus
            for stream_name in redis_consumer.get_stream_names() {
                metrics.record_stream_messages_claimed(stream_name, 0);
            }

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let snapshot = metrics.get_snapshot().await;

                        // Query stream metrics for each stream
                        for stream_name in redis_consumer.get_stream_names() {
                            if let Ok((lag, pending_count)) = redis_consumer.get_stream_metrics(stream_name).await {
                                metrics.record_stream_lag(stream_name, lag);
                                metrics.record_stream_pending_messages(stream_name, pending_count);
                                debug!("Stream {} metrics - lag: {}, pending: {}", stream_name, lag, pending_count);
                            }
                        }

                        // Calculate instantaneous rate (events since last check)
                        let now = std::time::Instant::now();
                        let elapsed_since_last_check = now.duration_since(last_check_time).as_secs_f64();
                        let events_since_last_check = snapshot.total_events_processed.saturating_sub(last_event_count);
                        let current_rate = if elapsed_since_last_check > 0.0 {
                            events_since_last_check as f64 / elapsed_since_last_check
                        } else {
                            0.0
                        };

                        // Update the metrics with the calculated rate
                        metrics.update_event_rate(current_rate).await;

                        // Calculate average rate since start
                        let total_elapsed = now.duration_since(start_time).as_secs_f64();
                        let avg_rate = if total_elapsed > 0.0 {
                            snapshot.total_events_processed as f64 / total_elapsed
                        } else {
                            0.0
                        };

                        info!(
                            "Metrics - Events processed: {}, not-ok: {}, batches: {}, rate: {:.2}/s (avg: {:.2}/s)",
                            snapshot.total_events_processed,
                            snapshot.total_events_failed,
                            snapshot.total_batches_processed,
                            current_rate,
                            avg_rate
                        );

                        // Update tracking variables for next iteration
                        last_check_time = now;
                        last_event_count = snapshot.total_events_processed;
                    }
                    _ = cancellation_token.cancelled() => {
                        break;
                    }
                }
            }
        })
    }

    /// Get a reference to the database pool for shared use
    pub fn get_pool(&self) -> &sqlx::PgPool {
        self.postgres_client.pool()
    }

    /// Get a clone of the cancellation token for shutdown coordination
    pub fn get_cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }
}