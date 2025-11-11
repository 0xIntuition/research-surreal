// Analytics worker implementation
// Consumes term update messages from RabbitMQ and updates analytics tables

use super::processor::update_analytics_tables;
use crate::{
    consumer::{rabbitmq_consumer::RabbitMQConsumer, TermUpdateMessage},
    error::Result,
    monitoring::metrics::Metrics,
    Config,
};
use sqlx::PgPool;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub async fn start_analytics_worker(
    config: Config,
    pool: PgPool,
    metrics: Arc<Metrics>,
    cancellation_token: CancellationToken,
) -> Result<()> {
    info!("Starting analytics worker");

    // Create RabbitMQ consumer for term_updates queue
    let consumer = RabbitMQConsumer::new(
        &config.rabbitmq_url,
        &config.queue_prefix,
        &[config.analytics_exchange.clone()],
        config.prefetch_count,
    )
    .await?;

    info!(
        "Analytics worker connected to RabbitMQ, consuming from queue '{}.{}'",
        config.queue_prefix, config.analytics_exchange
    );

    let mut total_processed = 0u64;
    let start_time = std::time::Instant::now();
    let mut last_summary_time = std::time::Instant::now();
    const SUMMARY_INTERVAL_SECS: u64 = 60; // Log summary every 60 seconds

    // Exponential backoff state
    let mut consecutive_failures = 0u32;
    const MAX_BACKOFF_SECS: u64 = 60;
    const INITIAL_BACKOFF_SECS: u64 = 1;
    const MAX_CONSECUTIVE_FAILURES: u32 = 10;

    // Rate limiting configuration
    let max_messages_per_second = config.max_messages_per_second;
    let min_batch_interval_ms = config.min_batch_interval_ms;
    let mut last_batch_time = std::time::Instant::now();

    // Main processing loop
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("Analytics worker received shutdown signal");
                break;
            }
            result = process_batch(&consumer, &pool, &metrics, &config) => {
                match result {
                    Ok(processed_count) => {
                        if processed_count > 0 {
                            // Reset failure counter on success
                            consecutive_failures = 0;

                            total_processed += processed_count as u64;
                            let elapsed = start_time.elapsed().as_secs();
                            let rate = if elapsed > 0 {
                                total_processed as f64 / elapsed as f64
                            } else {
                                0.0
                            };
                            debug!(
                                "Processed batch of {} term updates (total: {}, rate: {:.2} msg/s)",
                                processed_count, total_processed, rate
                            );

                            // Update queue depth metric
                            let queue_name = format!("{}.{}", config.queue_prefix, config.analytics_exchange);
                            if let Ok(queue_depth) = consumer.get_queue_depth(&queue_name).await {
                                metrics.record_analytics_messages_pending(queue_depth as i64);
                            }

                            // Log periodic summary
                            if last_summary_time.elapsed().as_secs() >= SUMMARY_INTERVAL_SECS {
                                if let Ok(queue_depth) = consumer.get_queue_depth(&queue_name).await {
                                    info!(
                                        "Analytics summary for queue '{}': processed {} msgs total ({:.1} msg/s avg), {} pending",
                                        queue_name, total_processed, rate, queue_depth
                                    );
                                } else {
                                    info!(
                                        "Analytics summary for queue '{}': processed {} msgs total ({:.1} msg/s avg)",
                                        queue_name, total_processed, rate
                                    );
                                }
                                last_summary_time = std::time::Instant::now();
                            }

                            // Rate limiting
                            let rate_limit_delay_ms = if max_messages_per_second > 0 {
                                let ms_per_message = 1000.0 / max_messages_per_second as f64;
                                (ms_per_message * processed_count as f64) as u64
                            } else {
                                0
                            };

                            let required_delay_ms = std::cmp::max(min_batch_interval_ms, rate_limit_delay_ms);
                            let batch_elapsed = last_batch_time.elapsed();
                            if batch_elapsed.as_millis() < required_delay_ms as u128 {
                                let sleep_ms = required_delay_ms - batch_elapsed.as_millis() as u64;
                                debug!(
                                    "Rate limiting: sleeping {}ms (batch_interval={}, rate_limit_delay={}, actual={})",
                                    sleep_ms, min_batch_interval_ms, rate_limit_delay_ms, required_delay_ms
                                );
                                tokio::time::sleep(tokio::time::Duration::from_millis(sleep_ms)).await;
                            }

                            last_batch_time = std::time::Instant::now();
                        }
                    }
                    Err(e) => {
                        consecutive_failures += 1;
                        error!(
                            "Error processing batch (failure {} of {}): {}",
                            consecutive_failures, MAX_CONSECUTIVE_FAILURES, e
                        );

                        let backoff_secs = std::cmp::min(
                            INITIAL_BACKOFF_SECS * 2u64.pow(consecutive_failures.saturating_sub(1)),
                            MAX_BACKOFF_SECS
                        );

                        warn!(
                            "Backing off for {} seconds before retry (consecutive failures: {})",
                            backoff_secs, consecutive_failures
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(backoff_secs)).await;

                        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                            error!(
                                "Analytics worker has failed {} times consecutively. Continuing with max backoff.",
                                MAX_CONSECUTIVE_FAILURES
                            );
                            consecutive_failures = MAX_CONSECUTIVE_FAILURES;
                        }
                    }
                }
            }
        }
    }

    info!("Analytics worker stopped");
    Ok(())
}

async fn process_batch(
    consumer: &RabbitMQConsumer,
    pool: &PgPool,
    metrics: &Arc<Metrics>,
    _config: &Config,
) -> Result<usize> {
    // Consume messages from RabbitMQ
    let messages = consumer.consume_batch(100).await?; // Process 100 messages at a time

    if messages.is_empty() {
        // Record empty batch to ensure gauge shows 0 during idle periods
        metrics.record_analytics_batch_size(0);
        return Ok(0);
    }

    // Record batch size
    metrics.record_analytics_batch_size(messages.len());

    debug!("Received {} messages from analytics queue", messages.len());

    let mut successful = 0;
    let mut failed = 0;

    // Process each message
    for message in messages {
        let start_time = std::time::Instant::now();

        // Parse the term update message from the event data
        let term_update: TermUpdateMessage =
            match serde_json::from_value(message.event.event_data.clone()) {
                Ok(update) => update,
                Err(e) => {
                    error!("Failed to parse term update message: {}", e);
                    // NACK with requeue=false for malformed messages
                    if let Some(ref acker) = message.acker {
                        if let Err(nack_err) = consumer.nack_message(acker, false).await {
                            warn!("Failed to nack malformed message: {}", nack_err);
                        } else {
                            metrics.record_analytics_message_failure();
                        }
                    }
                    failed += 1;
                    continue;
                }
            };

        match update_analytics_tables(pool, metrics, &term_update).await {
            Ok(()) => {
                // Record processing duration
                metrics.record_analytics_processing_duration(start_time.elapsed());

                // Record successful consumption
                metrics.record_analytics_message_consumed();

                debug!(
                    "Successfully updated analytics for term {}",
                    term_update.term_id
                );

                // ACK the message
                if let Some(ref acker) = message.acker {
                    if let Err(e) = consumer.ack_message(acker).await {
                        warn!("Failed to ack message: {}", e);
                    }
                }
                successful += 1;
            }
            Err(e) => {
                // Record failure
                metrics.record_analytics_message_failure();

                warn!(
                    "Failed to update analytics for term {}: {}. Message will be requeued.",
                    term_update.term_id, e
                );

                // NACK with requeue=true for processing failures
                if let Some(ref acker) = message.acker {
                    if let Err(nack_err) = consumer.nack_message(acker, true).await {
                        warn!("Failed to nack message: {}", nack_err);
                    }
                }
                failed += 1;
            }
        }
    }

    debug!(
        "Batch complete for analytics queue: {} successful, {} failed (requeued)",
        successful, failed
    );

    Ok(successful + failed)
}
