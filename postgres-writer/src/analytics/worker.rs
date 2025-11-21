// Analytics worker implementation
// Consumes term update messages from PostgreSQL queue and updates analytics tables

use super::processor::update_analytics_tables;
use crate::{
    consumer::{postgres_queue_consumer::TermQueueConsumer, TermUpdateMessage},
    error::{Result, SyncError},
    monitoring::metrics::Metrics,
    Config,
};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub async fn start_analytics_worker(
    config: Config,
    pool: PgPool,
    metrics: Arc<Metrics>,
    cancellation_token: CancellationToken,
) -> Result<()> {
    info!("Starting analytics worker");

    // Create queue consumer
    let queue_consumer = TermQueueConsumer::new(
        pool.clone(),
        Duration::from_millis(config.queue_poll_interval_ms),
        config.max_retry_attempts,
    );

    info!(
        "Analytics worker using PostgreSQL queue (poll_interval: {}ms, max_retries: {})",
        config.queue_poll_interval_ms, config.max_retry_attempts
    );

    // Log initial queue state
    match queue_consumer.get_queue_stats().await {
        Ok(stats) => {
            info!(
                "Analytics worker started, {} pending messages in queue ({} retrying)",
                stats.pending_count, stats.retry_count
            );
            if let Some(age) = stats.oldest_pending_age_secs {
                info!("Oldest pending message: {:.1}s old", age);
            }
        }
        Err(e) => {
            info!("Analytics worker started, processing messages...");
            debug!("Could not get initial queue stats: {}", e);
        }
    }

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

    // Spawn cleanup task
    let cleanup_consumer = TermQueueConsumer::new(
        pool.clone(),
        Duration::from_millis(config.queue_poll_interval_ms),
        config.max_retry_attempts,
    );
    let cleanup_metrics = metrics.clone();
    let cleanup_retention_hours = config.queue_retention_hours;
    let failed_retention_hours = config.failed_message_retention_hours;
    let cleanup_cancellation = cancellation_token.child_token();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3600)); // 1 hour
        loop {
            tokio::select! {
                _ = cleanup_cancellation.cancelled() => {
                    info!("Queue cleanup task received shutdown signal");
                    break;
                }
                _ = interval.tick() => {
                    // Cleanup old processed messages
                    match cleanup_consumer.cleanup_old_messages(cleanup_retention_hours).await {
                        Ok(deleted) => {
                            if deleted > 0 {
                                info!("Cleaned up {} old processed messages", deleted);
                                cleanup_metrics.record_queue_cleanup_processed(deleted);
                            }
                        }
                        Err(e) => {
                            error!("Failed to cleanup old messages: {}", e);
                        }
                    }

                    // Cleanup permanently failed messages
                    match cleanup_consumer.cleanup_permanently_failed(failed_retention_hours).await {
                        Ok(deleted) => {
                            if deleted > 0 {
                                cleanup_metrics.record_queue_cleanup_failed(deleted);
                            }
                        }
                        Err(e) => {
                            error!("Failed to cleanup permanently failed messages: {}", e);
                        }
                    }
                }
            }
        }
    });

    // Spawn metrics update task
    let metrics_consumer = TermQueueConsumer::new(
        pool.clone(),
        Duration::from_millis(config.queue_poll_interval_ms),
        config.max_retry_attempts,
    );
    let metrics_ref = metrics.clone();
    let metrics_cancellation = cancellation_token.child_token();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            tokio::select! {
                _ = metrics_cancellation.cancelled() => {
                    info!("Queue metrics task received shutdown signal");
                    break;
                }
                _ = interval.tick() => {
                    // Update pending message count
                    if let Ok(stats) = metrics_consumer.get_queue_stats().await {
                        metrics_ref.record_analytics_messages_pending(stats.pending_count);
                    }

                    // Update permanently failed message count
                    if let Ok(failed_count) = metrics_consumer.get_permanently_failed_count().await {
                        metrics_ref.record_queue_permanently_failed(failed_count);
                    }
                }
            }
        }
    });

    // Main processing loop
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("Analytics worker received shutdown signal");
                break;
            }
            result = process_batch(&queue_consumer, &pool, &metrics, config.analytics_batch_size) => {
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

                            // Log periodic summary
                            if last_summary_time.elapsed().as_secs() >= SUMMARY_INTERVAL_SECS {
                                match queue_consumer.get_queue_stats().await {
                                    Ok(stats) => {
                                        info!(
                                            "Analytics summary: processed {} msgs total ({:.1} msg/s avg), {} pending",
                                            total_processed, rate, stats.pending_count
                                        );
                                    }
                                    Err(_) => {
                                        info!(
                                            "Analytics summary: processed {} msgs total ({:.1} msg/s avg)",
                                            total_processed, rate
                                        );
                                    }
                                }

                                last_summary_time = std::time::Instant::now();
                            }

                            // Rate limiting: enforce delay based on both batch interval and rate limit
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
                                tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                            }

                            last_batch_time = std::time::Instant::now();
                        } else {
                            // No messages, sleep for poll interval
                            tokio::time::sleep(queue_consumer.poll_interval()).await;
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
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;

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
    queue_consumer: &TermQueueConsumer,
    pool: &PgPool,
    metrics: &Arc<Metrics>,
    batch_size: usize,
) -> Result<usize> {
    // Poll for messages
    let messages = queue_consumer
        .poll_messages(batch_size as i64)
        .await
        .map_err(SyncError::Sqlx)?;

    if messages.is_empty() {
        metrics.record_analytics_batch_size(0);
        return Ok(0);
    }

    metrics.record_analytics_batch_size(messages.len());

    debug!("Received {} messages from queue", messages.len());

    let mut successful_ids: Vec<i64> = Vec::new();
    let mut failed_messages: Vec<(i64, String)> = Vec::new();

    // Process each message
    for message in &messages {
        let start_time = std::time::Instant::now();

        // Create TermUpdateMessage from queue message
        let term_update = TermUpdateMessage {
            term_id: message.term_id.clone(),
            timestamp: message.created_at.timestamp(),
        };

        match update_analytics_tables(pool, metrics, &term_update).await {
            Ok(()) => {
                metrics.record_analytics_processing_duration(start_time.elapsed());
                metrics.record_analytics_message_consumed();

                debug!(
                    "Successfully updated analytics for term {}",
                    term_update.term_id
                );

                // Collect ID for batch marking
                successful_ids.push(message.id);
            }
            Err(e) => {
                metrics.record_analytics_message_failure();

                warn!(
                    "Failed to update analytics for term {} (message_id: {}): {}. Will mark as failed for retry.",
                    term_update.term_id, message.id, e
                );

                // Collect ID and error for batch marking
                failed_messages.push((message.id, e.to_string()));
            }
        }
    }

    // Batch mark successful messages
    if !successful_ids.is_empty() {
        match queue_consumer.mark_batch_processed(&successful_ids).await {
            Ok(marked_count) => {
                debug!("Marked {} messages as processed", marked_count);
                metrics.record_queue_messages_marked_processed(marked_count);
            }
            Err(e) => {
                error!(
                    "Failed to mark {} messages as processed: {}",
                    successful_ids.len(),
                    e
                );
            }
        }
    }

    // Batch mark failed messages
    if !failed_messages.is_empty() {
        match queue_consumer.mark_batch_failed(&failed_messages).await {
            Ok(marked_count) => {
                debug!("Marked {} messages as failed for retry", marked_count);
                metrics.record_queue_messages_marked_failed(marked_count);
            }
            Err(e) => {
                error!(
                    "Failed to mark {} messages as failed: {}",
                    failed_messages.len(),
                    e
                );
            }
        }
    }

    debug!(
        "Batch complete: {} successful, {} failed (will retry)",
        successful_ids.len(),
        failed_messages.len()
    );

    Ok(messages.len())
}
