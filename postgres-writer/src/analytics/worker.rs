// Analytics worker implementation
// Consumes term update messages from mpsc channel and updates analytics tables

use super::processor::update_analytics_tables;
use crate::{error::Result, monitoring::metrics::Metrics, Config};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub async fn start_analytics_worker(
    _config: Config,
    pool: PgPool,
    metrics: Arc<Metrics>,
    mut term_update_rx: UnboundedReceiver<String>,
    cancellation_token: CancellationToken,
) -> Result<()> {
    info!("Starting analytics worker (consuming from mpsc channel)");

    let mut total_processed = 0u64;
    let start_time = std::time::Instant::now();
    let mut last_summary_time = std::time::Instant::now();
    const SUMMARY_INTERVAL_SECS: u64 = 60; // Log summary every 60 seconds

    // Exponential backoff state
    let mut consecutive_failures = 0u32;
    const MAX_BACKOFF_SECS: u64 = 60;
    const INITIAL_BACKOFF_SECS: u64 = 1;
    const MAX_CONSECUTIVE_FAILURES: u32 = 10;

    // Batch collection settings
    const MAX_BATCH_SIZE: usize = 100;
    const BATCH_TIMEOUT_MS: u64 = 100; // Collect for up to 100ms before processing

    // Main processing loop
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("Analytics worker received shutdown signal");
                break;
            }
            result = collect_and_process_batch(
                &mut term_update_rx,
                &pool,
                &metrics,
                MAX_BATCH_SIZE,
                BATCH_TIMEOUT_MS,
            ) => {
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
                                info!(
                                    "Analytics summary: processed {} term updates total ({:.1} msg/s avg)",
                                    total_processed, rate
                                );
                                last_summary_time = std::time::Instant::now();
                            }
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

/// Collect term updates from mpsc channel and process them in batches
async fn collect_and_process_batch(
    term_update_rx: &mut UnboundedReceiver<String>,
    pool: &PgPool,
    metrics: &Arc<Metrics>,
    max_batch_size: usize,
    batch_timeout_ms: u64,
) -> Result<usize> {
    let mut term_ids = Vec::with_capacity(max_batch_size);
    let timeout = tokio::time::Duration::from_millis(batch_timeout_ms);

    // Collect first message (blocking wait)
    let first_term_id = match term_update_rx.recv().await {
        Some(term_id) => term_id,
        None => {
            // Channel closed, no more messages
            debug!("Term update channel closed");
            return Ok(0);
        }
    };
    term_ids.push(first_term_id);

    // Try to collect more messages up to batch size with timeout
    let deadline = tokio::time::Instant::now() + timeout;
    while term_ids.len() < max_batch_size {
        match tokio::time::timeout_at(deadline, term_update_rx.recv()).await {
            Ok(Some(term_id)) => term_ids.push(term_id),
            Ok(None) => {
                // Channel closed
                debug!("Term update channel closed");
                break;
            }
            Err(_) => {
                // Timeout - process what we have
                break;
            }
        }
    }

    // Record batch size
    metrics.record_analytics_batch_size(term_ids.len());

    debug!(
        "Processing batch of {} term updates from mpsc channel",
        term_ids.len()
    );

    let mut successful = 0;
    let mut failed = 0;

    // Process each term update
    for term_id in term_ids {
        let start_time = std::time::Instant::now();

        // Create a simple term update message
        let term_update = crate::consumer::TermUpdateMessage {
            term_id: term_id.clone(),
            timestamp: chrono::Utc::now().timestamp(),
        };

        match update_analytics_tables(pool, metrics, &term_update).await {
            Ok(()) => {
                // Record processing duration
                metrics.record_analytics_processing_duration(start_time.elapsed());

                // Record successful consumption
                metrics.record_analytics_message_consumed();

                debug!("Successfully updated analytics for term {}", term_id);
                successful += 1;
            }
            Err(e) => {
                // Record failure
                metrics.record_analytics_message_failure();

                warn!("Failed to update analytics for term {}: {}", term_id, e);
                failed += 1;
                // Note: With mpsc channel, we don't have retry semantics
                // Failed updates are simply logged and counted
            }
        }
    }

    debug!(
        "Batch complete: {} successful, {} failed",
        successful, failed
    );

    // Record pending messages metric (0 since mpsc doesn't have a queue depth)
    metrics.record_analytics_messages_pending(0);

    Ok(successful + failed)
}
