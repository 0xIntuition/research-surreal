// Analytics worker implementation
// Consumes term update messages from Redis and updates analytics tables

use super::processor::update_analytics_tables;
use crate::{
    consumer::TermUpdateMessage,
    error::{Result, SyncError},
    Config,
};
use redis::{aio::MultiplexedConnection, Client};
use sqlx::PgPool;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const ANALYTICS_CONSUMER_GROUP_PREFIX: &str = "analytics";

pub async fn start_analytics_worker(
    config: Config,
    pool: PgPool,
    cancellation_token: CancellationToken,
) -> Result<()> {
    info!("Starting analytics worker");

    // Connect to Redis
    let redis_client = Client::open(config.redis_url.clone()).map_err(SyncError::Redis)?;
    let mut redis_conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .map_err(SyncError::Redis)?;

    info!("Analytics worker connected to Redis");

    // Build consumer group name with suffix
    let consumer_group = match &config.consumer_group_suffix {
        Some(suffix) => format!("{ANALYTICS_CONSUMER_GROUP_PREFIX}-{suffix}"),
        None => format!("{ANALYTICS_CONSUMER_GROUP_PREFIX}-worker"),
    };

    let consumer_name = config.consumer_name.clone();
    let stream_name = config.analytics_stream_name.clone();

    info!(
        "Analytics worker using stream '{}', consumer group '{}', and consumer name '{}'",
        stream_name, consumer_group, consumer_name
    );

    // Ensure consumer group exists
    let result: std::result::Result<String, redis::RedisError> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&stream_name)
        .arg(&consumer_group)
        .arg("0")
        .arg("MKSTREAM")
        .query_async(&mut redis_conn)
        .await;

    match result {
        Ok(_) => info!("Created consumer group '{}'", consumer_group),
        Err(e) if e.to_string().contains("BUSYGROUP") => {
            debug!("Consumer group '{}' already exists", consumer_group)
        }
        Err(e) => {
            error!("Failed to create consumer group: {}", e);
            return Err(SyncError::Redis(e));
        }
    }

    // Log initial stream state
    match get_stream_pending_count(&mut redis_conn, &stream_name, &consumer_group).await {
        Ok(pending) => {
            info!(
                "Analytics worker started, {} pending messages in stream",
                pending
            );
        }
        Err(e) => {
            info!("Analytics worker started, processing messages...");
            debug!("Could not get initial pending count: {}", e);
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

    // Rate limiting: max messages per second
    // This prevents overwhelming the system during message floods
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
            result = process_batch(&mut redis_conn, &pool, &stream_name, &consumer_group, &consumer_name) => {
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
                                // Get stream pending count
                                match get_stream_pending_count(&mut redis_conn, &stream_name, &consumer_group).await {
                                    Ok(pending) => {
                                        info!(
                                            "Analytics summary for stream '{}': processed {} msgs total ({:.1} msg/s avg), {} pending",
                                            stream_name, total_processed, rate, pending
                                        );
                                    }
                                    Err(e) => {
                                        info!(
                                            "Analytics summary for stream '{}': processed {} msgs total ({:.1} msg/s avg)",
                                            stream_name, total_processed, rate
                                        );
                                        debug!("Failed to get pending count: {}", e);
                                    }
                                }

                                last_summary_time = std::time::Instant::now();
                            }

                            // Rate limiting: enforce minimum interval between batches
                            let batch_elapsed = last_batch_time.elapsed();
                            if batch_elapsed.as_millis() < min_batch_interval_ms as u128 {
                                let sleep_ms = min_batch_interval_ms - batch_elapsed.as_millis() as u64;
                                tokio::time::sleep(tokio::time::Duration::from_millis(sleep_ms)).await;
                            }

                            // Check if we're exceeding rate limit
                            if rate > max_messages_per_second as f64 {
                                warn!(
                                    "Processing rate ({:.2} msg/s) exceeds limit ({} msg/s), throttling...",
                                    rate, max_messages_per_second
                                );
                                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
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

                        // Calculate exponential backoff: 1s, 2s, 4s, 8s, 16s, 32s, 60s (max)
                        let backoff_secs = std::cmp::min(
                            INITIAL_BACKOFF_SECS * 2u64.pow(consecutive_failures.saturating_sub(1)),
                            MAX_BACKOFF_SECS
                        );

                        warn!(
                            "Backing off for {} seconds before retry (consecutive failures: {})",
                            backoff_secs, consecutive_failures
                        );
                        tokio::time::sleep(tokio::time::Duration::from_secs(backoff_secs)).await;

                        // If we've hit max consecutive failures, log a critical error
                        if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                            error!(
                                "Analytics worker has failed {} times consecutively. Continuing with max backoff.",
                                MAX_CONSECUTIVE_FAILURES
                            );
                            // Continue processing but keep the backoff at maximum
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

/// Get the number of pending messages in the stream for this consumer group
async fn get_stream_pending_count(
    redis_conn: &mut MultiplexedConnection,
    stream_name: &str,
    consumer_group: &str,
) -> Result<u64> {
    // XPENDING returns summary: [min_id, max_id, count, consumers]
    let result: redis::Value = redis::cmd("XPENDING")
        .arg(stream_name)
        .arg(consumer_group)
        .query_async(redis_conn)
        .await
        .map_err(SyncError::Redis)?;

    // Parse the third element which is the pending count
    if let redis::Value::Array(ref arr) = result {
        if arr.len() >= 3 {
            if let redis::Value::Int(count) = arr[2] {
                return Ok(count as u64);
            }
        }
    }

    Ok(0)
}

async fn process_batch(
    redis_conn: &mut MultiplexedConnection,
    pool: &PgPool,
    stream_name: &str,
    consumer_group: &str,
    consumer_name: &str,
) -> Result<usize> {
    // Read messages from stream
    let mut cmd = redis::cmd("XREADGROUP");
    cmd.arg("GROUP")
        .arg(consumer_group)
        .arg(consumer_name)
        .arg("COUNT")
        .arg(100) // Process 100 messages at a time
        .arg("BLOCK")
        .arg(5000) // Block for 5 seconds
        .arg("STREAMS")
        .arg(stream_name)
        .arg(">"); // Only new messages

    let result: redis::Value = cmd
        .query_async(redis_conn)
        .await
        .map_err(SyncError::Redis)?;

    let messages = parse_messages(result)?;

    if messages.is_empty() {
        return Ok(0);
    }

    let first_msg_id = messages
        .first()
        .map(|(id, _)| id.as_str())
        .unwrap_or("none");
    let last_msg_id = messages.last().map(|(id, _)| id.as_str()).unwrap_or("none");

    debug!(
        "Received {} messages from stream '{}' (IDs: {} ... {})",
        messages.len(),
        stream_name,
        first_msg_id,
        last_msg_id
    );

    let mut successful = 0;
    let mut failed = 0;
    let mut messages_to_ack = Vec::new();

    // Process each message
    for (message_id, term_update) in &messages {
        match update_analytics_tables(pool, term_update).await {
            Ok(_) => {
                debug!(
                    "Successfully updated analytics for term {}",
                    term_update.term_id
                );
                messages_to_ack.push(message_id);
                successful += 1;
            }
            Err(e) => {
                warn!(
                    "Failed to update analytics for term {} (message_id: {}): {}. Message will NOT be ACK'd and will be retried.",
                    term_update.term_id, message_id, e
                );
                failed += 1;
                // Don't add to messages_to_ack - this message will be retried
            }
        }
    }

    // Only ACK successful messages
    for message_id in &messages_to_ack {
        let _: u64 = redis::cmd("XACK")
            .arg(stream_name)
            .arg(consumer_group)
            .arg(message_id)
            .query_async(redis_conn)
            .await
            .map_err(SyncError::Redis)?;
    }

    debug!(
        "Batch complete for stream '{}': {} successful, {} failed (will retry)",
        stream_name, successful, failed
    );

    Ok(messages.len())
}

fn parse_messages(value: redis::Value) -> Result<Vec<(String, TermUpdateMessage)>> {
    let mut messages = Vec::new();

    if let redis::Value::Array(streams) = value {
        for stream in streams {
            if let redis::Value::Array(stream_data) = stream {
                if stream_data.len() >= 2 {
                    if let redis::Value::Array(stream_messages) = &stream_data[1] {
                        for message in stream_messages {
                            if let Some((id, term_update)) = parse_single_message(message)? {
                                messages.push((id, term_update));
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(messages)
}

fn parse_single_message(message: &redis::Value) -> Result<Option<(String, TermUpdateMessage)>> {
    if let redis::Value::Array(message_data) = message {
        if message_data.len() >= 2 {
            let message_id = match &message_data[0] {
                redis::Value::BulkString(id) => String::from_utf8_lossy(id).to_string(),
                _ => return Ok(None),
            };

            if let redis::Value::Array(fields) = &message_data[1] {
                let mut field_map = HashMap::new();

                for chunk in fields.chunks(2) {
                    if chunk.len() == 2 {
                        let key = match &chunk[0] {
                            redis::Value::BulkString(k) => String::from_utf8_lossy(k).to_string(),
                            _ => continue,
                        };
                        let value = match &chunk[1] {
                            redis::Value::BulkString(v) => String::from_utf8_lossy(v).to_string(),
                            _ => continue,
                        };
                        field_map.insert(key, value);
                    }
                }

                if let Some(data) = field_map.get("data") {
                    let term_update: TermUpdateMessage =
                        serde_json::from_str(data).map_err(SyncError::Serde)?;
                    return Ok(Some((message_id, term_update)));
                }
            }
        }
    }

    Ok(None)
}
