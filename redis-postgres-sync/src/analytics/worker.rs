// Analytics worker implementation
// Consumes term update messages from Redis and updates analytics tables

use super::processor::update_analytics_tables;
use crate::{consumer::TermUpdateMessage, error::{Result, SyncError}, Config};
use redis::{aio::MultiplexedConnection, Client};
use sqlx::PgPool;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

const STREAM_NAME: &str = "term_updates";
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
        Some(suffix) => format!("{}-{}", ANALYTICS_CONSUMER_GROUP_PREFIX, suffix),
        None => format!("{}-worker", ANALYTICS_CONSUMER_GROUP_PREFIX),
    };

    let consumer_name = config.consumer_name.clone();

    info!(
        "Analytics worker using consumer group '{}' and consumer name '{}'",
        consumer_group, consumer_name
    );

    // Ensure consumer group exists
    let result: std::result::Result<String, redis::RedisError> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(STREAM_NAME)
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

    info!("Analytics worker started, processing messages...");

    let mut total_processed = 0u64;
    let start_time = std::time::Instant::now();

    // Main processing loop
    loop {
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                info!("Analytics worker received shutdown signal");
                break;
            }
            result = process_batch(&mut redis_conn, &pool, STREAM_NAME, &consumer_group, &consumer_name) => {
                match result {
                    Ok(processed_count) => {
                        if processed_count > 0 {
                            total_processed += processed_count as u64;
                            let elapsed = start_time.elapsed().as_secs();
                            let rate = if elapsed > 0 {
                                total_processed as f64 / elapsed as f64
                            } else {
                                0.0
                            };
                            info!(
                                "Processed batch of {} term updates (total: {}, rate: {:.2} msg/s)",
                                processed_count, total_processed, rate
                            );
                        }
                    }
                    Err(e) => {
                        error!("Error processing batch: {}", e);
                        // Sleep on error to avoid tight loop
                        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                    }
                }
            }
        }
    }

    info!("Analytics worker stopped");
    Ok(())
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

    info!("Received {} term update messages from analytics stream", messages.len());

    let mut successful = 0;
    let mut failed = 0;
    let mut messages_to_ack = Vec::new();

    // Process each message
    for (message_id, term_update) in &messages {
        match update_analytics_tables(pool, term_update).await {
            Ok(_) => {
                info!("Successfully updated analytics for term {}", term_update.term_id);
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

    info!(
        "Batch complete: {} successful, {} failed (will retry)",
        successful, failed
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
