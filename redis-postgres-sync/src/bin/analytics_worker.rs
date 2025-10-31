// Analytics worker binary
// Consumes term update messages from Redis and updates analytics tables
// Handles eventual consistency for triple_vault, triple_term, predicate_object, subject_predicate

use redis::{aio::MultiplexedConnection, Client};
use redis_postgres_sync::{Config, consumer::TermUpdateMessage, error::{Result, SyncError}};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    info!("Starting analytics worker");

    // Load configuration
    let config = Config::from_env().expect("Failed to load configuration");

    // Connect to PostgreSQL
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url)
        .await
        .map_err(|e| {
            error!("Failed to connect to PostgreSQL: {}", e);
            SyncError::Connection(format!("Failed to connect to PostgreSQL: {}", e))
        })?;

    info!("Connected to PostgreSQL database");

    // Connect to Redis
    let redis_client = Client::open(config.redis_url.clone()).map_err(SyncError::Redis)?;
    let mut redis_conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .map_err(SyncError::Redis)?;

    info!("Connected to Redis");

    // Create consumer group for analytics worker
    let consumer_group = "analytics_worker";
    let consumer_name = "worker_1";
    let stream_name = "term_updates";

    // Ensure consumer group exists
    let result: std::result::Result<String, redis::RedisError> = redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(stream_name)
        .arg(consumer_group)
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

    // Main processing loop
    loop {
        match process_batch(&mut redis_conn, &pool, stream_name, consumer_group, consumer_name).await
        {
            Ok(processed_count) => {
                if processed_count > 0 {
                    info!("Processed batch of {} term updates", processed_count);
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

    debug!("Received {} term update messages", messages.len());

    // Process all messages in a single transaction for efficiency
    let mut tx = pool.begin().await.map_err(|e| {
        SyncError::Sqlx(e)
    })?;

    for (_message_id, term_update) in &messages {
        match update_analytics_tables(&mut tx, term_update).await {
            Ok(_) => debug!("Updated analytics for term {}", term_update.term_id),
            Err(e) => {
                warn!(
                    "Failed to update analytics for term {}: {}",
                    term_update.term_id, e
                );
                // Continue processing other messages
            }
        }
    }

    tx.commit().await.map_err(|e| {
        SyncError::Sqlx(e)
    })?;

    // ACK all messages
    for (message_id, _) in &messages {
        let _: u64 = redis::cmd("XACK")
            .arg(stream_name)
            .arg(consumer_group)
            .arg(message_id)
            .query_async(redis_conn)
            .await
            .map_err(SyncError::Redis)?;
    }

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

async fn update_analytics_tables(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    term_update: &TermUpdateMessage,
) -> Result<()> {
    // Find all triples that reference this term (as the triple itself, counter, subject, predicate, or object)
    let affected_triples: Vec<(String, String, String, String, String)> = sqlx::query_as(
        r#"
        SELECT
            term_id::text,
            counter_term_id::text,
            subject_id::text,
            predicate_id::text,
            object_id::text
        FROM triple
        WHERE term_id = $1
           OR counter_term_id = $1
           OR subject_id = $1
           OR predicate_id = $1
           OR object_id = $1
        "#,
    )
    .bind(&term_update.term_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| SyncError::Sqlx(e))?;

    if affected_triples.is_empty() {
        info!("No triples affected by term update: {}", term_update.term_id);
        return Ok(());
    }

    info!("Found {} triples affected by term {}", affected_triples.len(), term_update.term_id);

    // Update analytics for each affected triple
    for (term_id, counter_term_id, subject_id, predicate_id, object_id) in affected_triples {
        // Update triple_vault (combine pro + counter vault data)
        update_triple_vault(tx, &term_id, &counter_term_id).await?;

        // Update triple_term (aggregate triple_vault by term)
        update_triple_term(tx, &term_id, &counter_term_id).await?;

        // Update predicate_object aggregates
        update_predicate_object(tx, &predicate_id, &object_id).await?;

        // Update subject_predicate aggregates
        update_subject_predicate(tx, &subject_id, &predicate_id).await?;
    }

    Ok(())
}

async fn update_triple_vault(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    term_id: &str,
    counter_term_id: &str,
) -> Result<()> {
    // Strip 0x prefix from counter_term_id if present (vault.term_id doesn't have 0x prefix)
    let counter_term_clean = counter_term_id.strip_prefix("0x").unwrap_or(counter_term_id);

    // Aggregate pro vault + counter vault data for each curve_id
    sqlx::query(
        r#"
        INSERT INTO triple_vault (term_id, counter_term_id, curve_id, total_shares, total_assets, position_count, market_cap, updated_at)
        SELECT
            $1::text,
            $2::text,
            COALESCE(v1.curve_id, v2.curve_id),
            COALESCE(v1.total_shares, 0) + COALESCE(v2.total_shares, 0),
            COALESCE(v1.total_assets, 0) + COALESCE(v2.total_assets, 0),
            COALESCE(v1.position_count, 0) + COALESCE(v2.position_count, 0),
            COALESCE(v1.market_cap, 0) + COALESCE(v2.market_cap, 0),
            NOW()
        FROM vault v1
        FULL OUTER JOIN vault v2 ON v1.curve_id = v2.curve_id
        WHERE v1.term_id = $1 OR v2.term_id = $3
        ON CONFLICT (term_id, counter_term_id, curve_id) DO UPDATE
        SET total_shares = EXCLUDED.total_shares,
            total_assets = EXCLUDED.total_assets,
            position_count = EXCLUDED.position_count,
            market_cap = EXCLUDED.market_cap,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(term_id)
    .bind(counter_term_id)  // Keep original with 0x for storage
    .bind(counter_term_clean)  // Use cleaned version for vault lookup
    .execute(&mut **tx)
    .await
    .map_err(|e| SyncError::Sqlx(e))?;

    Ok(())
}

async fn update_triple_term(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    term_id: &str,
    counter_term_id: &str,
) -> Result<()> {
    // Aggregate triple_vault across all curves
    sqlx::query(
        r#"
        INSERT INTO triple_term (term_id, counter_term_id, total_assets, total_market_cap, total_position_count, updated_at)
        SELECT
            term_id,
            counter_term_id,
            COALESCE(SUM(total_assets), 0),
            COALESCE(SUM(market_cap), 0),
            COALESCE(SUM(position_count), 0),
            NOW()
        FROM triple_vault
        WHERE term_id = $1 AND counter_term_id = $2
        GROUP BY term_id, counter_term_id
        ON CONFLICT (term_id, counter_term_id) DO UPDATE
        SET total_assets = EXCLUDED.total_assets,
            total_market_cap = EXCLUDED.total_market_cap,
            total_position_count = EXCLUDED.total_position_count,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(term_id)
    .bind(counter_term_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| SyncError::Sqlx(e))?;

    Ok(())
}

async fn update_predicate_object(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    predicate_id: &str,
    object_id: &str,
) -> Result<()> {
    // Aggregate by predicate-object pairs
    sqlx::query(
        r#"
        INSERT INTO predicate_object (predicate_id, object_id, triple_count, total_position_count, total_market_cap, updated_at)
        SELECT
            t.predicate_id,
            t.object_id,
            COUNT(DISTINCT t.term_id),
            COALESCE(SUM(tt.total_position_count), 0),
            COALESCE(SUM(tt.total_market_cap), 0),
            NOW()
        FROM triple t
        LEFT JOIN triple_term tt ON t.term_id = tt.term_id AND t.counter_term_id = tt.counter_term_id
        WHERE t.predicate_id = $1 AND t.object_id = $2
        GROUP BY t.predicate_id, t.object_id
        ON CONFLICT (predicate_id, object_id) DO UPDATE
        SET triple_count = EXCLUDED.triple_count,
            total_position_count = EXCLUDED.total_position_count,
            total_market_cap = EXCLUDED.total_market_cap,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(predicate_id)
    .bind(object_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| SyncError::Sqlx(e))?;

    Ok(())
}

async fn update_subject_predicate(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    subject_id: &str,
    predicate_id: &str,
) -> Result<()> {
    // Aggregate by subject-predicate pairs
    sqlx::query(
        r#"
        INSERT INTO subject_predicate (subject_id, predicate_id, triple_count, total_position_count, total_market_cap, updated_at)
        SELECT
            t.subject_id,
            t.predicate_id,
            COUNT(DISTINCT t.term_id),
            COALESCE(SUM(tt.total_position_count), 0),
            COALESCE(SUM(tt.total_market_cap), 0),
            NOW()
        FROM triple t
        LEFT JOIN triple_term tt ON t.term_id = tt.term_id AND t.counter_term_id = tt.counter_term_id
        WHERE t.subject_id = $1 AND t.predicate_id = $2
        GROUP BY t.subject_id, t.predicate_id
        ON CONFLICT (subject_id, predicate_id) DO UPDATE
        SET triple_count = EXCLUDED.triple_count,
            total_position_count = EXCLUDED.total_position_count,
            total_market_cap = EXCLUDED.total_market_cap,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(subject_id)
    .bind(predicate_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| SyncError::Sqlx(e))?;

    Ok(())
}
