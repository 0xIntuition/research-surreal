use serde_json::Value;
use sqlx::PgPool;
use tracing::{debug, error};

use super::utils::parse_hex_to_u64;
use crate::core::types::TransactionInformation;
use crate::error::{Result, SyncError};

pub async fn handle_generic_event(
    pool: &PgPool,
    event_name: &str,
    event_data: Value,
    tx_info: &TransactionInformation,
) -> Result<()> {
    let log_index = parse_hex_to_u64(&tx_info.log_index)?;

    // Store the event data as JSONB
    let event_data_json = sqlx::types::Json(event_data);

    sqlx::query(
        r#"
        INSERT INTO generic_events (
            transaction_hash, log_index, event_name, event_data,
            address, block_hash, block_number, network, transaction_index, block_timestamp
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (transaction_hash, log_index) DO UPDATE SET
            event_name = EXCLUDED.event_name,
            event_data = EXCLUDED.event_data,
            address = EXCLUDED.address,
            block_hash = EXCLUDED.block_hash,
            block_number = EXCLUDED.block_number,
            network = EXCLUDED.network,
            transaction_index = EXCLUDED.transaction_index,
            block_timestamp = EXCLUDED.block_timestamp
        "#,
    )
    .bind(&tx_info.transaction_hash)
    .bind(log_index as i64)
    .bind(event_name)
    .bind(event_data_json)
    .bind(&tx_info.address)
    .bind(&tx_info.block_hash)
    .bind(tx_info.block_number as i64)
    .bind(&tx_info.network)
    .bind(tx_info.transaction_index as i64)
    .bind(tx_info.block_timestamp)
    .execute(pool)
    .await
    .map_err(|e| {
        error!("Failed to insert {event_name} record: {e}");
        SyncError::from(e)
    })?;

    debug!("Created {event_name} record");
    Ok(())
}
