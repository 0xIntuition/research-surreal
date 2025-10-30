use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::{debug, error};

use super::utils::parse_hex_to_u64;
use crate::core::types::TransactionInformation;
use crate::error::{Result, SyncError};

#[derive(Debug, Serialize, Deserialize)]
pub struct AtomCreatedEvent {
    #[serde(rename = "atomData")]
    pub atom_data: String, // Hex-encoded string that will be decoded
    #[serde(rename = "atomWallet")]
    pub atom_wallet: String,
    pub creator: String,
    #[serde(rename = "termId")]
    pub term_id: String,
}

pub async fn handle_atom_created(
    pool: &PgPool,
    event: AtomCreatedEvent,
    tx_info: &TransactionInformation,
) -> Result<()> {
    // Decode atom_data from hex to UTF-8 string
    let decoded_atom_data = hex::decode(&event.atom_data)
        .map_err(|e| {
            error!("Failed to decode atom_data from hex: {}", e);
            SyncError::ParseError(format!("Invalid hex in atom_data: {}", e))
        })
        .and_then(|bytes| {
            String::from_utf8(bytes).map_err(|e| {
                error!("Failed to convert atom_data to UTF-8 string: {}", e);
                SyncError::ParseError(format!("Invalid UTF-8 in atom_data: {}", e))
            })
        })?;

    debug!("Decoded atom_data: {}", decoded_atom_data);

    let log_index = parse_hex_to_u64(&tx_info.log_index)?;

    // Insert into atom_created_events table
    sqlx::query(
        r#"
        INSERT INTO atom_created_events (
            transaction_hash, log_index, atom_data, atom_wallet, creator, term_id,
            address, block_hash, block_number, network, transaction_index, block_timestamp
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT (transaction_hash, log_index) DO UPDATE SET
            atom_data = EXCLUDED.atom_data,
            atom_wallet = EXCLUDED.atom_wallet,
            creator = EXCLUDED.creator,
            term_id = EXCLUDED.term_id,
            address = EXCLUDED.address,
            block_hash = EXCLUDED.block_hash,
            block_number = EXCLUDED.block_number,
            network = EXCLUDED.network,
            transaction_index = EXCLUDED.transaction_index,
            block_timestamp = EXCLUDED.block_timestamp
        "#
    )
    .bind(&tx_info.transaction_hash)
    .bind(log_index as i64)
    .bind(&decoded_atom_data)
    .bind(&event.atom_wallet)
    .bind(&event.creator)
    .bind(&event.term_id)
    .bind(&tx_info.address)
    .bind(&tx_info.block_hash)
    .bind(tx_info.block_number as i64)
    .bind(&tx_info.network)
    .bind(tx_info.transaction_index as i64)
    .bind(&tx_info.block_timestamp)
    .execute(pool)
    .await
    .map_err(|e| {
        error!("Failed to insert AtomCreated record: {}", e);
        SyncError::from(e)
    })?;

    debug!("Created AtomCreated record");
    Ok(())
}
