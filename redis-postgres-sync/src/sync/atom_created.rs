use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::{debug, error};

use super::utils::{ensure_hex_prefix, parse_hex_to_u64, to_eip55_address};
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
    // Decode atom_data from hex to bytes
    let decoded_bytes = hex::decode(&event.atom_data)
        .map_err(|e| {
            error!("Failed to decode atom_data from hex: {}", e);
            SyncError::ParseError(format!("Invalid hex in atom_data: {}", e))
        })?;

    // Try to convert to UTF-8 string, fall back to storing raw bytes if it fails
    let (decoded_atom_data, raw_data) = match String::from_utf8(decoded_bytes.clone()) {
        Ok(utf8_string) => {
            debug!("Decoded atom_data as UTF-8: {}", utf8_string);
            (Some(utf8_string), None)
        }
        Err(e) => {
            debug!("Failed to decode atom_data as UTF-8 ({}), storing raw bytes instead", e);
            (None, Some(decoded_bytes))
        }
    };

    let log_index = parse_hex_to_u64(&tx_info.log_index)?;

    // Format IDs with 0x prefix and addresses in EIP-55 format
    let term_id = ensure_hex_prefix(&event.term_id);
    let atom_wallet = to_eip55_address(&event.atom_wallet)?;
    let creator = to_eip55_address(&event.creator)?;

    // Insert into atom_created_events table
    sqlx::query(
        r#"
        INSERT INTO atom_created_events (
            transaction_hash, log_index, atom_data, raw_data, atom_wallet, creator, term_id,
            address, block_hash, block_number, network, transaction_index, block_timestamp
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (transaction_hash, log_index) DO UPDATE SET
            atom_data = EXCLUDED.atom_data,
            raw_data = EXCLUDED.raw_data,
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
    .bind(&raw_data)
    .bind(&atom_wallet)
    .bind(&creator)
    .bind(&term_id)
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
