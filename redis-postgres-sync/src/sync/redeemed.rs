
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::{debug, error};

use crate::error::{Result, SyncError};
use crate::core::types::TransactionInformation;
use super::utils::{ensure_hex_prefix, parse_hex_to_u64, to_eip55_address};

#[derive(Debug, Serialize, Deserialize)]
pub struct RedeemedEvent {
    pub assets: String,
    #[serde(rename = "curveId")]
    pub curve_id: String,
    pub fees: String,
    pub receiver: String,
    pub sender: String,
    pub shares: String,
    #[serde(rename = "termId")]
    pub term_id: String,
    #[serde(rename = "totalShares")]
    pub total_shares: String,
    #[serde(rename = "vaultType")]
    pub vault_type: u8,
}

pub async fn handle_redeemed(
    pool: &PgPool,
    event: RedeemedEvent,
    tx_info: &TransactionInformation,
) -> Result<()> {
    let log_index = parse_hex_to_u64(&tx_info.log_index)?;

    // Format IDs with 0x prefix and addresses in EIP-55 format
    let term_id = ensure_hex_prefix(&event.term_id);
    let curve_id = ensure_hex_prefix(&event.curve_id);
    let receiver = to_eip55_address(&event.receiver)?;
    let sender = to_eip55_address(&event.sender)?;

    sqlx::query(
        r#"
        INSERT INTO redeemed_events (
            transaction_hash, log_index, assets, curve_id, fees, receiver, sender,
            shares, term_id, total_shares, vault_type,
            address, block_hash, block_number, network, transaction_index, block_timestamp
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
        ON CONFLICT (transaction_hash, log_index) DO UPDATE SET
            assets = EXCLUDED.assets,
            curve_id = EXCLUDED.curve_id,
            fees = EXCLUDED.fees,
            receiver = EXCLUDED.receiver,
            sender = EXCLUDED.sender,
            shares = EXCLUDED.shares,
            term_id = EXCLUDED.term_id,
            total_shares = EXCLUDED.total_shares,
            vault_type = EXCLUDED.vault_type,
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
    .bind(&event.assets)
    .bind(&curve_id)
    .bind(&event.fees)
    .bind(&receiver)
    .bind(&sender)
    .bind(&event.shares)
    .bind(&term_id)
    .bind(&event.total_shares)
    .bind(event.vault_type as i16)
    .bind(&tx_info.address)
    .bind(&tx_info.block_hash)
    .bind(tx_info.block_number as i64)
    .bind(&tx_info.network)
    .bind(tx_info.transaction_index as i64)
    .bind(&tx_info.block_timestamp)
    .execute(pool)
    .await
    .map_err(|e| {
        error!("Failed to insert Redeemed record: {}", e);
        SyncError::from(e)
    })?;

    debug!("Created Redeemed record");
    Ok(())
}
