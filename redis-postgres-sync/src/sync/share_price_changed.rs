
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::{debug, error};

use crate::core::types::TransactionInformation;
use crate::error::{Result, SyncError};
use super::utils::parse_hex_to_u64;

#[derive(Debug, Serialize, Deserialize)]
pub struct SharePriceChangedEvent {
    #[serde(rename = "termId")]
    pub term_id: String,
    #[serde(rename = "curveId")]
    pub curve_id: String,
    #[serde(rename = "sharePrice")]
    pub share_price: String,
    #[serde(rename = "totalAssets")]
    pub total_assets: String,
    #[serde(rename = "totalShares")]
    pub total_shares: String,
    #[serde(rename = "vaultType")]
    pub vault_type: u8,
}

pub async fn handle_share_price_changed(
    pool: &PgPool,
    event: SharePriceChangedEvent,
    tx_info: &TransactionInformation,
) -> Result<()> {
    let log_index = parse_hex_to_u64(&tx_info.log_index)?;

    sqlx::query(
        r#"
        INSERT INTO share_price_changed_events (
            transaction_hash, log_index, term_id, curve_id, share_price,
            total_assets, total_shares, vault_type,
            address, block_hash, block_number, network, transaction_index, block_timestamp
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (transaction_hash, log_index) DO UPDATE SET
            term_id = EXCLUDED.term_id,
            curve_id = EXCLUDED.curve_id,
            share_price = EXCLUDED.share_price,
            total_assets = EXCLUDED.total_assets,
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
    .bind(&event.term_id)
    .bind(&event.curve_id)
    .bind(&event.share_price)
    .bind(&event.total_assets)
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
        error!("Failed to insert SharePriceChanged record: {}", e);
        SyncError::from(e)
    })?;

    debug!("Created SharePriceChanged record");
    Ok(())
}
