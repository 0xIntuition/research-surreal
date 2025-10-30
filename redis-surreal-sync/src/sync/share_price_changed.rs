use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::Surreal;
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

#[derive(Debug, Serialize, Deserialize)]
struct SharePriceChangedRecord {
    #[serde(rename = "termId")]
    term_id: String,
    #[serde(rename = "curveId")]
    curve_id: String,
    #[serde(rename = "sharePrice")]
    share_price: String,
    #[serde(rename = "totalAssets")]
    total_assets: String,
    #[serde(rename = "totalShares")]
    total_shares: String,
    #[serde(rename = "vaultType")]
    vault_type: u8,
    transaction_information: TransactionInfo,
}

#[derive(Debug, Serialize, Deserialize)]
struct TransactionInfo {
    address: String,
    block_hash: String,
    block_number: u64,
    log_index: u64,
    network: String,
    transaction_hash: String,
    transaction_index: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    block_timestamp: Option<DateTime<Utc>>,
}

pub async fn handle_share_price_changed(
    db: &Surreal<surrealdb::engine::any::Any>,
    event: SharePriceChangedEvent,
    tx_info: &TransactionInformation,
) -> Result<()> {
    let record = SharePriceChangedRecord {
        term_id: event.term_id,
        curve_id: event.curve_id,
        share_price: event.share_price,
        total_assets: event.total_assets,
        total_shares: event.total_shares,
        vault_type: event.vault_type,
        transaction_information: TransactionInfo {
            address: tx_info.address.clone(),
            block_hash: tx_info.block_hash.clone(),
            block_number: tx_info.block_number,
            log_index: parse_hex_to_u64(&tx_info.log_index)?,
            network: tx_info.network.clone(),
            transaction_hash: tx_info.transaction_hash.clone(),
            transaction_index: tx_info.transaction_index,
            block_timestamp: tx_info.block_timestamp,
        },
    };

    let _: Option<SharePriceChangedRecord> =
        db.create("share_price_changed")
            .content(record)
            .await
            .map_err(|e| {
                error!("Failed to create SharePriceChanged record: {}", e);
                SyncError::from(e)
            })?;

    debug!("Created SharePriceChanged record");
    Ok(())
}
