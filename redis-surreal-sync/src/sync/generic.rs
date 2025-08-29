use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use surrealdb::Surreal;
use tracing::{debug, error};

use crate::error::{Result, SyncError};
use crate::core::types::TransactionInformation;
use super::utils::parse_hex_to_u64;

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

pub async fn handle_generic_event(
    db: &Surreal<surrealdb::engine::any::Any>,
    event_name: &str,
    event_data: Value,
    tx_info: &TransactionInformation,
) -> Result<()> {
    let mut result = event_data;
    
    if let Some(obj) = result.as_object_mut() {
        let tx_info_value = serde_json::to_value(TransactionInfo {
            address: tx_info.address.clone(),
            block_hash: tx_info.block_hash.clone(),
            block_number: tx_info.block_number,
            log_index: parse_hex_to_u64(&tx_info.log_index)?,
            network: tx_info.network.clone(),
            transaction_hash: tx_info.transaction_hash.clone(),
            transaction_index: tx_info.transaction_index,
            block_timestamp: tx_info.block_timestamp,
        }).map_err(|e| {
            error!("Failed to serialize transaction info: {}", e);
            SyncError::Processing(e.to_string())
        })?;
        
        obj.remove("transaction_information");
        obj.insert("transaction_information".to_string(), tx_info_value);
    }
    
    let table_name = event_name.to_lowercase();
    
    // Ensure we're using a serde_json::Value to work around SurrealDB enum serialization issue
    let record_value = serde_json::to_value(&result).map_err(SyncError::Serde)?;
    
    let _: Option<Value> = db
        .create(&table_name)
        .content(record_value)
        .await
        .map_err(|e| {
            error!("Failed to create {} record: {}", event_name, e);
            SyncError::from(e)
        })?;
    
    debug!("Created {} record", event_name);
    Ok(())
}