use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::{RecordId, Surreal};
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

#[derive(Debug, Serialize, Deserialize)]
struct AtomCreatedRecord {
    #[serde(rename = "atomData")]
    atom_data: String, // Decoded string (e.g., IPFS URI)
    #[serde(rename = "atomWallet")]
    atom_wallet: String,
    #[serde(rename = "creator")]
    creator: RecordId, // Reference to account:{address}
    #[serde(rename = "termId")]
    term_id: String,
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

#[derive(Debug, Serialize, Deserialize)]
struct Account {
    address: String,
}

pub async fn handle_atom_created(
    db: &Surreal<surrealdb::engine::any::Any>,
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

    // Ensure the account exists in the database
    let _: Option<Account> = db
        .upsert(("account", event.creator.clone()))
        .content(Account {
            address: event.creator.clone(),
        })
        .await
        .map_err(|e| {
            error!("Failed to create Account: {}", event.creator.clone());
            SyncError::from(e)
        })?;

    let record = AtomCreatedRecord {
        atom_data: decoded_atom_data,
        atom_wallet: event.atom_wallet,
        creator: RecordId::from(("account", event.creator.clone())),
        term_id: event.term_id,
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

    let _: Option<AtomCreatedRecord> = db
        .upsert(("atomcreated", record.term_id.clone()))
        .content(record)
        .await
        .map_err(|e| {
            error!("Failed to create AtomCreated record: {}", e);
            SyncError::from(e)
        })?;

    debug!("Created AtomCreated record");
    Ok(())
}
