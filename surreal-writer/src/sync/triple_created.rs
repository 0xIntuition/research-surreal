use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::Surreal;
use tracing::{debug, error};

use super::utils::parse_hex_to_u64;
use crate::core::types::TransactionInformation;
use crate::error::{Result, SyncError};

#[derive(Debug, Serialize, Deserialize)]
pub struct TripleCreatedEvent {
    pub creator: String,
    #[serde(rename = "objectId")]
    pub object_id: String,
    #[serde(rename = "predicateId")]
    pub predicate_id: String,
    #[serde(rename = "subjectId")]
    pub subject_id: String,
    #[serde(rename = "termId")]
    pub term_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct TripleCreatedRecord {
    creator: String,
    #[serde(rename = "objectId")]
    object_id: String,
    #[serde(rename = "predicateId")]
    predicate_id: String,
    #[serde(rename = "subjectId")]
    subject_id: String,
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

pub async fn handle_triple_created(
    db: &Surreal<surrealdb::engine::any::Any>,
    event: TripleCreatedEvent,
    tx_info: &TransactionInformation,
) -> Result<()> {
    let record = TripleCreatedRecord {
        creator: event.creator,
        object_id: event.object_id,
        predicate_id: event.predicate_id,
        subject_id: event.subject_id,
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

    let _: Option<TripleCreatedRecord> =
        db.create("triplecreated")
            .content(record)
            .await
            .map_err(|e| {
                error!("Failed to create TripleCreated record: {}", e);
                SyncError::from(e)
            })?;

    debug!("Created TripleCreated record");
    Ok(())
}
