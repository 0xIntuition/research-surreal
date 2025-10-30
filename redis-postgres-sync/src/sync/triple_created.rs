
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
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

pub async fn handle_triple_created(
    pool: &PgPool,
    event: TripleCreatedEvent,
    tx_info: &TransactionInformation,
) -> Result<()> {
    let log_index = parse_hex_to_u64(&tx_info.log_index)?;

    sqlx::query(
        r#"
        INSERT INTO triple_created_events (
            transaction_hash, log_index, creator, object_id, predicate_id, subject_id, term_id,
            address, block_hash, block_number, network, transaction_index, block_timestamp
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (transaction_hash, log_index) DO UPDATE SET
            creator = EXCLUDED.creator,
            object_id = EXCLUDED.object_id,
            predicate_id = EXCLUDED.predicate_id,
            subject_id = EXCLUDED.subject_id,
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
    .bind(&event.creator)
    .bind(&event.object_id)
    .bind(&event.predicate_id)
    .bind(&event.subject_id)
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
        error!("Failed to insert TripleCreated record: {}", e);
        SyncError::from(e)
    })?;

    debug!("Created TripleCreated record");
    Ok(())
}
