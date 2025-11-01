use serde_json::Value;
use sqlx::PgPool;
use tracing::{debug, error};

use crate::error::Result;
use crate::core::types::TransactionInformation;

use super::atom_created::{AtomCreatedEvent, handle_atom_created};
use super::deposited::{DepositedEvent, handle_deposited};
use super::triple_created::{TripleCreatedEvent, handle_triple_created};
use super::redeemed::{RedeemedEvent, handle_redeemed};
use super::share_price_changed::{SharePriceChangedEvent, handle_share_price_changed};
use super::generic::handle_generic_event;

/// Process an event and store it directly in PostgreSQL
pub async fn process_event(
    pool: &PgPool,
    event_name: &str,
    event_data: &Value,
    tx_info: &TransactionInformation,
) -> Result<()> {
    debug!("Processing event '{}' with transaction info: {}", event_name, serde_json::to_string_pretty(&tx_info).unwrap_or_else(|_| "Failed to serialize tx_info for logging".to_string()));

    match event_name {
        "AtomCreated" => {
            debug!("Processing AtomCreated event with raw data: {}", serde_json::to_string_pretty(&event_data).unwrap_or_else(|_| "Failed to serialize event_data for logging".to_string()));
            let event: AtomCreatedEvent = serde_json::from_value(event_data.clone())
                .map_err(|e| {
                    error!("Failed to deserialize AtomCreated event: {}", e);
                    error!("Raw event data: {}", serde_json::to_string_pretty(&event_data).unwrap_or_else(|_| "Failed to serialize for logging".to_string()));
                    e
                })?;
            handle_atom_created(pool, event, tx_info).await?
        },
        "Deposited" => {
            debug!("Processing Deposited event with raw data: {}", serde_json::to_string_pretty(&event_data).unwrap_or_else(|_| "Failed to serialize event_data for logging".to_string()));
            let event: DepositedEvent = serde_json::from_value(event_data.clone())
                .map_err(|e| {
                    error!("Failed to deserialize Deposited event: {}", e);
                    error!("Raw event data: {}", serde_json::to_string_pretty(&event_data).unwrap_or_else(|_| "Failed to serialize for logging".to_string()));
                    e
                })?;
            handle_deposited(pool, event, tx_info).await?
        },
        "TripleCreated" => {
            debug!("Processing TripleCreated event with raw data: {}", serde_json::to_string_pretty(&event_data).unwrap_or_else(|_| "Failed to serialize event_data for logging".to_string()));
            let event: TripleCreatedEvent = serde_json::from_value(event_data.clone())
                .map_err(|e| {
                    error!("Failed to deserialize TripleCreated event: {}", e);
                    error!("Raw event data: {}", serde_json::to_string_pretty(&event_data).unwrap_or_else(|_| "Failed to serialize for logging".to_string()));
                    e
                })?;
            handle_triple_created(pool, event, tx_info).await?
        },
        "Redeemed" => {
            debug!("Processing Redeemed event with raw data: {}", serde_json::to_string_pretty(&event_data).unwrap_or_else(|_| "Failed to serialize event_data for logging".to_string()));
            let event: RedeemedEvent = serde_json::from_value(event_data.clone())
                .map_err(|e| {
                    error!("Failed to deserialize Redeemed event: {}", e);
                    error!("Raw event data: {}", serde_json::to_string_pretty(&event_data).unwrap_or_else(|_| "Failed to serialize for logging".to_string()));
                    e
                })?;
            handle_redeemed(pool, event, tx_info).await?
        },
        "SharePriceChanged" => {
            debug!("Processing SharePriceChanged event with raw data: {}", serde_json::to_string_pretty(&event_data).unwrap_or_else(|_| "Failed to serialize event_data for logging".to_string()));
            let event: SharePriceChangedEvent = serde_json::from_value(event_data.clone())
                .map_err(|e| {
                    error!("Failed to deserialize SharePriceChanged event: {}", e);
                    error!("Raw event data: {}", serde_json::to_string_pretty(&event_data).unwrap_or_else(|_| "Failed to serialize for logging".to_string()));
                    e
                })?;
            handle_share_price_changed(pool, event, tx_info).await?
        },
        _ => {
            debug!("Processing generic event '{}' with raw data: {}", event_name, serde_json::to_string_pretty(&event_data).unwrap_or_else(|_| "Failed to serialize event_data for logging".to_string()));
            handle_generic_event(pool, event_name, event_data.clone(), tx_info).await?
        },
    }

    debug!("Processed {} event", event_name);
    Ok(())
}
