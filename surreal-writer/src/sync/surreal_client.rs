use surrealdb::{engine::any::connect, Surreal};
use tracing::{debug, error, info};

use crate::core::types::{RindexerEvent, TransactionInformation};
use crate::error::{Result, SyncError};
use super::event_handlers;

pub struct SurrealClient {
    db: Surreal<surrealdb::engine::any::Any>,
}

impl SurrealClient {
    pub async fn new(
        url: &str,
        username: &str,
        password: &str,
        namespace: &str,
        database: &str,
    ) -> Result<Self> {
        let db = connect(url).await?;

        db.signin(surrealdb::opt::auth::Root {
            username,
            password,
        }).await?;

        db.use_ns(namespace).use_db(database).await?;

        info!("Connected to SurrealDB at {} (ns: {}, db: {})", url, namespace, database);

        Ok(Self { db })
    }

    pub async fn sync_event(&self, event: &RindexerEvent) -> Result<()> {
        debug!("Starting sync for event: {}", event.event_name);
        debug!("Event signature hash: {}", event.event_signature_hash);
        debug!("Event network: {}", event.network);
        debug!("Full event data: {}", serde_json::to_string_pretty(&event.event_data).unwrap_or_else(|_| "Failed to serialize event_data for logging".to_string()));
        
        // Extract transaction information
        let tx_info = self.extract_transaction_info(event)?;
        debug!("Extracted transaction info: {}", serde_json::to_string_pretty(&tx_info).unwrap_or_else(|_| "Failed to serialize tx_info for logging".to_string()));
        
        // Process the event using the appropriate handler which will create the DB entry
        event_handlers::process_event(
            &self.db,
            &event.event_name,
            &event.event_data,
            &tx_info
        ).await.map_err(|e| {
            error!("Failed to process event '{}': {}", event.event_name, e);
            error!("Event data that failed: {}", serde_json::to_string_pretty(&event.event_data).unwrap_or_else(|_| "Failed to serialize for logging".to_string()));
            e
        })?;

        debug!("Successfully synced event: {}", event.event_name);
        Ok(())
    }
    
    /// Get a reference to the database connection for custom operations
    pub fn db(&self) -> &Surreal<surrealdb::engine::any::Any> {
        &self.db
    }

    fn extract_transaction_info(&self, event: &RindexerEvent) -> Result<TransactionInformation> {
        let tx_info_value = event.event_data
            .get("transaction_information")
            .ok_or_else(|| SyncError::Processing("Missing transaction_information in event".to_string()))?;

        serde_json::from_value(tx_info_value.clone())
            .map_err(SyncError::Serde)
    }
}