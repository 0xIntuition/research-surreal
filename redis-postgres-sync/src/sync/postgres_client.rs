use sqlx::postgres::{PgPool, PgPoolOptions};
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};

use crate::core::types::{RindexerEvent, TransactionInformation};
use crate::error::{Result, SyncError};
use crate::processors::CascadeProcessor;
use crate::consumer::RedisPublisher;
use super::event_handlers;

pub struct PostgresClient {
    pool: PgPool,
    cascade_processor: CascadeProcessor,
    redis_publisher: Option<RedisPublisher>,
}

impl PostgresClient {
    pub async fn new(database_url: &str, redis_url: Option<&str>) -> Result<Self> {
        // Create connection pool
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(database_url)
            .await
            .map_err(|e| {
                error!("Failed to connect to PostgreSQL: {}", e);
                SyncError::Connection(format!("Failed to connect to PostgreSQL: {}", e))
            })?;

        info!("Connected to PostgreSQL database");

        // Run migrations with retry logic to handle race conditions on fresh database startup
        const MAX_RETRIES: u32 = 5;
        const INITIAL_DELAY_MS: u64 = 1000;

        let mut last_error = None;
        for attempt in 1..=MAX_RETRIES {
            match sqlx::migrate!("./migrations").run(&pool).await {
                Ok(_) => {
                    info!("Database migrations completed successfully");
                    break;
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt < MAX_RETRIES {
                        let delay_ms = INITIAL_DELAY_MS * 2_u64.pow(attempt - 1);
                        warn!(
                            "Migration attempt {}/{} failed, retrying in {}ms: {}",
                            attempt, MAX_RETRIES, delay_ms, last_error.as_ref().unwrap()
                        );
                        sleep(Duration::from_millis(delay_ms)).await;
                    } else {
                        error!("Failed to run migrations after {} attempts: {}", MAX_RETRIES, last_error.as_ref().unwrap());
                        return Err(SyncError::Connection(format!(
                            "Failed to run migrations after {} attempts: {}",
                            MAX_RETRIES, last_error.unwrap()
                        )));
                    }
                }
            }
        }

        // Initialize Redis publisher if URL provided
        let redis_publisher = if let Some(url) = redis_url {
            match RedisPublisher::new(url).await {
                Ok(publisher) => Some(publisher),
                Err(e) => {
                    warn!("Failed to initialize Redis publisher: {}. Analytics updates will be disabled.", e);
                    None
                }
            }
        } else {
            None
        };

        Ok(Self {
            pool,
            cascade_processor: CascadeProcessor::new(),
            redis_publisher,
        })
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
        // This will trigger the database triggers that update base tables (atom, triple, position, vault)
        event_handlers::process_event(
            &self.pool,
            &event.event_name,
            &event.event_data,
            &tx_info
        ).await.map_err(|e| {
            error!("Failed to process event '{}': {}", event.event_name, e);
            error!("Event data that failed: {}", serde_json::to_string_pretty(&event.event_data).unwrap_or_else(|_| "Failed to serialize for logging".to_string()));
            e
        })?;

        // After event insert and triggers, run cascade updates in a separate transaction
        // to update aggregated tables (vault, term)
        let term_ids = self.run_cascade_after_event(event).await?;

        // After successful commit, publish to Redis for analytics worker
        if let Some(publisher) = &self.redis_publisher {
            for term_id in term_ids {
                // TODO: Get counter_term_id for triples
                if let Err(e) = publisher.publish_term_update(&term_id, None).await {
                    warn!("Failed to publish term update to Redis: {}", e);
                    // Don't fail the whole operation if Redis publish fails
                }
            }
        }

        debug!("Successfully synced event: {}", event.event_name);
        Ok(())
    }

    /// Run cascade updates after event processing
    /// This updates aggregated tables (vault, term) based on triggered base table updates
    async fn run_cascade_after_event(
        &self,
        event: &RindexerEvent,
    ) -> Result<Vec<String>> {
        let mut tx = self.pool.begin().await.map_err(|e| {
            SyncError::Sqlx(e)
        })?;

        let term_ids = match event.event_name.as_str() {
            "Deposited" => {
                // Extract deposit event data
                if let Ok(receiver) = event.event_data.get("receiver")
                    .and_then(|v| v.as_str())
                    .ok_or(SyncError::Processing("Missing receiver".to_string())) {
                    if let Ok(term_id) = event.event_data.get("termId")
                        .and_then(|v| v.as_str())
                        .ok_or(SyncError::Processing("Missing termId".to_string())) {
                        if let Ok(curve_id) = event.event_data.get("curveId")
                            .and_then(|v| v.as_str())
                            .ok_or(SyncError::Processing("Missing curveId".to_string())) {
                            self.cascade_processor.process_position_change(
                                &mut tx, receiver, term_id, curve_id
                            ).await?;
                            vec![term_id.to_string()]
                        } else {
                            vec![]
                        }
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            }
            "Redeemed" => {
                // Extract redeem event data
                if let Ok(receiver) = event.event_data.get("receiver")
                    .and_then(|v| v.as_str())
                    .ok_or(SyncError::Processing("Missing receiver".to_string())) {
                    if let Ok(term_id) = event.event_data.get("termId")
                        .and_then(|v| v.as_str())
                        .ok_or(SyncError::Processing("Missing termId".to_string())) {
                        if let Ok(curve_id) = event.event_data.get("curveId")
                            .and_then(|v| v.as_str())
                            .ok_or(SyncError::Processing("Missing curveId".to_string())) {
                            self.cascade_processor.process_position_change(
                                &mut tx, receiver, term_id, curve_id
                            ).await?;
                            vec![term_id.to_string()]
                        } else {
                            vec![]
                        }
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            }
            "SharePriceChanged" => {
                // Extract price change event data
                if let Ok(term_id) = event.event_data.get("termId")
                    .and_then(|v| v.as_str())
                    .ok_or(SyncError::Processing("Missing termId".to_string())) {
                    if let Ok(curve_id) = event.event_data.get("curveId")
                        .and_then(|v| v.as_str())
                        .ok_or(SyncError::Processing("Missing curveId".to_string())) {
                        self.cascade_processor.process_price_change(
                            &mut tx, term_id, curve_id
                        ).await?;
                        vec![term_id.to_string()]
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            }
            "AtomCreated" => {
                // Extract atom creation data
                if let Ok(term_id) = event.event_data.get("termId")
                    .and_then(|v| v.as_str())
                    .ok_or(SyncError::Processing("Missing termId".to_string())) {
                    self.cascade_processor.process_atom_creation(&mut tx, term_id).await?;
                    vec![term_id.to_string()]
                } else {
                    vec![]
                }
            }
            "TripleCreated" => {
                // Extract triple creation data
                if let Ok(term_id) = event.event_data.get("termId")
                    .and_then(|v| v.as_str())
                    .ok_or(SyncError::Processing("Missing termId".to_string())) {
                    if let Ok(counter_term_id) = event.event_data.get("counterTermId")
                        .and_then(|v| v.as_str())
                        .ok_or(SyncError::Processing("Missing counterTermId".to_string())) {
                        self.cascade_processor.process_triple_creation(
                            &mut tx, term_id, counter_term_id
                        ).await?;
                        vec![term_id.to_string(), counter_term_id.to_string()]
                    } else {
                        vec![]
                    }
                } else {
                    vec![]
                }
            }
            _ => {
                debug!("No cascade processing for event type: {}", event.event_name);
                vec![]
            }
        };

        // Commit the transaction
        tx.commit().await.map_err(|e| {
            SyncError::Sqlx(e)
        })?;

        Ok(term_ids)
    }

    /// Get a reference to the database connection pool for custom operations
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    fn extract_transaction_info(&self, event: &RindexerEvent) -> Result<TransactionInformation> {
        let tx_info_value = event.event_data
            .get("transaction_information")
            .ok_or_else(|| SyncError::Processing("Missing transaction_information in event".to_string()))?;

        serde_json::from_value(tx_info_value.clone())
            .map_err(SyncError::Serde)
    }
}
