use sqlx::postgres::{PgPool, PgPoolOptions};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, warn};

use super::event_handlers;
use super::utils::{ensure_hex_prefix, to_eip55_address};
use crate::consumer::RedisPublisher;
use crate::core::types::{RindexerEvent, TransactionInformation};
use crate::error::{Result, SyncError};
use crate::monitoring::Metrics;
use crate::processors::{CascadeProcessor, TermUpdater};

pub struct PostgresClient {
    pool: PgPool,
    cascade_processor: CascadeProcessor,
    redis_publisher: Option<Mutex<RedisPublisher>>,
    metrics: Metrics,
}

impl PostgresClient {
    pub async fn new(
        database_url: &str,
        redis_url: Option<&str>,
        analytics_stream_name: String,
        metrics: Metrics,
    ) -> Result<Self> {
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

        for attempt in 1..=MAX_RETRIES {
            match sqlx::migrate!("./migrations").run(&pool).await {
                Ok(_) => {
                    info!("Database migrations completed successfully");
                    break;
                }
                Err(e) => {
                    if attempt < MAX_RETRIES {
                        let delay_ms = INITIAL_DELAY_MS * 2_u64.pow(attempt - 1);
                        warn!(
                            "Migration attempt {}/{} failed, retrying in {}ms: {}",
                            attempt, MAX_RETRIES, delay_ms, e
                        );
                        sleep(Duration::from_millis(delay_ms)).await;
                    } else {
                        error!(
                            "Failed to run migrations after {} attempts: {}",
                            MAX_RETRIES, e
                        );
                        return Err(SyncError::Connection(format!(
                            "Failed to run migrations after {} attempts: {}",
                            MAX_RETRIES, e
                        )));
                    }
                }
            }
        }

        // Initialize Redis publisher if URL provided
        let redis_publisher = if let Some(url) = redis_url {
            match RedisPublisher::new(url, analytics_stream_name).await {
                Ok(publisher) => Some(Mutex::new(publisher)),
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
            metrics,
        })
    }

    pub async fn sync_event(&self, event: &RindexerEvent) -> Result<()> {
        let event_start = std::time::Instant::now();
        let event_type = event.event_name.as_str();

        debug!("Starting sync for event: {}", event.event_name);
        debug!("Event signature hash: {}", event.event_signature_hash);
        debug!("Event network: {}", event.network);
        debug!(
            "Full event data: {}",
            serde_json::to_string_pretty(&event.event_data)
                .unwrap_or_else(|_| "Failed to serialize event_data for logging".to_string())
        );

        // Extract transaction information
        let tx_info = self.extract_transaction_info(event)?;
        debug!(
            "Extracted transaction info: {}",
            serde_json::to_string_pretty(&tx_info)
                .unwrap_or_else(|_| "Failed to serialize tx_info for logging".to_string())
        );

        // TODO: Refactor to use a single transaction for both event insert and cascade updates
        // Currently these use separate transactions which could lead to inconsistent state if
        // cascade fails after event is committed. This requires refactoring all event handlers
        // to accept a Transaction<Postgres> instead of &PgPool. Tracked for future PR.

        // Process the event using the appropriate handler which will create the DB entry
        // This will trigger the database triggers that update base tables (atom, triple, position, vault)
        let event_handler_result = event_handlers::process_event(
            &self.pool,
            &event.event_name,
            &event.event_data,
            &tx_info,
        )
        .await;

        if let Err(e) = event_handler_result {
            error!("Failed to process event '{}': {}", event.event_name, e);
            error!(
                "Event data that failed: {}",
                serde_json::to_string_pretty(&event.event_data)
                    .unwrap_or_else(|_| "Failed to serialize for logging".to_string())
            );

            // Record failure metrics
            self.metrics.record_event_by_type_failure(event_type);
            self.metrics
                .record_event_processing_duration(event_type, event_start.elapsed());

            return Err(e);
        }

        // After event insert and triggers, run cascade updates in a separate transaction
        // to update aggregated tables (vault, term)
        let cascade_start = std::time::Instant::now();
        let term_ids = self.run_cascade_after_event(event).await.map_err(|e| {
            // Record cascade failure - this is a specific edge case where event was already
            // committed but cascade failed. This is tracked separately due to the TODO about
            // transaction consistency at lines 105-108.
            self.metrics.record_cascade_failure(event_type);
            self.metrics.record_event_by_type_failure(event_type);
            self.metrics
                .record_event_processing_duration(event_type, event_start.elapsed());
            e
        })?;

        // Record cascade processing duration
        // NOTE: This measures only the cascade update queries (vault/term aggregations).
        // Redis publishing operations below are NOT included in this metric, as they
        // occur after cascade completion. This keeps cascade_duration focused on
        // database performance monitoring.
        self.metrics
            .record_cascade_duration(event_type, cascade_start.elapsed());

        // After successful commit, publish to Redis for analytics worker
        if let Some(publisher_mutex) = &self.redis_publisher {
            // Collect all data needed for publishing BEFORE acquiring the lock
            // Use batched transactions for counter_term_id lookups to prevent long-running
            // transactions that could cause lock contention on high term_id counts
            const MAX_BATCH_SIZE: usize = 50;
            let term_updater = TermUpdater::new();
            let mut term_data = Vec::new();

            // Process term_ids in chunks to avoid long-running transactions
            for chunk in term_ids.chunks(MAX_BATCH_SIZE) {
                let mut tx = self.pool.begin().await.map_err(|e| SyncError::Sqlx(e))?;

                for term_id in chunk {
                    // Get counter_term_id for triples using the shared transaction
                    let counter_term_id =
                        term_updater.get_counter_term_id(&mut tx, term_id).await?;
                    term_data.push((term_id.clone(), counter_term_id));
                }

                // Commit after each chunk
                tx.commit().await.map_err(|e| SyncError::Sqlx(e))?;
            }

            // Now acquire the lock and publish all messages quickly
            let mut publisher = publisher_mutex.lock().await;
            for (term_id, counter_term_id) in term_data {
                if let Err(e) = publisher
                    .publish_term_update(&term_id, counter_term_id.as_deref())
                    .await
                {
                    warn!("Failed to publish term update to Redis: {}", e);
                    // Don't fail the whole operation if Redis publish fails
                }
            }
        }

        debug!("Successfully synced event: {}", event.event_name);

        // Record successful event processing with metrics
        self.metrics.record_event_by_type_success(event_type);
        self.metrics
            .record_event_processing_duration(event_type, event_start.elapsed());

        Ok(())
    }

    /// Run cascade updates after event processing
    /// This updates aggregated tables (vault, term) based on triggered base table updates
    async fn run_cascade_after_event(&self, event: &RindexerEvent) -> Result<Vec<String>> {
        let mut tx = self.pool.begin().await.map_err(|e| SyncError::Sqlx(e))?;

        let term_ids = match event.event_name.as_str() {
            "Deposited" => {
                // Extract deposit event data and format IDs
                let receiver = event
                    .event_data
                    .get("receiver")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| SyncError::Processing("Missing receiver".to_string()))?;
                let term_id = event
                    .event_data
                    .get("termId")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| SyncError::Processing("Missing termId".to_string()))?;
                let curve_id = event
                    .event_data
                    .get("curveId")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| SyncError::Processing("Missing curveId".to_string()))?;

                // Format IDs to match database format
                let term_id_formatted = ensure_hex_prefix(term_id);
                let curve_id_formatted = curve_id.to_string(); // Keep curve_id as-is (without 0x prefix)
                let receiver_formatted = to_eip55_address(receiver)?;

                self.cascade_processor
                    .process_position_change(
                        &mut tx,
                        &receiver_formatted,
                        &term_id_formatted,
                        &curve_id_formatted,
                    )
                    .await?;
                self.metrics
                    .record_database_operation("Deposited", "position_update");
                self.metrics
                    .record_database_operation("Deposited", "vault_aggregation");
                self.metrics
                    .record_database_operation("Deposited", "term_aggregation");
                vec![term_id_formatted]
            }
            "Redeemed" => {
                // Extract redeem event data and format IDs
                let receiver = event
                    .event_data
                    .get("receiver")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| SyncError::Processing("Missing receiver".to_string()))?;
                let term_id = event
                    .event_data
                    .get("termId")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| SyncError::Processing("Missing termId".to_string()))?;
                let curve_id = event
                    .event_data
                    .get("curveId")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| SyncError::Processing("Missing curveId".to_string()))?;

                // Format IDs to match database format
                let term_id_formatted = ensure_hex_prefix(term_id);
                let curve_id_formatted = curve_id.to_string(); // Keep curve_id as-is (without 0x prefix)
                let receiver_formatted = to_eip55_address(receiver)?;

                self.cascade_processor
                    .process_position_change(
                        &mut tx,
                        &receiver_formatted,
                        &term_id_formatted,
                        &curve_id_formatted,
                    )
                    .await?;
                self.metrics
                    .record_database_operation("Redeemed", "position_update");
                self.metrics
                    .record_database_operation("Redeemed", "vault_aggregation");
                self.metrics
                    .record_database_operation("Redeemed", "term_aggregation");
                vec![term_id_formatted]
            }
            "SharePriceChanged" => {
                // Extract price change event data and format IDs
                let term_id = event
                    .event_data
                    .get("termId")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| SyncError::Processing("Missing termId".to_string()))?;
                let curve_id = event
                    .event_data
                    .get("curveId")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| SyncError::Processing("Missing curveId".to_string()))?;

                // Format IDs to match database format
                let term_id_formatted = ensure_hex_prefix(term_id);
                let curve_id_formatted = curve_id.to_string(); // Keep curve_id as-is (without 0x prefix)

                self.cascade_processor
                    .process_price_change(&mut tx, &term_id_formatted, &curve_id_formatted)
                    .await?;
                self.metrics
                    .record_database_operation("SharePriceChanged", "vault_update");
                self.metrics
                    .record_database_operation("SharePriceChanged", "term_aggregation");
                vec![term_id_formatted]
            }
            "AtomCreated" => {
                // Extract atom creation data and format IDs
                let term_id = event
                    .event_data
                    .get("termId")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| SyncError::Processing("Missing termId".to_string()))?;

                // Format IDs to match database format
                let term_id_formatted = ensure_hex_prefix(term_id);

                self.cascade_processor
                    .process_atom_creation(&mut tx, &term_id_formatted)
                    .await?;
                self.metrics
                    .record_database_operation("AtomCreated", "term_initialization");
                vec![term_id_formatted]
            }
            "TripleCreated" => {
                // Extract triple creation data and calculate counter_term_id
                let term_id = event
                    .event_data
                    .get("termId")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| SyncError::Processing("Missing termId".to_string()))?;

                // Format IDs to match database format
                let term_id_formatted = ensure_hex_prefix(term_id);

                // Calculate counter_term_id using the same logic as the event handler
                let counter_term_id_formatted =
                    super::utils::calculate_counter_term_id(&term_id_formatted)?;

                self.cascade_processor
                    .process_triple_creation(
                        &mut tx,
                        &term_id_formatted,
                        &counter_term_id_formatted,
                    )
                    .await?;
                self.metrics
                    .record_database_operation("TripleCreated", "term_initialization");
                vec![term_id_formatted, counter_term_id_formatted]
            }
            _ => {
                debug!("No cascade processing for event type: {}", event.event_name);
                vec![]
            }
        };

        // Commit the transaction
        tx.commit().await.map_err(|e| SyncError::Sqlx(e))?;

        Ok(term_ids)
    }

    /// Get a reference to the database connection pool for custom operations
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    fn extract_transaction_info(&self, event: &RindexerEvent) -> Result<TransactionInformation> {
        let tx_info_value = event
            .event_data
            .get("transaction_information")
            .ok_or_else(|| {
                SyncError::Processing("Missing transaction_information in event".to_string())
            })?;

        serde_json::from_value(tx_info_value.clone()).map_err(SyncError::Serde)
    }
}
