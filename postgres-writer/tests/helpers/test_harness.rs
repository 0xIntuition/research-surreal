use anyhow::Result;
use lapin::{
    options::{
        BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions,
        QueuePurgeOptions,
    },
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties, ExchangeKind,
};
use postgres_writer::core::types::RindexerEvent;
use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::{postgres::Postgres, rabbitmq::RabbitMq};

pub struct TestHarness {
    _rabbitmq_container: ContainerAsync<RabbitMq>,
    _postgres_container: ContainerAsync<Postgres>,
    rabbitmq_url: String,
    database_url: String,
    test_db_name: String,
    pool: Option<PgPool>,
    exchanges: Vec<String>,
    queue_prefix: String,
}

impl TestHarness {
    /// Creates a new test environment with isolated containers
    pub async fn new() -> Result<Self> {
        // Start RabbitMQ container
        let rabbitmq_container = RabbitMq::default().start().await?;
        let rabbitmq_port = rabbitmq_container.get_host_port_ipv4(5672).await?;
        let rabbitmq_url = format!("amqp://guest:guest@127.0.0.1:{rabbitmq_port}");

        // Start PostgreSQL container
        let postgres_container = Postgres::default()
            .with_db_name("test")
            .with_user("test")
            .with_password("test")
            .start()
            .await?;
        let postgres_port = postgres_container.get_host_port_ipv4(5432).await?;

        // Create unique database for this test
        let test_db_name = format!("test_{}", uuid::Uuid::new_v4().simple());
        let database_url = format!("postgres://test:test@127.0.0.1:{postgres_port}/{test_db_name}");

        let exchanges = vec![
            "atom_created".to_string(),
            "triple_created".to_string(),
            "deposited".to_string(),
            "redeemed".to_string(),
            "share_price_changed".to_string(),
            "term_updates".to_string(),
        ];
        let queue_prefix = "test".to_string();

        let harness = Self {
            _rabbitmq_container: rabbitmq_container,
            _postgres_container: postgres_container,
            rabbitmq_url,
            database_url: database_url.clone(),
            test_db_name: test_db_name.clone(),
            pool: None,
            exchanges,
            queue_prefix,
        };

        // Create test database and run migrations
        harness.setup_database().await?;

        // Setup RabbitMQ exchanges and queues
        harness.setup_rabbitmq().await?;

        Ok(harness)
    }

    /// Sets up database schema and runs migrations
    async fn setup_database(&self) -> Result<()> {
        // Validate database name to prevent SQL injection
        // UUIDs are safe, but we validate format anyway for defense in depth
        if !self.test_db_name.starts_with("test_")
            || !self.test_db_name[5..]
                .chars()
                .all(|c| c.is_ascii_alphanumeric())
        {
            return Err(anyhow::anyhow!(
                "Invalid database name format: {}. Expected 'test_' followed by alphanumeric characters",
                self.test_db_name
            ));
        }

        // Connect to default postgres database
        let admin_url = self.database_url.replace(&self.test_db_name, "test");
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await?;

        // Create test database (now safe after validation)
        sqlx::query(&format!("CREATE DATABASE \"{}\"", self.test_db_name))
            .execute(&admin_pool)
            .await?;

        admin_pool.close().await;

        // Run migrations
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&self.database_url)
            .await?;

        sqlx::migrate!("./migrations").run(&pool).await?;

        pool.close().await;

        Ok(())
    }

    /// Sets up RabbitMQ exchanges and queues
    async fn setup_rabbitmq(&self) -> Result<()> {
        let connection =
            Connection::connect(&self.rabbitmq_url, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;

        // Declare exchanges
        for exchange in &self.exchanges {
            channel
                .exchange_declare(
                    exchange,
                    ExchangeKind::Direct,
                    ExchangeDeclareOptions {
                        durable: true,
                        auto_delete: false,
                        internal: false,
                        nowait: false,
                        passive: false,
                    },
                    FieldTable::default(),
                )
                .await?;
        }

        // Declare queues and bind them to exchanges
        for exchange in &self.exchanges {
            let queue_name = format!("{}.{}", self.queue_prefix, exchange);
            let routing_key = format!("intuition.{}", exchange);

            channel
                .queue_declare(
                    &queue_name,
                    QueueDeclareOptions {
                        durable: true,
                        exclusive: false,
                        auto_delete: false,
                        nowait: false,
                        passive: false,
                    },
                    FieldTable::default(),
                )
                .await?;

            channel
                .queue_bind(
                    &queue_name,
                    exchange,
                    &routing_key,
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await?;
        }

        connection.close(0, "Setup complete").await?;
        Ok(())
    }

    /// Clears all data from RabbitMQ queues and PostgreSQL
    pub async fn clear_data(&mut self) -> Result<()> {
        // Clear RabbitMQ queues
        let connection =
            Connection::connect(&self.rabbitmq_url, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;

        for exchange in &self.exchanges {
            let queue_name = format!("{}.{}", self.queue_prefix, exchange);
            channel
                .queue_purge(&queue_name, QueuePurgeOptions::default())
                .await?;
        }

        connection.close(0, "Clear complete").await?;

        // Clear PostgreSQL tables efficiently in a single transaction
        // Using CASCADE handles all foreign key constraints automatically
        let pool = self.get_pool().await?;

        // Use a single transaction to truncate all tables efficiently
        // Start with base tables and CASCADE will handle dependent tables
        sqlx::query(
            "TRUNCATE TABLE
                atom,
                triple,
                position,
                vault,
                term,
                triple_vault,
                triple_term,
                subject_predicate,
                predicate_object,
                atom_created_events,
                triple_created_events,
                deposited_events,
                redeemed_events,
                share_price_changed_events
            CASCADE",
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    /// Gets a connection pool for assertions (cached for reuse)
    pub async fn get_pool(&mut self) -> Result<&PgPool> {
        if self.pool.is_none() {
            let pool = PgPoolOptions::new()
                .max_connections(5)
                .connect(&self.database_url)
                .await?;
            self.pool = Some(pool);
        }
        Ok(self.pool.as_ref().unwrap())
    }

    /// Publishes events to RabbitMQ exchange
    /// If exchange is "rindexer_producer", events are automatically routed to the correct exchange based on event_name
    pub async fn publish_events(&self, exchange: &str, events: Vec<RindexerEvent>) -> Result<()> {
        let connection =
            Connection::connect(&self.rabbitmq_url, ConnectionProperties::default()).await?;
        let channel = connection.create_channel().await?;

        for event in events {
            // Route events to the correct exchange based on event_name
            let target_exchange = if exchange == "rindexer_producer" {
                // Auto-route based on event name
                match event.event_name.as_str() {
                    "AtomCreated" => "atom_created",
                    "TripleCreated" => "triple_created",
                    "Deposited" => "deposited",
                    "Redeemed" => "redeemed",
                    "SharePriceChanged" => "share_price_changed",
                    _ => return Err(anyhow::anyhow!("Unknown event type: {}", event.event_name)),
                }
            } else {
                exchange
            };

            let routing_key = format!("intuition.{}", target_exchange);
            let payload = serde_json::to_vec(&event)?;

            channel
                .basic_publish(
                    target_exchange,
                    &routing_key,
                    BasicPublishOptions::default(),
                    &payload,
                    BasicProperties::default(),
                )
                .await?
                .await?; // Wait for confirmation
        }

        connection.close(0, "Publish complete").await?;
        Ok(())
    }

    /// Waits for events to be processed
    pub async fn wait_for_processing(
        &mut self,
        expected_count: usize,
        timeout_secs: u64,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        let pool = self.get_pool().await?;

        loop {
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM (
                    SELECT 1 FROM atom_created_events
                    UNION ALL SELECT 1 FROM triple_created_events
                    UNION ALL SELECT 1 FROM deposited_events
                    UNION ALL SELECT 1 FROM redeemed_events
                    UNION ALL SELECT 1 FROM share_price_changed_events
                ) AS all_events",
            )
            .fetch_one(pool)
            .await?;

            if count as usize >= expected_count {
                break;
            }

            if start.elapsed().as_secs() > timeout_secs {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for events. Expected: {expected_count}, Got: {count}"
                ));
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        Ok(())
    }

    /// Waits for cascade processing to complete by checking that all vault aggregations are updated
    /// This is more reliable than fixed sleep durations
    pub async fn wait_for_cascade(&mut self, term_id: &str, timeout_secs: u64) -> Result<()> {
        let start = std::time::Instant::now();
        let pool = self.get_pool().await?;

        loop {
            // Check if term aggregation has been updated (indicates cascade processing is complete)
            let exists: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM term WHERE id = $1)")
                    .bind(term_id)
                    .fetch_one(pool)
                    .await?;

            if exists {
                // Additional check: ensure the term has non-zero updated_at timestamp
                let is_updated: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM term WHERE id = $1 AND updated_at IS NOT NULL)",
                )
                .bind(term_id)
                .fetch_one(pool)
                .await?;

                if is_updated {
                    break;
                }
            }

            if start.elapsed().as_secs() > timeout_secs {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for cascade processing for term: {term_id}"
                ));
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        Ok(())
    }

    pub fn rabbitmq_url(&self) -> &str {
        &self.rabbitmq_url
    }

    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    /// Creates a default Config for testing
    pub fn default_config(&self) -> postgres_writer::config::Config {
        postgres_writer::config::Config {
            rabbitmq_url: self.rabbitmq_url().to_string(),
            database_url: self.database_url().to_string(),
            database_pool_size: 10,
            exchanges: self.exchanges.clone(),
            queue_prefix: self.queue_prefix.clone(),
            prefetch_count: 20,
            batch_size: 10,
            batch_timeout_ms: 1000,
            workers: 1,
            processing_timeout_ms: 5000,
            max_retries: 3,
            circuit_breaker_threshold: 10,
            circuit_breaker_timeout_ms: 60000,
            http_port: 0,
            shutdown_timeout_secs: 30,
        }
    }

    /// Creates a Config with specified number of workers
    pub fn config_with_workers(&self, workers: usize) -> postgres_writer::config::Config {
        let mut config = self.default_config();
        config.workers = workers;
        config
    }
}

// Drop implementation removed - PgPool closes automatically when dropped
// The previous implementation was causing deadlocks by creating a new runtime
// inside an existing tokio runtime context

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_harness_creation() {
        let harness = TestHarness::new().await.unwrap();
        assert!(!harness.rabbitmq_url().is_empty());
        assert!(!harness.database_url().is_empty());
    }

    #[tokio::test]
    async fn test_clear_data() {
        let mut harness = TestHarness::new().await.unwrap();
        harness.clear_data().await.unwrap();
    }
}
