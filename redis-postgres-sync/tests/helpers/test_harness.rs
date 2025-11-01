use anyhow::Result;
use redis::aio::MultiplexedConnection;
use redis_postgres_sync::core::types::RindexerEvent;
use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::{postgres::Postgres, redis::Redis};

pub struct TestHarness {
    _redis_container: ContainerAsync<Redis>,
    _postgres_container: ContainerAsync<Postgres>,
    redis_url: String,
    database_url: String,
    test_db_name: String,
    pool: Option<PgPool>,
}

impl TestHarness {
    /// Creates a new test environment with isolated containers
    pub async fn new() -> Result<Self> {
        // Start Redis container
        let redis_container = Redis::default().start().await?;
        let redis_port = redis_container.get_host_port_ipv4(6379).await?;
        let redis_url = format!("redis://127.0.0.1:{}", redis_port);

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
        let database_url = format!(
            "postgres://test:test@127.0.0.1:{}/{}",
            postgres_port, test_db_name
        );

        let harness = Self {
            _redis_container: redis_container,
            _postgres_container: postgres_container,
            redis_url,
            database_url: database_url.clone(),
            test_db_name: test_db_name.clone(),
            pool: None,
        };

        // Create test database and run migrations
        harness.setup_database().await?;

        Ok(harness)
    }

    /// Sets up database schema and runs migrations
    async fn setup_database(&self) -> Result<()> {
        // Validate database name to prevent SQL injection
        // UUIDs are safe, but we validate format anyway for defense in depth
        if !self.test_db_name.starts_with("test_") ||
           !self.test_db_name[5..].chars().all(|c| c.is_ascii_alphanumeric()) {
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

    /// Clears all data from Redis and PostgreSQL
    pub async fn clear_data(&mut self) -> Result<()> {
        // Clear Redis streams
        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut con = client.get_multiplexed_async_connection().await?;

        // Delete all streams
        redis::cmd("FLUSHDB").query_async::<()>(&mut con).await?;

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
            CASCADE"
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

    /// Gets Redis connection
    pub async fn get_redis_connection(&self) -> Result<MultiplexedConnection> {
        let client = redis::Client::open(self.redis_url.as_str())?;
        client
            .get_multiplexed_async_connection()
            .await
            .map_err(Into::into)
    }

    /// Publishes events to Redis stream using pipelining for better performance
    pub async fn publish_events(&self, stream: &str, events: Vec<RindexerEvent>) -> Result<()> {
        let mut con = self.get_redis_connection().await?;

        // Use Redis pipeline to batch all XADD commands into a single round trip
        // This is much faster than sending each command individually
        let mut pipe = redis::pipe();

        for event in events {
            let json = serde_json::to_string(&event)?;
            pipe.cmd("XADD")
                .arg(stream)
                .arg("*") // Auto-generate ID
                .arg("data")
                .arg(json);
        }

        // Execute all commands at once
        pipe.query_async::<()>(&mut con).await?;

        Ok(())
    }

    /// Waits for events to be processed
    pub async fn wait_for_processing(&mut self, expected_count: usize, timeout_secs: u64) -> Result<()> {
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
                    "Timeout waiting for events. Expected: {}, Got: {}",
                    expected_count,
                    count
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
            let exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM term WHERE id = $1)"
            )
            .bind(term_id)
            .fetch_one(pool)
            .await?;

            if exists {
                // Additional check: ensure the term has non-zero updated_at timestamp
                let is_updated: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM term WHERE id = $1 AND updated_at IS NOT NULL)"
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
                    "Timeout waiting for cascade processing for term: {}",
                    term_id
                ));
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        Ok(())
    }

    pub fn redis_url(&self) -> &str {
        &self.redis_url
    }

    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    /// Creates a default Config for testing
    pub fn default_config(&self) -> redis_postgres_sync::config::Config {
        redis_postgres_sync::config::Config {
            redis_url: self.redis_url().to_string(),
            database_url: self.database_url().to_string(),
            stream_names: vec!["rindexer_producer".to_string()],
            consumer_group: "test-group".to_string(),
            consumer_name: "test-consumer".to_string(),
            batch_size: 10,
            batch_timeout_ms: 1000,
            workers: 1,
            processing_timeout_ms: 5000,
            max_retries: 3,
            circuit_breaker_threshold: 10,
            circuit_breaker_timeout_ms: 60000,
            http_port: 0,
            consumer_group_suffix: None,
            analytics_stream_name: "term_updates".to_string(),
        }
    }

    /// Creates a Config with specified number of workers
    pub fn config_with_workers(&self, workers: usize) -> redis_postgres_sync::config::Config {
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
        assert!(!harness.redis_url().is_empty());
        assert!(!harness.database_url().is_empty());
    }

    #[tokio::test]
    async fn test_clear_data() {
        let mut harness = TestHarness::new().await.unwrap();
        harness.clear_data().await.unwrap();
    }
}
