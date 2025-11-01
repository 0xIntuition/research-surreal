use anyhow::Result;
use redis::aio::MultiplexedConnection;
use redis_postgres_sync::core::types::RindexerEvent;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::sync::Arc;
use testcontainers::{clients::Cli, Container, Image};
use testcontainers_modules::{postgres::Postgres, redis::Redis};

pub struct TestHarness {
    _docker: Arc<Cli>,
    _redis_container: Container<'static, Redis>,
    _postgres_container: Container<'static, Postgres>,
    redis_url: String,
    database_url: String,
    test_db_name: String,
}

impl TestHarness {
    /// Creates a new test environment with isolated containers
    pub async fn new() -> Result<Self> {
        let docker = Arc::new(Cli::default());

        // Start Redis container
        let redis_container = docker.run(Redis::default());
        let redis_port = redis_container.get_host_port_ipv4(6379);
        let redis_url = format!("redis://127.0.0.1:{}", redis_port);

        // Start PostgreSQL container
        let postgres_container = docker.run(
            Postgres::default()
                .with_db_name("test")
                .with_user("test")
                .with_password("test"),
        );
        let postgres_port = postgres_container.get_host_port_ipv4(5432);

        // Create unique database for this test
        let test_db_name = format!("test_{}", uuid::Uuid::new_v4().simple());
        let database_url = format!(
            "postgres://test:test@127.0.0.1:{}/{}",
            postgres_port, test_db_name
        );

        let harness = Self {
            _docker: docker,
            _redis_container: redis_container,
            _postgres_container: postgres_container,
            redis_url,
            database_url: database_url.clone(),
            test_db_name: test_db_name.clone(),
        };

        // Create test database and run migrations
        harness.setup_database().await?;

        Ok(harness)
    }

    /// Sets up database schema and runs migrations
    async fn setup_database(&self) -> Result<()> {
        // Connect to default postgres database
        let admin_url = self.database_url.replace(&self.test_db_name, "test");
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await?;

        // Create test database
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
    pub async fn clear_data(&self) -> Result<()> {
        // Clear Redis streams
        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut con = client.get_multiplexed_async_connection().await?;

        // Delete all streams
        redis::cmd("FLUSHDB").query_async(&mut con).await?;

        // Clear PostgreSQL tables in dependency order
        let pool = self.get_pool().await?;

        // Analytics tables first
        sqlx::query("TRUNCATE TABLE predicate_object CASCADE")
            .execute(&pool)
            .await?;
        sqlx::query("TRUNCATE TABLE subject_predicate CASCADE")
            .execute(&pool)
            .await?;
        sqlx::query("TRUNCATE TABLE triple_term CASCADE")
            .execute(&pool)
            .await?;
        sqlx::query("TRUNCATE TABLE triple_vault CASCADE")
            .execute(&pool)
            .await?;

        // Aggregated tables
        sqlx::query("TRUNCATE TABLE term CASCADE")
            .execute(&pool)
            .await?;

        // Base tables
        sqlx::query("TRUNCATE TABLE vault CASCADE")
            .execute(&pool)
            .await?;
        sqlx::query("TRUNCATE TABLE position CASCADE")
            .execute(&pool)
            .await?;
        sqlx::query("TRUNCATE TABLE triple CASCADE")
            .execute(&pool)
            .await?;
        sqlx::query("TRUNCATE TABLE atom CASCADE")
            .execute(&pool)
            .await?;

        // Event tables
        sqlx::query("TRUNCATE TABLE share_price_changed_events CASCADE")
            .execute(&pool)
            .await?;
        sqlx::query("TRUNCATE TABLE redeemed_events CASCADE")
            .execute(&pool)
            .await?;
        sqlx::query("TRUNCATE TABLE deposited_events CASCADE")
            .execute(&pool)
            .await?;
        sqlx::query("TRUNCATE TABLE triple_created_events CASCADE")
            .execute(&pool)
            .await?;
        sqlx::query("TRUNCATE TABLE atom_created_events CASCADE")
            .execute(&pool)
            .await?;

        pool.close().await;

        Ok(())
    }

    /// Gets a connection pool for assertions
    pub async fn get_pool(&self) -> Result<PgPool> {
        PgPoolOptions::new()
            .max_connections(5)
            .connect(&self.database_url)
            .await
            .map_err(Into::into)
    }

    /// Gets Redis connection
    pub async fn get_redis_connection(&self) -> Result<MultiplexedConnection> {
        let client = redis::Client::open(self.redis_url.as_str())?;
        client
            .get_multiplexed_async_connection()
            .await
            .map_err(Into::into)
    }

    /// Publishes events to Redis stream
    pub async fn publish_events(&self, stream: &str, events: Vec<RindexerEvent>) -> Result<()> {
        let mut con = self.get_redis_connection().await?;

        for event in events {
            let json = serde_json::to_string(&event)?;
            redis::cmd("XADD")
                .arg(stream)
                .arg("*") // Auto-generate ID
                .arg("data")
                .arg(json)
                .query_async(&mut con)
                .await?;
        }

        Ok(())
    }

    /// Waits for events to be processed
    pub async fn wait_for_processing(&self, expected_count: usize, timeout_secs: u64) -> Result<()> {
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
            .fetch_one(&pool)
            .await?;

            if count as usize >= expected_count {
                break;
            }

            if start.elapsed().as_secs() > timeout_secs {
                pool.close().await;
                return Err(anyhow::anyhow!(
                    "Timeout waiting for events. Expected: {}, Got: {}",
                    expected_count,
                    count
                ));
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        pool.close().await;
        Ok(())
    }

    /// Waits for analytics to be processed (separate from event processing)
    pub async fn wait_for_analytics(
        &self,
        term_id: &str,
        counter_term_id: Option<&str>,
        timeout_secs: u64,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        let pool = self.get_pool().await?;

        loop {
            let exists: bool = if let Some(counter) = counter_term_id {
                sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM triple_term WHERE term_id = $1 AND counter_term_id = $2)",
                )
                .bind(term_id)
                .bind(counter)
                .fetch_one(&pool)
                .await?
            } else {
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM term WHERE id = $1)")
                    .bind(term_id)
                    .fetch_one(&pool)
                    .await?
            };

            if exists {
                break;
            }

            if start.elapsed().as_secs() > timeout_secs {
                pool.close().await;
                return Err(anyhow::anyhow!(
                    "Timeout waiting for analytics for term: {}",
                    term_id
                ));
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        pool.close().await;
        Ok(())
    }

    pub fn redis_url(&self) -> &str {
        &self.redis_url
    }

    pub fn database_url(&self) -> &str {
        &self.database_url
    }
}

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
        let harness = TestHarness::new().await.unwrap();
        harness.clear_data().await.unwrap();
    }
}
