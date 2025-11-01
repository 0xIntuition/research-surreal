# Testing Proposal for redis-postgres-sync

## Overview

This document proposes a comprehensive integration testing solution for the redis-postgres-sync component that handles non-sequential blockchain events with database triggers and Rust cascade processors.

## Goals

1. **Full Integration Testing**: Test the complete pipeline from Redis stream ingestion to final database state
2. **Mock Data Support**: Generate realistic blockchain events in non-sequential order
3. **Isolated Test Environment**: Each test runs with clean Redis and PostgreSQL state
4. **Test Observability**: Clear assertions on database state after event processing
5. **Performance Testing**: Validate handling of large batches and concurrent events

---

## Architecture

### Test Stack

```
┌─────────────────────────────────────────────────────┐
│  Integration Tests (tests/integration/)             │
├─────────────────────────────────────────────────────┤
│  Test Fixtures & Helpers (tests/helpers/)           │
├─────────────────────────────────────────────────────┤
│  redis-postgres-sync Application                    │
├──────────────────┬──────────────────────────────────┤
│  Testcontainers  │  Testcontainers                  │
│  Redis           │  PostgreSQL                      │
└──────────────────┴──────────────────────────────────┘
```

### Key Dependencies

```toml
[dev-dependencies]
# Test containers for isolated environments
testcontainers = "0.21"
testcontainers-modules = { version = "0.9", features = ["redis", "postgres"] }

# Test utilities
tokio-test = "0.4"
serial_test = "3.0"  # For tests that must run sequentially
rstest = "0.22"      # Parameterized testing
fake = "2.9"         # Mock data generation

# Assertions
assert2 = "0.3"      # Better assertions
similar-asserts = "1.5"  # Diff assertions for complex types
```

---

## Test Structure

### Directory Layout

```
redis-postgres-sync/
├── tests/
│   ├── integration/
│   │   ├── mod.rs
│   │   ├── atom_lifecycle.rs          # Atom creation and updates
│   │   ├── triple_lifecycle.rs        # Triple creation and updates
│   │   ├── position_lifecycle.rs      # Deposit/redeem flows
│   │   ├── vault_updates.rs           # Share price and vault state
│   │   ├── out_of_order.rs            # Non-sequential event handling
│   │   ├── concurrent_updates.rs      # Race condition testing
│   │   ├── cascade_processing.rs      # Aggregation validation
│   │   ├── analytics_pipeline.rs      # Analytics worker validation
│   │   └── error_scenarios.rs         # Failure modes
│   ├── helpers/
│   │   ├── mod.rs
│   │   ├── test_harness.rs            # Main test setup
│   │   ├── mock_events.rs             # Event generators
│   │   ├── db_assertions.rs           # Database state assertions
│   │   ├── redis_helpers.rs           # Redis stream utilities
│   │   └── fixtures.rs                # Common test data
│   └── test_main.rs
```

---

## Core Testing Components

### 1. Test Harness

The test harness manages the lifecycle of test infrastructure:

```rust
// tests/helpers/test_harness.rs

use testcontainers::{clients::Cli, Container, RunnableImage};
use testcontainers_modules::{postgres::Postgres, redis::Redis};

pub struct TestHarness {
    docker: Cli,
    redis_container: Container<'static, Redis>,
    postgres_container: Container<'static, Postgres>,
    redis_url: String,
    database_url: String,
    test_db_name: String,
}

impl TestHarness {
    /// Creates a new test environment with isolated containers
    pub async fn new() -> Result<Self> {
        let docker = Cli::default();

        // Start Redis container
        let redis_image = RunnableImage::from(Redis::default());
        let redis_container = docker.run(redis_image);
        let redis_port = redis_container.get_host_port_ipv4(6379);
        let redis_url = format!("redis://127.0.0.1:{}", redis_port);

        // Start PostgreSQL container
        let postgres_image = RunnableImage::from(
            Postgres::default()
                .with_db_name("test")
                .with_user("test")
                .with_password("test")
        );
        let postgres_container = docker.run(postgres_image);
        let postgres_port = postgres_container.get_host_port_ipv4(5432);

        // Create unique database for this test
        let test_db_name = format!("test_{}", uuid::Uuid::new_v4().to_string().replace('-', ""));
        let database_url = format!(
            "postgres://test:test@127.0.0.1:{}/{}",
            postgres_port, test_db_name
        );

        let harness = Self {
            docker,
            redis_container,
            postgres_container,
            redis_url,
            database_url: database_url.clone(),
            test_db_name: test_db_name.clone(),
        };

        // Create test database
        harness.setup_database().await?;

        Ok(harness)
    }

    /// Sets up database schema and runs migrations
    async fn setup_database(&self) -> Result<()> {
        // Connect to default postgres database
        let admin_url = self.database_url.replace(&self.test_db_name, "postgres");
        let admin_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await?;

        // Create test database
        sqlx::query(&format!("CREATE DATABASE {}", self.test_db_name))
            .execute(&admin_pool)
            .await?;

        admin_pool.close().await;

        // Run migrations
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&self.database_url)
            .await?;

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await?;

        pool.close().await;

        Ok(())
    }

    /// Clears all data from Redis and PostgreSQL
    pub async fn clear_data(&self) -> Result<()> {
        // Clear Redis streams
        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut con = client.get_connection()?;

        // Delete all streams
        redis::cmd("FLUSHDB").query(&mut con)?;

        // Clear PostgreSQL tables in dependency order
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&self.database_url)
            .await?;

        // Analytics tables first
        sqlx::query("TRUNCATE TABLE predicate_object CASCADE").execute(&pool).await?;
        sqlx::query("TRUNCATE TABLE subject_predicate CASCADE").execute(&pool).await?;
        sqlx::query("TRUNCATE TABLE triple_term CASCADE").execute(&pool).await?;
        sqlx::query("TRUNCATE TABLE triple_vault CASCADE").execute(&pool).await?;

        // Aggregated tables
        sqlx::query("TRUNCATE TABLE term CASCADE").execute(&pool).await?;

        // Base tables
        sqlx::query("TRUNCATE TABLE vault CASCADE").execute(&pool).await?;
        sqlx::query("TRUNCATE TABLE position CASCADE").execute(&pool).await?;
        sqlx::query("TRUNCATE TABLE triple CASCADE").execute(&pool).await?;
        sqlx::query("TRUNCATE TABLE atom CASCADE").execute(&pool).await?;

        // Event tables
        sqlx::query("TRUNCATE TABLE share_price_changed_events CASCADE").execute(&pool).await?;
        sqlx::query("TRUNCATE TABLE redeemed_events CASCADE").execute(&pool).await?;
        sqlx::query("TRUNCATE TABLE deposited_events CASCADE").execute(&pool).await?;
        sqlx::query("TRUNCATE TABLE triple_created_events CASCADE").execute(&pool).await?;
        sqlx::query("TRUNCATE TABLE atom_created_events CASCADE").execute(&pool).await?;

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
    pub fn get_redis_connection(&self) -> Result<redis::Connection> {
        let client = redis::Client::open(self.redis_url.as_str())?;
        client.get_connection().map_err(Into::into)
    }

    /// Publishes events to Redis stream
    pub async fn publish_events(&self, stream: &str, events: Vec<RindexerEvent>) -> Result<()> {
        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut con = client.get_async_connection().await?;

        for event in events {
            let json = serde_json::to_string(&event)?;
            redis::cmd("XADD")
                .arg(stream)
                .arg("*")  // Auto-generate ID
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
                ) AS all_events"
            )
            .fetch_one(&pool)
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

impl Drop for TestHarness {
    fn drop(&mut self) {
        // Containers are automatically cleaned up by testcontainers
    }
}
```

### 2. Mock Event Generators

```rust
// tests/helpers/mock_events.rs

use fake::{Fake, Faker};
use chrono::Utc;

pub struct EventBuilder {
    block_number: u64,
    log_index: u64,
    transaction_hash: String,
    network: String,
}

impl EventBuilder {
    pub fn new() -> Self {
        Self {
            block_number: 1000,
            log_index: 0,
            transaction_hash: format!("0x{}", hex::encode(Faker.fake::<[u8; 32]>())),
            network: "base_sepolia".to_string(),
        }
    }

    pub fn with_block(mut self, block_number: u64) -> Self {
        self.block_number = block_number;
        self
    }

    pub fn with_log_index(mut self, log_index: u64) -> Self {
        self.log_index = log_index;
        self
    }

    /// Creates AtomCreated event
    pub fn atom_created(&self, term_id: &str, creator: &str) -> RindexerEvent {
        let event_data = serde_json::json!({
            "termId": term_id,
            "creator": creator,
            "walletId": format!("0x{}", hex::encode(Faker.fake::<[u8; 20]>())),
            "data": "0x",
        });

        RindexerEvent {
            event_name: "AtomCreated".to_string(),
            event_signature_hash: "0x123...".to_string(),
            event_data,
            network: self.network.clone(),
            transaction_information: TransactionInformation {
                block_number: self.block_number,
                block_hash: format!("0x{}", hex::encode(Faker.fake::<[u8; 32]>())),
                block_timestamp: Utc::now().timestamp() as u64,
                transaction_hash: self.transaction_hash.clone(),
                transaction_index: 0,
                log_index: self.log_index,
                network: self.network.clone(),
            },
        }
    }

    /// Creates TripleCreated event
    pub fn triple_created(
        &self,
        term_id: &str,
        subject_id: &str,
        predicate_id: &str,
        object_id: &str,
    ) -> RindexerEvent {
        let event_data = serde_json::json!({
            "termId": term_id,
            "subjectId": subject_id,
            "predicateId": predicate_id,
            "objectId": object_id,
            "creator": format!("0x{}", hex::encode(Faker.fake::<[u8; 20]>())),
        });

        RindexerEvent {
            event_name: "TripleCreated".to_string(),
            event_signature_hash: "0x456...".to_string(),
            event_data,
            network: self.network.clone(),
            transaction_information: self.transaction_info(),
        }
    }

    /// Creates Deposited event
    pub fn deposited(
        &self,
        account_id: &str,
        term_id: &str,
        assets: u64,
        shares: u64,
    ) -> RindexerEvent {
        let event_data = serde_json::json!({
            "sender": account_id,
            "receiver": account_id,
            "id": term_id,
            "curve": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "assets": assets.to_string(),
            "shares": shares.to_string(),
            "totalAssets": (assets * 2).to_string(),
            "totalShares": (shares * 2).to_string(),
            "totalAssetsAfterTotalFees": assets.to_string(),
        });

        RindexerEvent {
            event_name: "Deposited".to_string(),
            event_signature_hash: "0x789...".to_string(),
            event_data,
            network: self.network.clone(),
            transaction_information: self.transaction_info(),
        }
    }

    /// Creates Redeemed event
    pub fn redeemed(
        &self,
        account_id: &str,
        term_id: &str,
        shares: u64,
        assets: u64,
    ) -> RindexerEvent {
        let event_data = serde_json::json!({
            "sender": account_id,
            "receiver": account_id,
            "owner": account_id,
            "id": term_id,
            "curve": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "assets": assets.to_string(),
            "shares": shares.to_string(),
            "totalAssets": "0",
            "totalShares": "0",
            "assetsForReceiver": assets.to_string(),
        });

        RindexerEvent {
            event_name: "Redeemed".to_string(),
            event_signature_hash: "0xabc...".to_string(),
            event_data,
            network: self.network.clone(),
            transaction_information: self.transaction_info(),
        }
    }

    /// Creates SharePriceChanged event
    pub fn share_price_changed(&self, term_id: &str, new_price: u64) -> RindexerEvent {
        let event_data = serde_json::json!({
            "id": term_id,
            "curve": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "oldSharePrice": "1000000000000000000",
            "newSharePrice": new_price.to_string(),
        });

        RindexerEvent {
            event_name: "SharePriceChanged".to_string(),
            event_signature_hash: "0xdef...".to_string(),
            event_data,
            network: self.network.clone(),
            transaction_information: self.transaction_info(),
        }
    }

    fn transaction_info(&self) -> TransactionInformation {
        TransactionInformation {
            block_number: self.block_number,
            block_hash: format!("0x{}", hex::encode(Faker.fake::<[u8; 32]>())),
            block_timestamp: Utc::now().timestamp() as u64,
            transaction_hash: self.transaction_hash.clone(),
            transaction_index: 0,
            log_index: self.log_index,
            network: self.network.clone(),
        }
    }
}

/// Helper to create non-sequential event sequences
pub struct NonSequentialScenario {
    events: Vec<RindexerEvent>,
}

impl NonSequentialScenario {
    pub fn new() -> Self {
        Self { events: vec![] }
    }

    /// Adds events in intentionally scrambled order
    pub fn add_scrambled_deposits(mut self, account: &str, term_id: &str) -> Self {
        let builder = EventBuilder::new();

        // Add deposits out of order: block 1005, then 1001, then 1003
        self.events.push(
            builder.clone().with_block(1005).with_log_index(0)
                .deposited(account, term_id, 5000, 5000)
        );
        self.events.push(
            builder.clone().with_block(1001).with_log_index(0)
                .deposited(account, term_id, 1000, 1000)
        );
        self.events.push(
            builder.clone().with_block(1003).with_log_index(0)
                .deposited(account, term_id, 3000, 3000)
        );

        self
    }

    pub fn build(self) -> Vec<RindexerEvent> {
        self.events
    }
}
```

### 3. Database Assertions

```rust
// tests/helpers/db_assertions.rs

use sqlx::PgPool;
use serde::Deserialize;

#[derive(Debug, Deserialize, PartialEq)]
pub struct AtomRow {
    pub term_id: String,
    pub creator_id: String,
    pub wallet_id: String,
    pub last_event_block: i64,
    pub last_event_log_index: i64,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct VaultRow {
    pub term_id: String,
    pub curve_id: String,
    pub total_shares: String,
    pub current_share_price: String,
    pub total_assets: String,
    pub market_cap: String,
    pub position_count: i32,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct TermRow {
    pub id: String,
    pub r#type: String,
    pub total_assets: String,
    pub total_market_cap: String,
}

pub struct DbAssertions;

impl DbAssertions {
    /// Asserts that an atom exists with expected values
    pub async fn assert_atom_exists(
        pool: &PgPool,
        term_id: &str,
        creator_id: &str,
    ) -> Result<AtomRow> {
        let row = sqlx::query_as!(
            AtomRow,
            r#"
            SELECT term_id, creator_id, wallet_id,
                   last_event_block, last_event_log_index
            FROM atom
            WHERE term_id = $1
            "#,
            term_id
        )
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Atom not found: {}", term_id))?;

        assert_eq!(row.creator_id, creator_id, "Creator mismatch for atom {}", term_id);

        Ok(row)
    }

    /// Asserts vault state
    pub async fn assert_vault_state(
        pool: &PgPool,
        term_id: &str,
        curve_id: &str,
        expected_position_count: i32,
    ) -> Result<VaultRow> {
        let row = sqlx::query_as!(
            VaultRow,
            r#"
            SELECT term_id, curve_id, total_shares, current_share_price,
                   total_assets, market_cap, position_count
            FROM vault
            WHERE term_id = $1 AND curve_id = $2
            "#,
            term_id,
            curve_id
        )
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Vault not found: {} / {}", term_id, curve_id))?;

        assert_eq!(
            row.position_count, expected_position_count,
            "Position count mismatch for vault {} / {}",
            term_id, curve_id
        );

        Ok(row)
    }

    /// Asserts term aggregation
    pub async fn assert_term_aggregation(
        pool: &PgPool,
        term_id: &str,
        expected_type: &str,
    ) -> Result<TermRow> {
        let row = sqlx::query_as!(
            TermRow,
            r#"
            SELECT id, type, total_assets, total_market_cap
            FROM term
            WHERE id = $1
            "#,
            term_id
        )
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Term not found: {}", term_id))?;

        assert_eq!(row.r#type, expected_type, "Term type mismatch for {}", term_id);

        Ok(row)
    }

    /// Asserts event count across all event tables
    pub async fn assert_total_events(pool: &PgPool, expected: usize) -> Result<()> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM (
                SELECT 1 FROM atom_created_events
                UNION ALL SELECT 1 FROM triple_created_events
                UNION ALL SELECT 1 FROM deposited_events
                UNION ALL SELECT 1 FROM redeemed_events
                UNION ALL SELECT 1 FROM share_price_changed_events
            ) AS all_events"
        )
        .fetch_one(pool)
        .await?;

        assert_eq!(count as usize, expected, "Event count mismatch");

        Ok(())
    }

    /// Asserts that analytics tables are populated
    pub async fn assert_analytics_for_triple(
        pool: &PgPool,
        term_id: &str,
        counter_term_id: &str,
    ) -> Result<()> {
        // Check triple_term exists
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM triple_term WHERE term_id = $1 AND counter_term_id = $2)"
        )
        .bind(term_id)
        .bind(counter_term_id)
        .fetch_one(pool)
        .await?;

        assert!(exists, "Analytics missing for triple {} / {}", term_id, counter_term_id);

        Ok(())
    }
}
```

---

## Example Integration Tests

### Test 1: Out-of-Order Event Processing

```rust
// tests/integration/out_of_order.rs

use crate::helpers::{TestHarness, EventBuilder, DbAssertions};

#[tokio::test]
async fn test_deposits_processed_correctly_despite_out_of_order_arrival() {
    let harness = TestHarness::new().await.unwrap();

    // Setup: Create atom first
    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let account_id = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";

    let atom_event = EventBuilder::new()
        .with_block(1000)
        .atom_created(term_id, account_id);

    harness.publish_events("rindexer_producer", vec![atom_event]).await.unwrap();

    // Publish deposits OUT OF ORDER
    let events = vec![
        // Block 1005 arrives first
        EventBuilder::new().with_block(1005).with_log_index(0)
            .deposited(account_id, term_id, 5000, 5000),
        // Block 1001 arrives second (older!)
        EventBuilder::new().with_block(1001).with_log_index(0)
            .deposited(account_id, term_id, 1000, 1000),
        // Block 1003 arrives third
        EventBuilder::new().with_block(1003).with_log_index(0)
            .deposited(account_id, term_id, 3000, 3000),
    ];

    harness.publish_events("rindexer_producer", events).await.unwrap();

    // Start pipeline
    let config = Config {
        redis_url: harness.redis_url().to_string(),
        database_url: harness.database_url().to_string(),
        stream_names: vec!["rindexer_producer".to_string()],
        ..Default::default()
    };

    let pipeline = EventProcessingPipeline::new(config).await.unwrap();
    let handle = tokio::spawn(async move {
        pipeline.start().await
    });

    // Wait for processing
    harness.wait_for_processing(4, 10).await.unwrap();

    // Assertions
    let pool = harness.get_pool().await.unwrap();

    // Should use the LATEST event (block 1005)
    let position: (String, i64, i64) = sqlx::query_as(
        "SELECT shares, last_deposit_block, last_deposit_log_index
         FROM position
         WHERE account_id = $1 AND term_id = $2"
    )
    .bind(account_id)
    .bind(term_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(position.0, "5000"); // Shares from block 1005
    assert_eq!(position.1, 1005);   // Block number
    assert_eq!(position.2, 0);      // Log index

    // Total deposits should accumulate ALL events
    let total_deposits: String = sqlx::query_scalar(
        "SELECT total_deposit_assets_after_total_fees FROM position
         WHERE account_id = $1 AND term_id = $2"
    )
    .bind(account_id)
    .bind(term_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(total_deposits, "9000"); // 1000 + 3000 + 5000

    handle.abort();
}
```

### Test 2: Concurrent Position Updates with Advisory Locks

```rust
// tests/integration/concurrent_updates.rs

#[tokio::test]
async fn test_concurrent_deposits_to_same_vault_maintain_consistency() {
    let harness = TestHarness::new().await.unwrap();

    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let curve_id = "0x0000000000000000000000000000000000000000000000000000000000000000";

    // Create 10 different accounts depositing concurrently
    let mut events = vec![];
    for i in 0..10 {
        let account_id = format!("0x{:040x}", i);
        events.push(
            EventBuilder::new()
                .with_block(1000 + i)
                .with_log_index(0)
                .deposited(&account_id, term_id, 1000, 1000)
        );
    }

    // Publish all at once
    harness.publish_events("rindexer_producer", events).await.unwrap();

    // Start pipeline with multiple workers
    let config = Config {
        redis_url: harness.redis_url().to_string(),
        database_url: harness.database_url().to_string(),
        workers: 8, // High concurrency
        ..Default::default()
    };

    let pipeline = EventProcessingPipeline::new(config).await.unwrap();
    let handle = tokio::spawn(async move {
        pipeline.start().await
    });

    harness.wait_for_processing(10, 10).await.unwrap();

    // Assertions
    let pool = harness.get_pool().await.unwrap();

    // Vault should have exactly 10 positions
    let vault = DbAssertions::assert_vault_state(
        &pool,
        term_id,
        curve_id,
        10 // Expected position count
    ).await.unwrap();

    // Total shares should be sum of all deposits
    assert_eq!(vault.total_shares, "10000");

    // Each position should exist
    for i in 0..10 {
        let account_id = format!("0x{:040x}", i);
        let shares: String = sqlx::query_scalar(
            "SELECT shares FROM position WHERE account_id = $1 AND term_id = $2"
        )
        .bind(&account_id)
        .bind(term_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(shares, "1000");
    }

    handle.abort();
}
```

### Test 3: Full Lifecycle with Analytics

```rust
// tests/integration/analytics_pipeline.rs

#[tokio::test]
async fn test_triple_lifecycle_updates_analytics_tables() {
    let harness = TestHarness::new().await.unwrap();

    // Step 1: Create atoms
    let subject_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let predicate_id = "0x0000000000000000000000000000000000000000000000000000000000000002";
    let object_id = "0x0000000000000000000000000000000000000000000000000000000000000003";
    let creator = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";

    let mut events = vec![
        EventBuilder::new().with_block(1000).atom_created(subject_id, creator),
        EventBuilder::new().with_block(1001).atom_created(predicate_id, creator),
        EventBuilder::new().with_block(1002).atom_created(object_id, creator),
    ];

    // Step 2: Create triple
    let triple_id = "0x0000000000000000000000000000000000000000000000000000000000000010";
    events.push(
        EventBuilder::new().with_block(1003)
            .triple_created(triple_id, subject_id, predicate_id, object_id)
    );

    // Step 3: Deposit to triple vault
    events.push(
        EventBuilder::new().with_block(1004)
            .deposited(creator, triple_id, 10000, 10000)
    );

    // Step 4: Update share price
    events.push(
        EventBuilder::new().with_block(1005)
            .share_price_changed(triple_id, 1500000000000000000) // 1.5 ETH
    );

    harness.publish_events("rindexer_producer", events).await.unwrap();

    // Start pipeline with analytics worker
    let config = Config {
        redis_url: harness.redis_url().to_string(),
        database_url: harness.database_url().to_string(),
        analytics_stream_name: "term_updates".to_string(),
        ..Default::default()
    };

    let pipeline = EventProcessingPipeline::new(config.clone()).await.unwrap();
    let analytics_worker = AnalyticsWorker::new(config).await.unwrap();

    let pipeline_handle = tokio::spawn(async move {
        pipeline.start().await
    });

    let analytics_handle = tokio::spawn(async move {
        analytics_worker.start().await
    });

    // Wait for processing
    harness.wait_for_processing(6, 10).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await; // Analytics lag

    // Assertions
    let pool = harness.get_pool().await.unwrap();

    // Check triple exists
    let triple_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM triple WHERE term_id = $1)"
    )
    .bind(triple_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(triple_exists);

    // Check term aggregation
    let term = DbAssertions::assert_term_aggregation(&pool, triple_id, "Triple")
        .await
        .unwrap();
    assert_ne!(term.total_market_cap, "0");

    // Check analytics tables populated
    let counter_term_id = calculate_counter_term_id(triple_id);
    DbAssertions::assert_analytics_for_triple(&pool, triple_id, &counter_term_id)
        .await
        .unwrap();

    // Check predicate_object aggregation
    let po_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM predicate_object
         WHERE predicate_id = $1 AND object_id = $2)"
    )
    .bind(predicate_id)
    .bind(object_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(po_exists);

    pipeline_handle.abort();
    analytics_handle.abort();
}
```

---

## Testing Utilities

### Running Tests

```bash
# Run all integration tests
cd redis-postgres-sync
cargo test --test '*' -- --test-threads=1

# Run specific test
cargo test --test integration out_of_order

# Run with logging
RUST_LOG=debug cargo test --test integration

# Run with cleanup verification
cargo test --test integration -- --nocapture
```

### Performance Tests

```rust
// tests/integration/performance.rs

#[tokio::test]
#[ignore] // Run with --ignored flag
async fn benchmark_event_throughput() {
    let harness = TestHarness::new().await.unwrap();

    // Generate 10,000 events
    let mut events = vec![];
    for i in 0..10_000 {
        events.push(
            EventBuilder::new()
                .with_block(1000 + (i / 100))
                .with_log_index(i % 100)
                .deposited(
                    &format!("0x{:040x}", i % 100),
                    "0x0000000000000000000000000000000000000000000000000000000000000001",
                    1000,
                    1000,
                )
        );
    }

    let start = std::time::Instant::now();
    harness.publish_events("rindexer_producer", events).await.unwrap();

    let config = Config {
        redis_url: harness.redis_url().to_string(),
        database_url: harness.database_url().to_string(),
        workers: 8,
        batch_size: 500,
        ..Default::default()
    };

    let pipeline = EventProcessingPipeline::new(config).await.unwrap();
    let handle = tokio::spawn(async move {
        pipeline.start().await
    });

    harness.wait_for_processing(10_000, 60).await.unwrap();

    let elapsed = start.elapsed();
    let throughput = 10_000.0 / elapsed.as_secs_f64();

    println!("Processed 10,000 events in {:?}", elapsed);
    println!("Throughput: {:.2} events/sec", throughput);

    // Assert minimum throughput
    assert!(throughput > 100.0, "Throughput too low: {:.2} events/sec", throughput);

    handle.abort();
}
```

---

## CI Integration

### GitHub Actions Workflow

```yaml
# .github/workflows/integration-tests.yml

name: Integration Tests

on:
  push:
    branches: [ main, develop ]
    paths:
      - 'redis-postgres-sync/**'
  pull_request:
    paths:
      - 'redis-postgres-sync/**'

jobs:
  integration-tests:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Install Rust
        uses: dtolnay/rust-toolchain@stable

      - name: Cache cargo registry
        uses: actions/cache@v4
        with:
          path: ~/.cargo/registry
          key: ${{ runner.os }}-cargo-registry-${{ hashFiles('**/Cargo.lock') }}

      - name: Cache cargo build
        uses: actions/cache@v4
        with:
          path: redis-postgres-sync/target
          key: ${{ runner.os }}-cargo-build-${{ hashFiles('**/Cargo.lock') }}

      - name: Run integration tests
        run: |
          cd redis-postgres-sync
          cargo test --test '*' -- --test-threads=1
        env:
          RUST_LOG: info

      - name: Run performance benchmarks
        run: |
          cd redis-postgres-sync
          cargo test --test integration --ignored -- --nocapture
```

---

## Summary

This testing solution provides:

1. **Isolated Test Environment**: Each test runs with fresh Redis and PostgreSQL containers
2. **Data Cleanup**: `clear_data()` method truncates all tables between tests
3. **Mock Data Generation**: Flexible event builders for realistic scenarios
4. **Out-of-Order Testing**: Validates trigger logic for non-sequential events
5. **Concurrency Testing**: Validates advisory locks prevent race conditions
6. **Full Pipeline Testing**: Tests from Redis ingestion through analytics updates
7. **Clear Assertions**: Helper functions for validating database state
8. **CI Integration**: Automated testing on every PR

### Next Steps

1. Implement `TestHarness` and helper modules
2. Write initial integration tests for each event type
3. Add parameterized tests using `rstest` for edge cases
4. Create performance benchmarks
5. Document test patterns in TESTING.md
6. Set up CI pipeline

This approach ensures comprehensive testing of the redis-postgres-sync component while maintaining test isolation and reproducibility.
