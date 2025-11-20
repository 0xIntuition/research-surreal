# Plan: Replace Redis Analytics Stream with PostgreSQL Table Queue

## Overview

Replace the Redis-based term updates stream with a PostgreSQL table-based queue for the analytics worker. This removes the Redis dependency for analytics processing while maintaining all existing guarantees (durability, retry semantics, observability).

**Scope:**
- ✅ Remove Redis for analytics worker (term_updates stream)
- ✅ Keep Redis for rindexer event ingestion (blockchain events)
- ✅ Maintain separate transaction pattern (queue insert after cascade commit)
- ✅ Preserve all retry/durability guarantees

## Current Architecture

```
Event Processing Flow:
┌─────────────────────────────────────────────────────────────────┐
│ Redis (rindexer) → postgres-writer → PostgreSQL                │
│                          ↓                                       │
│                    Cascade Processor                            │
│                          ↓                                       │
│                    Redis Publisher ← TO BE REPLACED             │
│                          ↓                                       │
│                    Redis Stream (term_updates)                  │
│                          ↓                                       │
│                    Analytics Worker                             │
│                          ↓                                       │
│                    Analytics Tables (PostgreSQL)                │
└─────────────────────────────────────────────────────────────────┘
```

## Target Architecture

```
Event Processing Flow:
┌─────────────────────────────────────────────────────────────────┐
│ Redis (rindexer) → postgres-writer → PostgreSQL                │
│                          ↓                                       │
│                    Cascade Processor                            │
│                          ↓                                       │
│                    Queue Publisher (NEW)                        │
│                          ↓                                       │
│                    term_update_queue Table (NEW)                │
│                          ↓                                       │
│                    Analytics Worker (Modified)                  │
│                          ↓                                       │
│                    Analytics Tables (PostgreSQL)                │
└─────────────────────────────────────────────────────────────────┘
```

## Implementation Steps

### Step 1: Database Schema Migration

**File:** `postgres-writer/migrations/YYYYMMDDHHMMSS_term_update_queue.sql`

Create the queue table:

```sql
-- Term update queue for analytics processing
CREATE TABLE term_update_queue (
    id BIGSERIAL PRIMARY KEY,
    term_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ NULL,
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    last_attempt_at TIMESTAMPTZ NULL
);

-- Index for efficient polling of unprocessed messages
CREATE INDEX idx_term_update_queue_pending
ON term_update_queue (processed_at, created_at)
WHERE processed_at IS NULL;

-- Index for debugging and monitoring
CREATE INDEX idx_term_update_queue_term_id
ON term_update_queue (term_id);

-- Index for cleanup of processed messages
CREATE INDEX idx_term_update_queue_processed
ON term_update_queue (processed_at)
WHERE processed_at IS NOT NULL;

COMMENT ON TABLE term_update_queue IS 'Queue for term updates that need analytics processing';
COMMENT ON COLUMN term_update_queue.term_id IS 'Hex-prefixed term ID that needs analytics update';
COMMENT ON COLUMN term_update_queue.attempts IS 'Number of processing attempts (for retry logic)';
COMMENT ON COLUMN term_update_queue.last_error IS 'Error message from last failed attempt';
```

**Migration commands:**
```bash
cd postgres-writer
sqlx migrate add term_update_queue
# Edit the generated migration file with above SQL
sqlx migrate run
```

---

### Step 2: Create Queue Publisher Module

**File:** `postgres-writer/src/consumer/postgres_queue_publisher.rs`

```rust
use sqlx::PgPool;
use tracing::{debug, error, warn};

/// Publisher for term update queue
pub struct TermQueuePublisher {
    pool: PgPool,
}

impl TermQueuePublisher {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Publish a single term update to the queue
    pub async fn publish_term_update(&self, term_id: &str) -> Result<(), sqlx::Error> {
        debug!("Publishing term update to queue: {}", term_id);

        sqlx::query!(
            r#"
            INSERT INTO term_update_queue (term_id)
            VALUES ($1)
            "#,
            term_id
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Publish multiple term updates in a batch
    pub async fn publish_batch(&self, term_ids: Vec<String>) -> Result<(), sqlx::Error> {
        if term_ids.is_empty() {
            return Ok(());
        }

        debug!("Publishing {} term updates to queue", term_ids.len());

        // Use unnest for efficient batch insert
        sqlx::query!(
            r#"
            INSERT INTO term_update_queue (term_id)
            SELECT * FROM UNNEST($1::text[])
            "#,
            &term_ids
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
```

**Key decisions:**
- Simple INSERT (no deduplication) - let analytics processor handle duplicate term_ids
- Batch insert using UNNEST for efficiency
- Uses existing PgPool (no new connections needed)

---

### Step 3: Create Queue Consumer Module

**File:** `postgres-writer/src/consumer/postgres_queue_consumer.rs`

```rust
use sqlx::PgPool;
use tracing::{debug, error, info, warn};
use std::time::Duration;

/// Message from the term update queue
#[derive(Debug, Clone)]
pub struct QueueMessage {
    pub id: i64,
    pub term_id: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub attempts: i32,
}

/// Consumer for term update queue
pub struct TermQueueConsumer {
    pool: PgPool,
    poll_interval: Duration,
    max_retry_attempts: i32,
}

impl TermQueueConsumer {
    pub fn new(pool: PgPool, poll_interval: Duration, max_retry_attempts: i32) -> Self {
        Self {
            pool,
            poll_interval,
            max_retry_attempts,
        }
    }

    /// Poll for unprocessed messages
    /// Uses FOR UPDATE SKIP LOCKED for concurrent-safe consumption
    pub async fn poll_messages(&self, limit: i64) -> Result<Vec<QueueMessage>, sqlx::Error> {
        let messages = sqlx::query_as!(
            QueueMessage,
            r#"
            SELECT id, term_id, created_at, attempts
            FROM term_update_queue
            WHERE processed_at IS NULL
              AND attempts < $1
            ORDER BY created_at ASC
            LIMIT $2
            FOR UPDATE SKIP LOCKED
            "#,
            self.max_retry_attempts,
            limit
        )
        .fetch_all(&self.pool)
        .await?;

        if !messages.is_empty() {
            debug!("Polled {} messages from queue", messages.len());
        }

        Ok(messages)
    }

    /// Mark a message as successfully processed
    pub async fn mark_processed(&self, id: i64) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE term_update_queue
            SET processed_at = NOW()
            WHERE id = $1
            "#,
            id
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Mark a message as failed, increment attempts
    pub async fn mark_failed(&self, id: i64, error_msg: &str) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE term_update_queue
            SET attempts = attempts + 1,
                last_error = $2,
                last_attempt_at = NOW()
            WHERE id = $1
            "#,
            id,
            error_msg
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Clean up old processed messages
    pub async fn cleanup_old_messages(&self, retention_hours: i32) -> Result<u64, sqlx::Error> {
        let result = sqlx::query!(
            r#"
            DELETE FROM term_update_queue
            WHERE processed_at IS NOT NULL
              AND processed_at < NOW() - INTERVAL '1 hour' * $1
            "#,
            retention_hours
        )
        .execute(&self.pool)
        .await?;

        let deleted = result.rows_affected();
        if deleted > 0 {
            info!("Cleaned up {} old processed messages", deleted);
        }

        Ok(deleted)
    }

    /// Get queue statistics for monitoring
    pub async fn get_queue_stats(&self) -> Result<QueueStats, sqlx::Error> {
        let stats = sqlx::query_as!(
            QueueStats,
            r#"
            SELECT
                COUNT(*) FILTER (WHERE processed_at IS NULL) as "pending_count!",
                COUNT(*) FILTER (WHERE processed_at IS NULL AND attempts > 0) as "retry_count!",
                EXTRACT(EPOCH FROM (NOW() - MIN(created_at))) FILTER (WHERE processed_at IS NULL) as "oldest_pending_age_secs"
            FROM term_update_queue
            "#
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(stats)
    }

    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }
}

#[derive(Debug)]
pub struct QueueStats {
    pub pending_count: i64,
    pub retry_count: i64,
    pub oldest_pending_age_secs: Option<f64>,
}
```

**Key features:**
- `FOR UPDATE SKIP LOCKED` ensures concurrent workers don't process same message
- Retry limit prevents infinite loops on permanently failing messages
- Cleanup job prevents unbounded table growth
- Statistics for monitoring/alerting

---

### Step 4: Update PostgresClient

**File:** `postgres-writer/src/sync/postgres_client.rs`

**Changes:**

1. Replace Redis publisher with queue publisher:

```rust
// OLD:
use crate::consumer::redis_publisher::RedisPublisher;

// NEW:
use crate::consumer::postgres_queue_publisher::TermQueuePublisher;
```

2. Update struct field:

```rust
pub struct PostgresClient {
    pool: PgPool,
    // OLD: redis_publisher: Option<Mutex<RedisPublisher>>,
    // NEW:
    term_queue_publisher: Option<TermQueuePublisher>,
}
```

3. Update constructor:

```rust
impl PostgresClient {
    pub fn new(
        pool: PgPool,
        // OLD: redis_publisher: Option<RedisPublisher>,
        // NEW:
        term_queue_publisher: Option<TermQueuePublisher>,
    ) -> Self {
        Self {
            pool,
            // OLD: redis_publisher: redis_publisher.map(Mutex::new),
            // NEW:
            term_queue_publisher,
        }
    }
}
```

4. Update term update publishing (around line 188-198):

```rust
// OLD:
if let Some(redis_publisher) = &self.redis_publisher {
    let publisher = redis_publisher.lock().await;
    for term_id in &term_ids {
        if let Err(e) = publisher.publish_term_update(term_id).await {
            error!("Failed to publish term update to Redis: {}", e);
        }
    }
}

// NEW:
if let Some(publisher) = &self.term_queue_publisher {
    if let Err(e) = publisher.publish_batch(term_ids.clone()).await {
        error!("Failed to publish term updates to queue: {}", e);
    }
}
```

**Benefits of this change:**
- ✅ Eliminates mutex/lock contention (addresses TODO on line 18)
- ✅ Batch insert is more efficient than individual Redis XADDs
- ✅ Simpler error handling (no per-message failures)

---

### Step 5: Update Analytics Worker

**File:** `postgres-writer/src/analytics/worker.rs`

**Major changes:**

1. Replace Redis consumer with queue consumer:

```rust
// OLD:
use crate::consumer::redis_stream::RedisStreamConsumer;

// NEW:
use crate::consumer::postgres_queue_consumer::{TermQueueConsumer, QueueMessage};
```

2. Update worker function signature:

```rust
// OLD:
pub async fn start_analytics_worker(
    redis_consumer: RedisStreamConsumer,
    pg_pool: PgPool,
    // ...
)

// NEW:
pub async fn start_analytics_worker(
    queue_consumer: TermQueueConsumer,
    pg_pool: PgPool,
    // ...
)
```

3. Replace stream consumption with polling loop:

```rust
// OLD:
loop {
    match redis_consumer.consume_messages().await {
        Ok(messages) => {
            for msg in messages {
                // process...
                redis_consumer.ack_message(&msg.id).await?;
            }
        }
    }
}

// NEW:
loop {
    match queue_consumer.poll_messages(100).await {
        Ok(messages) => {
            for msg in messages {
                match process_term_update(&msg.term_id, &pg_pool).await {
                    Ok(_) => {
                        queue_consumer.mark_processed(msg.id).await?;
                    }
                    Err(e) => {
                        error!("Failed to process term {}: {}", msg.term_id, e);
                        queue_consumer.mark_failed(msg.id, &e.to_string()).await?;
                    }
                }
            }
        }
        Err(e) => {
            error!("Failed to poll queue: {}", e);
            tokio::time::sleep(queue_consumer.poll_interval()).await;
        }
    }

    // Sleep if no messages
    if messages.is_empty() {
        tokio::time::sleep(queue_consumer.poll_interval()).await;
    }
}
```

4. Add periodic cleanup task:

```rust
// Spawn cleanup task
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_hours(1));
    loop {
        interval.tick().await;
        if let Err(e) = queue_consumer.cleanup_old_messages(24).await {
            error!("Failed to cleanup old messages: {}", e);
        }
    }
});
```

**Key changes:**
- Polling replaces push-based consumption
- Explicit success/failure handling with `mark_processed` / `mark_failed`
- Periodic cleanup prevents table bloat
- Keep existing rate limiting logic

---

### Step 6: Update Configuration

**File:** `postgres-writer/src/config.rs`

**Remove:**
```rust
// OLD:
pub analytics_stream_name: String,
```

**Add:**
```rust
// NEW:
#[serde(default = "default_queue_poll_interval_ms")]
pub queue_poll_interval_ms: u64,

#[serde(default = "default_queue_retention_hours")]
pub queue_retention_hours: i32,

#[serde(default = "default_max_retry_attempts")]
pub max_retry_attempts: i32,

// Default functions
fn default_queue_poll_interval_ms() -> u64 { 100 }
fn default_queue_retention_hours() -> i32 { 24 }
fn default_max_retry_attempts() -> i32 { 3 }
```

**Environment variables:**
```bash
# Remove:
ANALYTICS_STREAM_NAME=term_updates

# Add:
QUEUE_POLL_INTERVAL_MS=100        # How often to poll for new messages
QUEUE_RETENTION_HOURS=24          # Keep processed messages for 24 hours
MAX_RETRY_ATTEMPTS=3              # Max attempts before giving up
```

---

### Step 7: Update Main Entry Point

**File:** `postgres-writer/src/main.rs`

**Changes:**

1. Remove Redis client initialization for analytics:

```rust
// OLD:
let redis_client = redis::Client::open(config.redis_url.clone())?;
let redis_publisher = RedisPublisher::new(redis_client.clone(), config.analytics_stream_name);

// NEW:
let term_queue_publisher = TermQueuePublisher::new(pool.clone());
```

2. Update PostgresClient initialization:

```rust
// OLD:
let postgres_client = Arc::new(PostgresClient::new(
    pool.clone(),
    Some(redis_publisher),
));

// NEW:
let postgres_client = Arc::new(PostgresClient::new(
    pool.clone(),
    Some(term_queue_publisher),
));
```

3. Update analytics worker initialization:

```rust
// OLD:
let analytics_consumer = RedisStreamConsumer::new(/* ... */);
start_analytics_worker(analytics_consumer, pool.clone()).await;

// NEW:
let queue_consumer = TermQueueConsumer::new(
    pool.clone(),
    Duration::from_millis(config.queue_poll_interval_ms),
    config.max_retry_attempts,
);
start_analytics_worker(queue_consumer, pool.clone()).await;
```

**Note:** Keep Redis client for rindexer event consumption!

---

### Step 8: Update Module Exports

**File:** `postgres-writer/src/consumer/mod.rs`

```rust
pub mod redis_stream;           // Keep - still used for rindexer events
// pub mod redis_publisher;     // Remove or keep if used elsewhere
pub mod postgres_queue_publisher;  // Add
pub mod postgres_queue_consumer;   // Add
```

---

### Step 9: Update Dependencies (Optional)

**File:** `postgres-writer/Cargo.toml`

**Option A: Make Redis optional (if you want flexibility):**

```toml
[dependencies]
redis = { version = "0.24", features = ["tokio-comp", "streams"], optional = true }

[features]
default = ["redis-streams"]
redis-streams = ["redis"]
```

**Option B: Keep as-is** (Redis still needed for rindexer events)

**Recommendation:** Keep as-is for now, can optimize later.

---

### Step 10: Update Monitoring/Metrics

**File:** `postgres-writer/src/monitoring/metrics.rs`

**Add new metrics:**

```rust
lazy_static! {
    // Queue depth metrics
    pub static ref TERM_QUEUE_PENDING: IntGauge = register_int_gauge!(
        "term_queue_pending_count",
        "Number of pending term updates in queue"
    ).unwrap();

    pub static ref TERM_QUEUE_RETRY: IntGauge = register_int_gauge!(
        "term_queue_retry_count",
        "Number of term updates being retried"
    ).unwrap();

    pub static ref TERM_QUEUE_OLDEST_AGE: Gauge = register_gauge!(
        "term_queue_oldest_pending_age_seconds",
        "Age of oldest pending message in seconds"
    ).unwrap();
}
```

**Update metrics collection in analytics worker:**

```rust
// Periodically update queue metrics
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;
        if let Ok(stats) = queue_consumer.get_queue_stats().await {
            TERM_QUEUE_PENDING.set(stats.pending_count);
            TERM_QUEUE_RETRY.set(stats.retry_count);
            if let Some(age) = stats.oldest_pending_age_secs {
                TERM_QUEUE_OLDEST_AGE.set(age);
            }
        }
    }
});
```

**Remove old Redis stream lag metrics** (if analytics-specific)

---

### Step 11: Update Tests

**File:** `postgres-writer/tests/integration_test.rs` (or similar)

**Key changes:**

1. Remove Redis testcontainer for analytics tests
2. Keep PostgreSQL testcontainer
3. Test queue operations:

```rust
#[tokio::test]
async fn test_term_queue_publish_and_consume() {
    let pool = setup_test_db().await;

    // Publish
    let publisher = TermQueuePublisher::new(pool.clone());
    publisher.publish_term_update("0x123").await.unwrap();

    // Consume
    let consumer = TermQueueConsumer::new(pool.clone(), Duration::from_millis(100), 3);
    let messages = consumer.poll_messages(10).await.unwrap();

    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].term_id, "0x123");

    // Mark processed
    consumer.mark_processed(messages[0].id).await.unwrap();

    // Verify processed
    let pending = consumer.poll_messages(10).await.unwrap();
    assert_eq!(pending.len(), 0);
}

#[tokio::test]
async fn test_term_queue_retry_logic() {
    let pool = setup_test_db().await;
    let publisher = TermQueuePublisher::new(pool.clone());
    let consumer = TermQueueConsumer::new(pool.clone(), Duration::from_millis(100), 3);

    publisher.publish_term_update("0x456").await.unwrap();

    // Fail 3 times
    for i in 0..3 {
        let messages = consumer.poll_messages(10).await.unwrap();
        assert_eq!(messages.len(), 1);
        consumer.mark_failed(messages[0].id, "test error").await.unwrap();
    }

    // 4th attempt should not return message (max attempts reached)
    let messages = consumer.poll_messages(10).await.unwrap();
    assert_eq!(messages.len(), 0);
}

#[tokio::test]
async fn test_queue_cleanup() {
    let pool = setup_test_db().await;
    let publisher = TermQueuePublisher::new(pool.clone());
    let consumer = TermQueueConsumer::new(pool.clone(), Duration::from_millis(100), 3);

    // Publish and process
    publisher.publish_term_update("0x789").await.unwrap();
    let messages = consumer.poll_messages(10).await.unwrap();
    consumer.mark_processed(messages[0].id).await.unwrap();

    // Cleanup (should delete processed message older than retention)
    let deleted = consumer.cleanup_old_messages(0).await.unwrap();
    assert_eq!(deleted, 1);
}
```

---

### Step 12: Update Documentation

**File:** `postgres-writer/README.md`

Update architecture section:

```markdown
## Architecture

### Data Flow

1. **Event Consumption**: Redis stream (from rindexer) → Event handlers
2. **Event Processing**: Insert into `*_events` tables (triggers update base tables)
3. **Cascade Processing**: Update vault/term aggregations (separate transaction)
4. **Queue Publishing**: Insert term_id into `term_update_queue` table
5. **Analytics Worker**: Poll queue → Update analytics tables → Mark processed

### Queue-Based Analytics

The analytics worker uses a PostgreSQL table (`term_update_queue`) as a message queue:

- **Publishing**: After cascade updates complete, term_ids are inserted into the queue
- **Consumption**: Worker polls using `FOR UPDATE SKIP LOCKED` for concurrent-safe processing
- **Retry Logic**: Failed messages are retried up to `MAX_RETRY_ATTEMPTS` times
- **Cleanup**: Processed messages are retained for `QUEUE_RETENTION_HOURS` then deleted

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `QUEUE_POLL_INTERVAL_MS` | 100 | Polling interval for queue consumer |
| `QUEUE_RETENTION_HOURS` | 24 | How long to keep processed messages |
| `MAX_RETRY_ATTEMPTS` | 3 | Max attempts before giving up on a message |
```

**File:** `CLAUDE.md`

Update architecture diagram and data flow section to reflect queue-based approach.

---

### Step 13: Update Docker Configuration

**File:** `docker/docker-compose.yml`

Update postgres-writer service environment:

```yaml
services:
  postgres-writer:
    environment:
      # Keep Redis URL for rindexer event consumption
      - REDIS_URL=redis://redis:6379

      # Remove:
      # - ANALYTICS_STREAM_NAME=term_updates

      # Add:
      - QUEUE_POLL_INTERVAL_MS=100
      - QUEUE_RETENTION_HOURS=24
      - MAX_RETRY_ATTEMPTS=3
```

**Note:** Keep Redis service running for rindexer event ingestion!

---

## Testing Plan

### Unit Tests
- ✅ Queue publisher: single and batch operations
- ✅ Queue consumer: poll, mark_processed, mark_failed
- ✅ Cleanup: old message deletion
- ✅ Statistics: queue depth, retry counts, age metrics

### Integration Tests
- ✅ End-to-end: event → cascade → queue → analytics
- ✅ Concurrent consumption: multiple workers with SKIP LOCKED
- ✅ Retry logic: failed messages retry until max attempts
- ✅ Cleanup: processed messages deleted after retention period

### Manual Testing
- ✅ Deploy to staging environment
- ✅ Monitor queue depth and processing lag
- ✅ Verify analytics tables update correctly
- ✅ Test failure scenarios (database connection loss, etc.)
- ✅ Check Prometheus metrics in Grafana

---

## Rollout Strategy

### Phase 1: Development
1. Create feature branch
2. Implement all code changes
3. Run unit and integration tests
4. Update documentation

### Phase 2: Testing
1. Deploy to local Docker environment
2. Run rindexer to generate events
3. Monitor queue and analytics processing
4. Verify Grafana dashboards show queue metrics

### Phase 3: Staging
1. Deploy to staging environment
2. Run alongside existing Redis-based system (if possible)
3. Compare analytics table consistency
4. Monitor for 24+ hours

### Phase 4: Production
1. Deploy during low-traffic window
2. Monitor queue depth and lag metrics
3. Have rollback plan ready (revert to Redis)
4. Verify analytics accuracy

---

## Rollback Plan

If issues arise:

1. **Quick rollback:** Revert code changes, redeploy previous version
2. **Data recovery:** Queue table retains all messages, can re-process if needed
3. **Redis fallback:** Keep Redis running during initial rollout for quick switch-back

---

## Performance Considerations

### Polling Overhead
- Default 100ms interval = 10 polls/second when idle
- Minimal CPU impact with proper indexing
- Can increase interval if needed (trade-off: higher latency)

### Table Growth
- ~1 row per term update
- Assuming 1000 term updates/second sustained: 86M rows/day
- Cleanup every 24 hours: ~86M rows deleted/day
- Consider partitioning if volume becomes issue

### Index Maintenance
- Partial indexes minimize overhead (WHERE processed_at IS NULL)
- VACUUM automatically runs on cleaned-up rows
- Monitor index bloat in production

---

## Success Metrics

- ✅ Zero Redis connections for analytics (check logs)
- ✅ Queue depth stays < 1000 under normal load
- ✅ Analytics lag < 5 seconds (p99)
- ✅ No message loss (verify counts match)
- ✅ CPU/memory usage comparable to Redis approach
- ✅ Grafana dashboards show queue metrics

---

## Future Enhancements

### Potential Improvements
1. **Deduplication**: Track last processed term state, skip redundant updates
2. **Partitioning**: Partition queue table by created_at for faster cleanup
3. **Priority queue**: Add priority column for critical term updates
4. **Dead letter queue**: Separate table for permanently failed messages
5. **Batch analytics**: Process multiple terms in single query for efficiency

### Alternative: PostgreSQL LISTEN/NOTIFY
If queue table becomes bottleneck, consider hybrid:
- Keep queue table for durability
- Add NOTIFY trigger on INSERT for push-based consumption
- Worker uses LISTEN + periodic polling as fallback

---

## Files to Modify

### New Files (7)
1. `postgres-writer/migrations/YYYYMMDDHHMMSS_term_update_queue.sql`
2. `postgres-writer/src/consumer/postgres_queue_publisher.rs`
3. `postgres-writer/src/consumer/postgres_queue_consumer.rs`
4. (Optional) `postgres-writer/tests/queue_tests.rs`

### Modified Files (8)
1. `postgres-writer/src/sync/postgres_client.rs`
2. `postgres-writer/src/analytics/worker.rs`
3. `postgres-writer/src/config.rs`
4. `postgres-writer/src/main.rs`
5. `postgres-writer/src/consumer/mod.rs`
6. `postgres-writer/src/monitoring/metrics.rs`
7. `postgres-writer/README.md`
8. `CLAUDE.md`

### Updated Files (2)
1. `docker/docker-compose.yml`
2. `postgres-writer/Cargo.toml` (if making Redis optional)

### Test Files (1+)
1. `postgres-writer/tests/integration_test.rs` (or new test files)

---

## Estimated Effort

- **Database migration:** 30 minutes
- **Queue publisher/consumer modules:** 2-3 hours
- **PostgresClient updates:** 1 hour
- **Analytics worker refactor:** 2-3 hours
- **Configuration updates:** 30 minutes
- **Tests:** 2-3 hours
- **Documentation:** 1 hour
- **Testing/validation:** 2-4 hours

**Total:** ~12-16 hours of development + testing time

---

## Questions / Decisions Needed

1. ✅ **Queue retention period:** 24 hours acceptable? (Configurable via env var)
2. ✅ **Max retry attempts:** 3 attempts reasonable? (Configurable via env var)
3. ✅ **Polling interval:** 100ms good balance between latency and overhead?
4. ⚠️ **Deduplication:** Should we deduplicate identical term_ids in queue? (Currently: no)
5. ⚠️ **Dead letter queue:** Create separate table for permanently failed messages? (Currently: no)
6. ⚠️ **Partitioning:** Implement table partitioning from start? (Currently: no, add later if needed)

---

## References

- Current Redis publisher: `postgres-writer/src/consumer/redis_publisher.rs`
- Current analytics worker: `postgres-writer/src/analytics/worker.rs`
- Current cascade logic: `postgres-writer/src/processors/cascade.rs`
- PostgreSQL SKIP LOCKED docs: https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE
- Queue pattern discussion: https://www.2ndquadrant.com/en/blog/what-is-select-skip-locked-for-in-postgresql-9-5/
