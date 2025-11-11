# Migration Plan: Redis Streams → RabbitMQ

**Status**: Planning Phase
**Created**: 2025-11-11
**Goal**: Replace Redis Streams with RabbitMQ for blockchain event distribution and internal messaging

---

## Architecture Decisions

Based on analysis and requirements clarification:

- **Exchange Type**: Direct exchange with routing keys per event type
- **Consumer Pattern**: Separate queues for each service (surreal-writer, postgres-writer)
- **Internal Communication**: Replace `term_updates` Redis stream with Tokio mpsc channel
- **Rindexer Configuration**: Update rindexer.yaml to publish to RabbitMQ

---

## Current Architecture (Redis)

```
Blockchain → Rindexer → Redis Streams (5 event streams) → Dual Consumers
                              ↓
                ┌─────────────┴─────────────┐
                ↓                           ↓
         SurrealDB Writer          PostgreSQL Writer
                ↓                           ↓
          SurrealDB                   PostgreSQL
                                            ↓
                                   (publishes to Redis)
                                            ↓
                                   term_updates stream
                                            ↓
                                   Analytics Worker
```

**Redis Streams Used**:
1. `intuition_testnet_atom_created`
2. `intuition_testnet_triple_created`
3. `intuition_testnet_deposited`
4. `intuition_testnet_redeemed`
5. `intuition_testnet_share_price_changed`
6. `term_updates` (internal)

---

## Target Architecture (RabbitMQ)

```
Blockchain → Rindexer → RabbitMQ Exchanges (5) → Queues → Dual Consumers
                              ↓
                ┌─────────────┴─────────────┐
                ↓                           ↓
         SurrealDB Writer          PostgreSQL Writer
                ↓                           ↓
          SurrealDB                   PostgreSQL
                                            ↓
                                   (mpsc channel)
                                            ↓
                                   Analytics Worker
```

**RabbitMQ Exchanges** (Direct type):
1. `atom_created` → routing key: `intuition.atom_created`
2. `triple_created` → routing key: `intuition.triple_created`
3. `deposited` → routing key: `intuition.deposited`
4. `redeemed` → routing key: `intuition.redeemed`
5. `share_price_changed` → routing key: `intuition.share_price_changed`

**Queues per Service**:
- SurrealDB Writer: `surreal.atom_created`, `surreal.triple_created`, etc.
- PostgreSQL Writer: `postgres.atom_created`, `postgres.triple_created`, etc.

---

## Migration Steps

### Step 1: Docker Infrastructure Setup ⭐ **START HERE**

**Objective**: Add RabbitMQ to Docker Compose and remove Redis

**Files to Modify**:
- `docker/docker-compose.yml`
- `docker/docker-compose.override.yml`

**Changes**:

1. **Add RabbitMQ service** to `docker-compose.override.yml`:
   ```yaml
   rabbitmq:
     image: rabbitmq:3.13-management-alpine
     container_name: rabbitmq
     ports:
       - "18101:5672"      # AMQP protocol
       - "18102:15672"     # Management UI
     environment:
       RABBITMQ_DEFAULT_USER: ${RABBITMQ_USER:-admin}
       RABBITMQ_DEFAULT_PASS: ${RABBITMQ_PASS:-admin}
     volumes:
       - rabbitmq_data:/var/lib/rabbitmq
     networks:
       - intuition-network
     healthcheck:
       test: ["CMD", "rabbitmq-diagnostics", "ping"]
       interval: 10s
       timeout: 5s
       retries: 5
   ```

2. **Add RabbitMQ Prometheus Exporter**:
   ```yaml
   rabbitmq-exporter:
     image: kbudde/rabbitmq-exporter:latest
     container_name: rabbitmq-exporter
     ports:
       - "18401:9419"
     environment:
       RABBIT_URL: http://rabbitmq:15672
       RABBIT_USER: ${RABBITMQ_USER:-admin}
       RABBIT_PASSWORD: ${RABBITMQ_PASS:-admin}
     networks:
       - intuition-network
     depends_on:
       - rabbitmq
   ```

3. **Add volume**:
   ```yaml
   volumes:
     rabbitmq_data:
   ```

4. **Remove Redis services**:
   - Remove `redis` service
   - Remove `redis-commander` service
   - Remove `redis-exporter` service
   - Remove `redis_data` volume

5. **Update environment variables** in service definitions:
   - Replace `REDIS_URL` with `RABBITMQ_URL`
   - Remove `REDIS_STREAMS`, `CONSUMER_GROUP`, `CONSUMER_GROUP_SUFFIX`
   - Add `RABBITMQ_URL=amqp://admin:admin@rabbitmq:5672`

**Port Allocation Changes**:
| Port | Old Service | New Service |
|------|-------------|-------------|
| 18101 | Redis | RabbitMQ (AMQP) |
| 18102 | SurrealDB | RabbitMQ Management UI |
| 18400 | RedisInsight | (removed) |
| 18401 | Redis Exporter | RabbitMQ Exporter |

**Note**: SurrealDB port will need to move to 18103 or similar.

**Validation**:
- [ ] Run `docker compose up -d rabbitmq`
- [ ] Access RabbitMQ Management UI at http://localhost:18102
- [ ] Verify RabbitMQ Prometheus exporter at http://localhost:18401/metrics
- [ ] Confirm RabbitMQ health check passes: `docker inspect rabbitmq`

**Dependencies**: None - this is the first step

**Estimated Effort**: 1-2 hours

---

### Step 2: Rindexer Configuration

**Objective**: Configure rindexer to publish blockchain events to RabbitMQ exchanges

**File to Modify**:
- `@rindexer/rindexer.yaml`

**Changes**:

1. **Add RabbitMQ connection** at the contract level:
   ```yaml
   contracts:
     - name: MultiVault
       details:
         - network: intuition_testnet
           address: "0x2Ece8D4dEdcB9918A398528f3fa4688b1d2CAB91"
           start_block: "8092570"
       abi: "./abis/MultiVault.abi.json"
       include_events:
         - AtomCreated
         - TripleCreated
         - Deposited
         - Redeemed
         - SharePriceChanged
       streams:
         rabbitmq:
           url: ${RABBITMQ_URL}
           exchanges:
             - exchange: atom_created
               exchange_type: direct
               routing_key: intuition.atom_created
               networks:
                 - intuition_testnet
               events:
                 - event_name: AtomCreated

             - exchange: triple_created
               exchange_type: direct
               routing_key: intuition.triple_created
               networks:
                 - intuition_testnet
               events:
                 - event_name: TripleCreated

             - exchange: deposited
               exchange_type: direct
               routing_key: intuition.deposited
               networks:
                 - intuition_testnet
               events:
                 - event_name: Deposited

             - exchange: redeemed
               exchange_type: direct
               routing_key: intuition.redeemed
               networks:
                 - intuition_testnet
               events:
                 - event_name: Redeemed

             - exchange: share_price_changed
               exchange_type: direct
               routing_key: intuition.share_price_changed
               networks:
                 - intuition_testnet
               events:
                 - event_name: SharePriceChanged
   ```

2. **Set environment variable**:
   ```bash
   export RABBITMQ_URL=amqp://admin:admin@localhost:18101
   ```

**Message Format** (rindexer will publish):
```json
{
  "event_name": "AtomCreated",
  "network": "intuition_testnet",
  "contract_address": "0x2Ece8D4dEdcB9918A398528f3fa4688b1d2CAB91",
  "block_number": 8092570,
  "transaction_hash": "0x...",
  "log_index": 0,
  "parameters": {
    "vaultId": "123",
    "creator": "0x...",
    "data": "..."
  }
}
```

**Validation**:
- [ ] Restart rindexer with new configuration
- [ ] Check RabbitMQ Management UI for created exchanges
- [ ] Verify messages appear in exchanges (without consumers yet)

**Dependencies**: Step 1 (RabbitMQ must be running)

**Estimated Effort**: 1 hour

---

### Step 3: SurrealDB Writer Migration

**Objective**: Migrate SurrealDB writer from Redis to RabbitMQ (simpler service, good test case)

**Files to Create**:
- `surreal-writer/src/consumer/rabbitmq_consumer.rs`

**Files to Modify**:
- `surreal-writer/src/consumer/mod.rs`
- `surreal-writer/src/core/pipeline.rs`
- `surreal-writer/src/config.rs`
- `surreal-writer/src/main.rs`
- `surreal-writer/Cargo.toml`

**Files to Delete**:
- `surreal-writer/src/consumer/redis_stream.rs`

**Key Implementation Details**:

1. **Add `lapin` dependency** to `Cargo.toml`:
   ```toml
   [dependencies]
   lapin = "2.3"
   ```

2. **Remove Redis dependency**:
   ```toml
   # Remove: redis = { version = "0.27", features = ["tokio-comp", "streams"] }
   ```

3. **Create RabbitMQ consumer** (`rabbitmq_consumer.rs`):
   - Connect to RabbitMQ with connection pooling
   - Create channel per worker
   - Declare queues: `surreal.atom_created`, `surreal.triple_created`, etc.
   - Bind queues to exchanges with routing keys
   - Set prefetch count (equivalent to batch size)
   - Implement `basic_consume` with manual acknowledgment
   - Handle messages: deserialize → process → ack/nack
   - Implement reconnection logic with exponential backoff

4. **Update configuration** (`config.rs`):
   ```rust
   pub struct Config {
       pub rabbitmq_url: String,
       pub queue_prefix: String,  // "surreal"
       pub exchanges: Vec<String>, // ["atom_created", "triple_created", ...]
       pub prefetch_count: u16,   // equivalent to batch_size
       // ... rest of config
   }
   ```

5. **Update pipeline** (`pipeline.rs`):
   - Replace `RedisStreamConsumer` with `RabbitMQConsumer`
   - Spawn workers per queue instead of per stream
   - Maintain same batch processing logic

**Message Mapping**:
- Redis field `event_data` → RabbitMQ message body (JSON)
- Redis message ID → RabbitMQ delivery tag (for ack)
- Redis XACK → RabbitMQ basic_ack
- Redis consumer group → Queue competing consumers

**Error Handling**:
- On processing error: `basic_nack` with requeue=true
- On fatal error: `basic_nack` with requeue=false (dead letter queue)
- Circuit breaker pattern remains the same

**Validation**:
- [ ] Build: `cargo build`
- [ ] Unit tests: `cargo test`
- [ ] Start service: `docker compose up surreal-writer`
- [ ] Verify messages consumed from RabbitMQ
- [ ] Check SurrealDB for inserted records
- [ ] Monitor metrics at http://localhost:18210/metrics

**Dependencies**: Steps 1, 2

**Estimated Effort**: 4-6 hours

---

### Step 4: PostgreSQL Writer Migration

**Objective**: Migrate PostgreSQL writer from Redis to RabbitMQ + replace term_updates stream with mpsc channel

**Files to Create**:
- `postgres-writer/src/consumer/rabbitmq_consumer.rs`

**Files to Modify**:
- `postgres-writer/src/consumer/mod.rs`
- `postgres-writer/src/core/pipeline.rs`
- `postgres-writer/src/analytics/worker.rs`
- `postgres-writer/src/config.rs`
- `postgres-writer/src/main.rs`
- `postgres-writer/Cargo.toml`

**Files to Delete**:
- `postgres-writer/src/consumer/redis_stream.rs`
- `postgres-writer/src/consumer/redis_publisher.rs`

**Key Implementation Details**:

1. **Add dependencies** to `Cargo.toml`:
   ```toml
   [dependencies]
   lapin = "2.3"
   tokio = { version = "1", features = ["sync"] }  # for mpsc
   ```

2. **Create RabbitMQ consumer** (similar to SurrealDB writer):
   - Same approach as Step 3
   - Queues: `postgres.atom_created`, `postgres.triple_created`, etc.

3. **Replace term_updates with mpsc channel** (`pipeline.rs`):
   ```rust
   use tokio::sync::mpsc;

   pub async fn run_event_processors(
       config: Arc<Config>,
       postgres_client: Arc<PostgresClient>,
   ) -> Result<()> {
       // Create mpsc channel for term updates
       let (term_tx, term_rx) = mpsc::unbounded_channel();

       // Pass sender to event processors
       let processors = spawn_rabbitmq_consumers(
           config.clone(),
           postgres_client.clone(),
           term_tx,
       );

       // Pass receiver to analytics worker
       let analytics_worker = spawn_analytics_worker(
           config.clone(),
           postgres_client.clone(),
           term_rx,
       );

       // ... rest of orchestration
   }
   ```

4. **Update cascade processor** to send term updates:
   ```rust
   // In vault_updater.rs and term_updater.rs
   pub async fn update_vault(
       client: &PostgresClient,
       event: &Event,
       term_tx: &mpsc::UnboundedSender<TermUpdateMessage>,
   ) -> Result<()> {
       // ... existing logic

       // Send term update via channel instead of Redis
       term_tx.send(TermUpdateMessage {
           term_id,
           timestamp: Utc::now(),
       })?;

       Ok(())
   }
   ```

5. **Update analytics worker** (`analytics/worker.rs`):
   ```rust
   pub async fn run_analytics_worker(
       config: Arc<Config>,
       postgres_client: Arc<PostgresClient>,
       mut term_rx: mpsc::UnboundedReceiver<TermUpdateMessage>,
   ) -> Result<()> {
       loop {
           // Batch collection with timeout
           let mut batch = Vec::new();

           // First message (blocking)
           if let Some(msg) = term_rx.recv().await {
               batch.push(msg);
           } else {
               break; // Channel closed
           }

           // Collect more messages (non-blocking, with timeout)
           let deadline = Instant::now() + Duration::from_millis(100);
           while batch.len() < config.batch_size && Instant::now() < deadline {
               match tokio::time::timeout(Duration::from_millis(10), term_rx.recv()).await {
                   Ok(Some(msg)) => batch.push(msg),
                   Ok(None) => break, // Channel closed
                   Err(_) => break,   // Timeout
               }
           }

           // Process batch
           process_term_updates(postgres_client.as_ref(), batch).await?;
       }

       Ok(())
   }
   ```

6. **Update configuration** (`config.rs`):
   - Remove: `REDIS_URL`, `REDIS_STREAMS`, `CONSUMER_GROUP`, `ANALYTICS_STREAM_NAME`
   - Add: `RABBITMQ_URL`, `QUEUE_PREFIX`, `EXCHANGES`

**Benefits of mpsc Channel**:
- **Performance**: No network overhead, faster communication
- **Simplicity**: No need for Redis connection/serialization
- **Reliability**: Backpressure handled by Tokio runtime
- **Testability**: Easy to mock in tests

**Validation**:
- [ ] Build: `cargo build`
- [ ] Run migrations: `sqlx migrate run`
- [ ] Unit tests: `cargo test`
- [ ] Start service: `docker compose up postgres-writer`
- [ ] Verify messages consumed from RabbitMQ
- [ ] Check PostgreSQL for inserted records
- [ ] Verify analytics tables updated (via mpsc channel)
- [ ] Monitor metrics at http://localhost:18211/metrics

**Dependencies**: Steps 1, 2, 3 (optional - can be parallel)

**Estimated Effort**: 6-8 hours

---

### Step 5: Testing Infrastructure Update

**Objective**: Update integration tests to use RabbitMQ testcontainers

**Files to Modify**:
- `postgres-writer/tests/helpers/test_harness.rs`
- `postgres-writer/tests/*.rs` (all integration tests)
- `postgres-writer/Cargo.toml` (dev dependencies)

**Key Changes**:

1. **Update dev dependencies** in `Cargo.toml`:
   ```toml
   [dev-dependencies]
   # Remove: testcontainers-modules = { version = "0.11", features = ["redis"] }
   # Add:
   testcontainers-modules = { version = "0.11", features = ["rabbitmq"] }
   ```

2. **Update test harness** (`test_harness.rs`):
   ```rust
   use testcontainers_modules::rabbitmq::RabbitMq;

   pub struct TestHarness {
       pub postgres: PostgresContainer,
       pub rabbitmq: RabbitMqContainer,
       pub rabbitmq_url: String,
       pub database_url: String,
       // Remove: redis, redis_url
   }

   impl TestHarness {
       pub async fn new() -> Self {
           let rabbitmq = RabbitMq::default().start().await;
           let rabbitmq_url = format!(
               "amqp://guest:guest@localhost:{}",
               rabbitmq.get_host_port_ipv4(5672).await
           );

           // ... rest of setup
       }

       pub async fn publish_message(
           &self,
           exchange: &str,
           routing_key: &str,
           message: &Event,
       ) -> Result<()> {
           let conn = lapin::Connection::connect(&self.rabbitmq_url, Default::default()).await?;
           let channel = conn.create_channel().await?;

           // Declare exchange
           channel.exchange_declare(
               exchange,
               lapin::ExchangeKind::Direct,
               Default::default(),
               Default::default(),
           ).await?;

           // Publish message
           let payload = serde_json::to_vec(&message)?;
           channel.basic_publish(
               exchange,
               routing_key,
               Default::default(),
               &payload,
               Default::default(),
           ).await?;

           Ok(())
       }
   }
   ```

3. **Update test cases**:
   - Replace `publish_to_stream()` with `publish_message()`
   - Replace stream names with exchange names
   - Adjust assertions for RabbitMQ semantics

**Validation**:
- [ ] Run tests: `cargo test`
- [ ] All integration tests pass
- [ ] Verify test isolation (each test gets fresh RabbitMQ)

**Dependencies**: Step 4

**Estimated Effort**: 3-4 hours

---

### Step 6: Monitoring & Metrics Update

**Objective**: Update Prometheus metrics and Grafana dashboards for RabbitMQ

**Files to Modify**:
- `postgres-writer/src/monitoring/metrics.rs`
- `surreal-writer/src/monitoring/metrics.rs`
- `infrastructure/monitoring/prometheus/prometheus.yml`
- `infrastructure/monitoring/grafana/dashboards/*.json`

**Key Changes**:

1. **Update Prometheus scrape configs** (`prometheus.yml`):
   ```yaml
   scrape_configs:
     # Remove redis-exporter job

     # Add RabbitMQ exporter
     - job_name: 'rabbitmq'
       static_configs:
         - targets: ['rabbitmq-exporter:9419']
   ```

2. **Update application metrics** (`metrics.rs`):
   ```rust
   // Remove Redis-specific metrics:
   // - stream_lag
   // - messages_claimed
   // - last_message_timestamp

   // Add RabbitMQ-specific metrics:
   lazy_static! {
       pub static ref QUEUE_DEPTH: IntGaugeVec = register_int_gauge_vec!(
           "rabbitmq_queue_depth",
           "Number of messages in queue",
           &["queue"]
       ).unwrap();

       pub static ref MESSAGES_CONSUMED: IntCounterVec = register_int_counter_vec!(
           "rabbitmq_messages_consumed_total",
           "Total messages consumed from queue",
           &["queue"]
       ).unwrap();

       pub static ref MESSAGES_ACKED: IntCounterVec = register_int_counter_vec!(
           "rabbitmq_messages_acked_total",
           "Total messages acknowledged",
           &["queue"]
       ).unwrap();

       pub static ref MESSAGES_NACKED: IntCounterVec = register_int_counter_vec!(
           "rabbitmq_messages_nacked_total",
           "Total messages negatively acknowledged",
           &["queue"]
       ).unwrap();
   }
   ```

3. **Update Grafana dashboards**:
   - Replace Redis panels with RabbitMQ panels
   - Use RabbitMQ exporter metrics
   - Update queries to use new metric names

**New Dashboard Panels**:
- Queue depth per queue
- Message rates (consumed/acked/nacked)
- Consumer count per queue
- Message age in queues
- Connection status

**Validation**:
- [ ] Prometheus scrapes RabbitMQ exporter
- [ ] Application metrics appear in Prometheus
- [ ] Grafana dashboards display data
- [ ] No Redis metrics remain

**Dependencies**: Step 4

**Estimated Effort**: 2-3 hours

---

### Step 7: Documentation Update

**Objective**: Update all documentation to reflect RabbitMQ architecture

**Files to Modify**:
- `README.md`
- `CLAUDE.md`
- `postgres-writer/README.md`
- `docker/README.md` (if exists)

**Key Updates**:

1. **Update architecture diagrams** - Replace Redis with RabbitMQ
2. **Update port allocation table** - RabbitMQ ports
3. **Update environment variables** - Remove Redis, add RabbitMQ
4. **Update development commands** - RabbitMQ management UI
5. **Update message flow documentation** - RabbitMQ semantics
6. **Add RabbitMQ setup instructions**

**New Sections to Add**:

**RabbitMQ Management**:
```markdown
### RabbitMQ Management

Access RabbitMQ Management UI at http://localhost:18102
- Username: admin
- Password: admin

Useful management tasks:
- View queues and message counts
- Monitor consumer connections
- Check exchange bindings
- Purge queues for testing
```

**Validation**:
- [ ] All documentation updated
- [ ] No Redis references remain (except in migration history)
- [ ] Architecture diagrams reflect RabbitMQ

**Dependencies**: Step 6

**Estimated Effort**: 2-3 hours

---

## Rollback Plan

If migration fails or issues arise:

1. **Keep Redis configuration** in a separate branch/tag
2. **Docker**: Revert docker-compose files to include Redis
3. **Code**: Git revert to pre-migration commit
4. **Data**: No data loss (databases unchanged)
5. **Rindexer**: Revert rindexer.yaml configuration

**Safety Measures**:
- Test thoroughly on local development first
- Deploy to staging before production
- Keep Redis infrastructure running in parallel initially
- Monitor error rates and performance metrics closely

---

## Success Criteria

- [ ] RabbitMQ running and accessible via management UI
- [ ] Rindexer successfully publishes to RabbitMQ exchanges
- [ ] SurrealDB writer consumes events and writes to SurrealDB
- [ ] PostgreSQL writer consumes events and writes to PostgreSQL
- [ ] Analytics worker receives term updates via mpsc channel
- [ ] All integration tests pass
- [ ] No Redis dependencies remain in code
- [ ] Monitoring/metrics working with RabbitMQ
- [ ] Documentation fully updated
- [ ] Performance equal to or better than Redis implementation

---

## Timeline Estimate

**Total Effort**: 20-30 hours

| Step | Effort | Can Parallelize |
|------|--------|-----------------|
| 1. Docker Infrastructure | 1-2h | No (foundation) |
| 2. Rindexer Config | 1h | No (depends on 1) |
| 3. SurrealDB Writer | 4-6h | No (test case first) |
| 4. PostgreSQL Writer | 6-8h | After step 3 |
| 5. Testing | 3-4h | After step 4 |
| 6. Monitoring | 2-3h | Can parallelize with 5 |
| 7. Documentation | 2-3h | Can parallelize with 6 |

**Recommended Approach**: Complete steps sequentially, testing thoroughly after each step.

---

## Notes

- Focus on **Step 1** first (Docker infrastructure)
- Each step should be tested before proceeding to next
- Use SurrealDB writer as simpler test case before PostgreSQL writer
- The mpsc channel replacement for term_updates is a significant simplification
- Maintain idempotency and retry semantics throughout migration
- RabbitMQ's automatic redelivery is simpler than Redis XCLAIM pattern

---

## Questions / Decisions Needed

- [ ] RabbitMQ version: 3.13 (latest stable) - confirmed
- [ ] Should we use RabbitMQ quorum queues for better durability?
- [ ] Dead letter queue strategy for failed messages?
- [ ] Message TTL configuration?
- [ ] Max queue length limits?
- [ ] Should exchanges be durable or transient?

---

**Next Session**: Start with Step 1 - Docker Infrastructure Setup
