# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Research Surreal is a high-performance blockchain event indexing and dual-database data pipeline for the Intuition protocol. It indexes blockchain events from the MultiVault contract, streams them through Redis, and synchronizes to both SurrealDB (NoSQL) and PostgreSQL (TimescaleDB) with real-time analytics.

## Architecture

The system follows this data flow:

```
Blockchain → Rindexer → Redis Streams → Dual Writers → SurrealDB + PostgreSQL
```

**Key Components:**
- **Rindexer**: Indexes blockchain events from Intuition testnet (chain ID: 13579)
- **Redis Streams**: Event buffering and distribution layer
- **SurrealDB Writer**: Rust service consuming events → SurrealDB (NoSQL)
- **PostgreSQL Writer**: Rust service consuming events → PostgreSQL (TimescaleDB)
- **Analytics Layer**: Materialized views and trigger-based aggregations in PostgreSQL
- **Monitoring**: Prometheus + Grafana for observability
- **Web Dashboard**: Next.js 15 app for real-time metrics

## Development Commands

### Rust Services (postgres-writer and surreal-writer)

**Set Docker Host** (required for tests):
```bash
export DOCKER_HOST=unix:///Users/simonas/.docker/run/docker.sock
```

**Build and Check:**
```bash
cd postgres-writer  # or surreal-writer
cargo check
cargo build
cargo build --release
cargo clippy
cargo fmt
```

**Run Tests:**
```bash
# Basic test run
cargo test

# With debug logging
RUST_LOG=debug cargo test

# With detailed logging (for specific module)
RUST_LOG=redis_postgres_sync=debug cargo test

# With Docker host (required for testcontainers)
DOCKER_HOST=unix:///Users/simonas/.docker/run/docker.sock cargo test

# Full debugging
env DOCKER_HOST=unix:///Users/simonas/.docker/run/docker.sock RUST_BACKTRACE=1 RUST_LOG=redis_postgres_sync=debug cargo test
```

**Database Migrations:**
```bash
cd postgres-writer
sqlx migrate run
```

### Docker Services

**Start all services:**
```bash
cd docker
docker compose up -d
```

**Restart sync services:**
```bash
./docker/restart-syncs.sh
```

**Check service status:**
```bash
docker ps
docker logs <container-name>
docker inspect <container-name>
```

**Health checks:**
```bash
curl http://localhost:18210/health  # SurrealDB writer
curl http://localhost:18211/health  # PostgreSQL writer
```

### Git Workflow

**View changes and status:**
```bash
git status
git log --oneline -10
git diff
```

**Create commits:**
```bash
git add .
git commit -m "message"
```

**Work with PRs:**
```bash
gh pr list
gh pr view <number>
gh pr checks <number>
gh api repos/0xIntuition/research-surreal/pulls/<number>/comments
```

## PostgreSQL Writer Architecture

This is the most complex component. Understanding its architecture is critical for making changes.

### Event Processing Flow

1. **Event Consumption**: Events arrive from Redis streams
2. **Event Table Insert**: Event written to immutable `*_events` table (idempotent via ON CONFLICT)
3. **Trigger Execution**: PostgreSQL triggers update base tables (`atom`, `triple`, `position`, `vault`)
4. **Cascade Processing**: Rust cascade processor updates aggregations (`vault` metrics, `term` totals)
5. **Transaction Commit**: All changes committed atomically
6. **Redis Publishing**: Term updates published to `term_updates` stream
7. **Analytics Worker**: Separate thread processes term updates → analytics tables

### Table Hierarchy

**Level 0 - Event Tables** (immutable audit log):
- `atom_created_events`, `triple_created_events`, `deposited_events`, `redeemed_events`, `share_price_changed_events`

**Level 1 - Base Tables** (updated by triggers):
- `atom`, `triple`, `position`, `vault`

**Level 2 - Aggregated Tables** (updated by Rust cascade):
- `term` (aggregates across all vaults for a term)

**Level 3 - Analytics Tables** (updated by analytics worker):
- `triple_vault`, `triple_term`, `predicate_object`, `subject_predicate`

### Transaction Strategy

The system uses **separate transactions** for event insertion and cascade updates:

1. **Transaction 1**: Insert event → Triggers fire → Base tables updated → COMMIT
2. **Transaction 2**: Cascade updates → COMMIT (on success, ACK Redis message)
3. **On Failure**: Redis redelivers message → Event insert is idempotent → Retry cascade

**Why separate transactions?**
- Better performance (smaller, faster transactions)
- Reduced lock contention
- Higher throughput under load
- Safe retry semantics via idempotent event handlers

See `postgres-writer/README.md` "Transaction Handling and Retry Semantics" section for full details.

### Code Organization

```
postgres-writer/src/
├── main.rs                    # Entry point
├── config.rs                  # Environment configuration
├── core/
│   ├── pipeline.rs            # Event processing orchestration
│   └── circuit_breaker.rs     # Reliability patterns
├── consumer/
│   ├── redis_stream.rs        # Redis stream consumer
│   └── redis_publisher.rs     # Publish term updates
├── sync/
│   ├── postgres_client.rs     # Database client + cascade orchestration
│   ├── event_handlers.rs      # Event routing
│   └── *.rs                   # Event-specific handlers
├── processors/
│   ├── cascade.rs             # Cascade processor orchestration
│   ├── vault_updater.rs       # Vault aggregation logic
│   └── term_updater.rs        # Term aggregation logic
├── analytics/
│   ├── worker.rs              # Analytics worker thread
│   └── processor.rs           # Analytics table updates
└── monitoring/
    ├── metrics.rs             # Prometheus metrics
    └── health.rs              # Health checks
```

## Known Issues and Critical Context

**Critical Issues:**
1. **Health endpoint naming**: Returns `surreal_sync_healthy` instead of `postgres_sync_healthy` (`src/monitoring/health.rs`)
2. **Non-graceful shutdown**: Analytics worker and HTTP server are aborted on shutdown, risking data loss

**High Priority:**
- Redis publisher lock contention under high load
- Advisory lock hash collision risk at scale
- No automatic backfill mechanism for analytics tables

**Migration History:**
- Migrations 20250130000002-20250130000010: Old materialized view approach (reference only, NOT used)
- Migrations 20250131000001-20250131000003: Current production approach (triggers + Rust cascade)

## Port Allocation

All services use ports in the **18000-18999 range**:

| Port | Service |
|------|---------|
| 18100 | PostgreSQL |
| 18101 | Redis |
| 18102 | SurrealDB API |
| 18200 | Rindexer GraphQL |
| 18210 | SurrealDB Writer (health/metrics) |
| 18211 | PostgreSQL Writer (health/metrics) |
| 18300 | Web Dashboard |
| 18301 | Surrealist (SurrealDB IDE) |
| 18400 | RedisInsight |
| 18500 | Prometheus |
| 18501 | Grafana |

## Key Configuration

### Environment Variables

**SurrealDB Writer:**
- `REDIS_URL`, `SURREAL_URL`, `SURREAL_USER`, `SURREAL_PASS`
- `REDIS_STREAMS`: Comma-separated stream names
- `BATCH_SIZE=20`, `PROCESSING_INTERVAL_MS=100`
- `CONSUMER_GROUP`, `CONSUMER_NAME`
- `TOKIO_WORKER_THREADS=4`

**PostgreSQL Writer:**
- `REDIS_URL`, `DATABASE_URL`
- `REDIS_STREAMS`: Comma-separated stream names
- `BATCH_SIZE=100`, `BATCH_TIMEOUT_MS=5000`
- `CONSUMER_GROUP`, `CONSUMER_GROUP_SUFFIX`
- `ANALYTICS_STREAM_NAME=term_updates`
- `TOKIO_WORKER_THREADS=4`

### Blockchain Contract

- **Contract**: MultiVault (0x2Ece8D4dEdcB9918A398528f3fa4688b1d2CAB91)
- **Network**: Intuition Testnet (Chain ID: 13579)
- **Start Block**: 8092570
- **Events**: AtomCreated, TripleCreated, Deposited, Redeemed, SharePriceChanged

## Important Patterns

### Out-of-Order Event Handling

Events may arrive out of order due to blockchain reorgs. All triggers use `is_event_newer()` helper:

```sql
is_event_newer(new_block, new_log_index, old_block, old_log_index)
```

Only updates if new event is actually newer by `(block_number, log_index)` tuple.

### Idempotent Event Handlers

All event handlers use `ON CONFLICT DO UPDATE` to safely handle retries:

```rust
INSERT INTO deposited_events (...) VALUES (...)
ON CONFLICT (transaction_hash, log_index) DO UPDATE SET ...
```

### Advisory Locks

Cascade processor uses PostgreSQL advisory locks to prevent race conditions:

```rust
// FNV-1a hash to generate lock ID from (term_id, curve_id)
SELECT pg_advisory_xact_lock(hash_value)
```

**Note**: Hash collisions are theoretically possible at scale.

### Adding New Event Types

1. Create event struct in `src/sync/your_event.rs`
2. Implement handler with `ON CONFLICT` for idempotency
3. Export handler in `src/sync/mod.rs`
4. Add routing in `src/sync/event_handlers.rs`
5. Add migration for trigger (if needed)
6. Add cascade logic in `src/processors/cascade.rs` (if needed)
7. Update `REDIS_STREAMS` configuration

## Testing Strategy

- Use `testcontainers` for Redis and PostgreSQL in tests
- Tests run in Docker containers (requires `DOCKER_HOST` env var)
- Use `serial_test` crate for tests that cannot run concurrently
- Mock data via `fake` crate
- Assertions via `assert2` and `similar-asserts`

## Deployment Context

This service runs on Dokploy with:
- Traefik reverse proxy for HTTPS
- Automatic Docker network (`dokploy-network`)
- Persistent volumes for databases
- Health checks and auto-restart policies
- Prometheus metrics scraping
- Grafana dashboards for monitoring

## Technology Stack

**Backend**: Rust (edition 2021), Tokio async runtime, sqlx (PostgreSQL), redis, surrealdb
**Frontend**: Next.js 15, TypeScript, React 18, Tailwind CSS
**Infrastructure**: Docker, Docker Compose, TimescaleDB (PostgreSQL 17), Redis 7, SurrealDB (RocksDB)
**Monitoring**: Prometheus, Grafana
**Indexing**: Rindexer (blockchain indexer)

## References

- Main README: `README.md`
- PostgreSQL Writer Docs: `postgres-writer/README.md`
- Docker Compose: `docker/docker-compose.yml`
- SurrealDB Schema: `surreal-writer/migrations/surrealdb-schema.surql`
- PostgreSQL Migrations: `postgres-writer/migrations/`
