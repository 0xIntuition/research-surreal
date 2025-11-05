# Research Backend

A high-performance blockchain event indexing and dual-database data pipeline system for the Intuition protocol, featuring real-time synchronization between Redis streams and both SurrealDB and PostgreSQL with comprehensive monitoring and analytics.

## Overview

This project provides a complete data pipeline solution that:

1. **Indexes blockchain events** from the Intuition testnet MultiVault contract using Rindexer
2. **Streams events** to Redis for high-throughput processing
3. **Synchronizes data** to dual storage backends:
   - **SurrealDB** for flexible NoSQL querying
   - **PostgreSQL** (TimescaleDB) for relational analytics and time-series data
4. **Provides analytics** through PostgreSQL triggers and Rust cascade processor for real-time aggregations
5. **Monitors pipeline** health with Prometheus and Grafana
6. **Visualizes data** through a Next.js dashboard

## Architecture

```
┌───────────┐    ┌──────────┐    ┌──────────────┐
│ Blockchain│───▶│ Rindexer │───▶│Redis Streams │
└───────────┘    └──────────┘    └──────┬───────┘
                                         │
                         ┌───────────────┴───────────────┐
                         ▼                               ▼
              ┌──────────────────┐          ┌──────────────────┐
              │  SurrealDB       │          │   PostgreSQL     │
              │    Writer        │          │     Writer       │
              └────────┬─────────┘          └────────┬─────────┘
                       ▼                             ▼
              ┌─────────────┐              ┌──────────────────┐
              │  SurrealDB  │              │   PostgreSQL     │
              │   (NoSQL)   │              │  (TimescaleDB)   │
              └──────┬──────┘              └────────┬─────────┘
                     │                              │
                     │                              ▼
                     │                    ┌──────────────────┐
                     │                    │  Materialized    │
                     │                    │     Views        │
                     │                    └────────┬─────────┘
                     └──────────┬─────────────────┘
                                ▼
                     ┌─────────────────────┐
                     │   Web Dashboard     │
                     └─────────────────────┘
                                │
                                ▼
                     ┌─────────────────────┐
                     │    Prometheus       │
                     │     (Metrics)       │
                     └──────────┬──────────┘
                                ▼
                     ┌─────────────────────┐
                     │      Grafana        │
                     └─────────────────────┘
```

## Components

### 1. Rindexer
- **Purpose**: Blockchain event indexer for Intuition testnet
- **Contract**: MultiVault (0x2Ece8D4dEdcB9918A398528f3fa4688b1d2CAB91)
- **Network**: Intuition Testnet (Chain ID: 13579)
- **Events**: AtomCreated, TripleCreated, Deposited, Redeemed, SharePriceChanged
- **Output**: Redis streams for real-time processing
- **Start Block**: 8092570
- **Port**: 18200 (GraphQL endpoint)

### 2. SurrealDB Writer (Rust)
- **Purpose**: High-performance event processor for NoSQL storage
- **Features**:
  - Multi-stream Redis consumer with batching (size: 20, interval: 100ms)
  - Circuit breaker for reliability
  - Prometheus metrics integration
  - Health check endpoints on port 18210
  - Graceful shutdown handling
  - Event-specific handlers for each blockchain event type
- **Performance**: Tokio async runtime with 4 configurable worker threads
- **Port**: 18210 (HTTP health/metrics)

### 3. PostgreSQL Writer (Rust)
- **Purpose**: High-performance event processor for relational analytics
- **Features**:
  - Event-driven architecture with Redis stream consumption
  - Async PostgreSQL integration via sqlx
  - Database migration management with trigger-based updates
  - Cascade processor for complex aggregations
  - Analytics worker for triple-level tables
  - Prometheus metrics integration
  - Health check endpoints on port 18211
  - Transaction-aware event storage
- **Performance**: Tokio async runtime with 4 configurable worker threads
- **Port**: 18211 (HTTP health/metrics)
- **Documentation**: See [postgres-writer/README.md](postgres-writer/README.md) and [postgres-writer/issues.md](postgres-writer/issues.md) for detailed architecture and known issues

### 4. SurrealDB
- **Purpose**: Multi-model NoSQL database for flexible querying
- **Features**:
  - RocksDB backend for performance
  - WebSocket and HTTP API support
  - Optimized configuration for production workloads
  - Accessed via Surrealist IDE
- **Port**: 18102 (HTTP/WebSocket API)

### 5. PostgreSQL (TimescaleDB)
- **Purpose**: Time-series relational database for analytics
- **Version**: PostgreSQL 17.5 via TimescaleDB image
- **Features**:
  - 13 database migrations (10 reference + 3 production)
  - Event tables: atom_created, triple_created, deposited, redeemed, share_price_changed
  - **Trigger-based updates** for real-time data synchronization:
    - Base tables: `atom`, `triple`, `position`, `vault` (updated by PostgreSQL triggers)
    - Aggregated tables: `term` (updated by Rust cascade processor)
    - Analytics tables: `triple_vault`, `triple_term`, `predicate_object`, `subject_predicate` (updated by analytics worker)
  - Composite indexes for performance
  - Advisory locks for preventing race conditions
- **Port**: 18100 (PostgreSQL protocol)

### 6. Web Dashboard (Next.js)
- **Purpose**: Real-time data visualization dashboard
- **Technology**: Next.js 15.1.3 with TypeScript
- **Features**:
  - Live atom count display
  - SurrealDB connection status
  - Responsive Tailwind CSS design
  - React 18 implementation
- **Port**: 18300 (HTTP via Traefik)

### 7. Admin & IDE Tools
- **Surrealist**: SurrealDB database IDE on port 18301
- **RedisInsight**: RedisDB admin UI on port 18400
- **Drizzle Studio**: PostgreSQL database IDE available at https://local.drizzle.studio/

### 8. Monitoring Stack
- **Prometheus**: Metrics collection (15s scrape interval) on port 18500
  - Configuration: `infrastructure/monitoring/prometheus.yml`
- **Grafana**: Dashboard visualization with pre-configured panels on port 18501
  - SurrealDB writer dashboard
  - PostgreSQL writer dashboard
  - Automatic data source provisioning
  - Configuration: `infrastructure/monitoring/grafana/`
- **Alertmanager**: Alert management (minimal configuration)
  - Configuration: `infrastructure/monitoring/alertmanager.yml`

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Environment variables (see Configuration section)

### Running the Stack

1. Clone the repository:
```bash
git clone https://github.com/0xIntuition/research-surreal.git 
cd research-surreal
```

2. Start all services:
```bash
cd docker
docker compose up -d
```

3. Access the services:

| Service | URL | Purpose |
|---------|-----|---------|
| **Web Dashboard** | http://localhost:18300/ | Real-time metrics visualization |
| **Grafana** | http://localhost:18501/ | Monitoring dashboards |
| **Surrealist** | http://localhost:18301/ | SurrealDB IDE |
| **RedisInsight** | http://localhost:18400/ | RedisDB admin interface |
| **Drizzle Studio** | https://local.drizzle.studio/ | PostgreSQL database IDE |
| **Prometheus** | http://localhost:18500/ | Metrics storage |
| **Rindexer** | http://localhost:18200/ | GraphQL API for indexed events |
| **SurrealDB API** | http://localhost:18102/ | Direct database access |
| **SurrealDB Writer Health** | http://localhost:18210/health | Health check endpoint |
| **PostgreSQL Writer Health** | http://localhost:18211/health | Health check endpoint |

## Port Allocation Strategy

This project uses a structured port numbering scheme in the **18000-18999 range** to avoid conflicts with common development tools and other local services. The ports are organized by service type:

| Port Range | Service Type | Services |
|------------|--------------|----------|
| **18100-18199** | Databases | PostgreSQL (18100), Redis (18101), SurrealDB (18102) |
| **18200-18299** | Backend APIs | Rindexer (18200), SurrealDB Writer (18210), PostgreSQL Writer (18211) |
| **18300-18399** | Frontend/UI | Web Dashboard (18300), Surrealist (18301) |
| **18400-18499** | Admin Tools | RedisInsight (18400) |
| **18500-18599** | Monitoring | Prometheus (18500), Grafana (18501) |

This scheme:
- Avoids conflicts with commonly used ports (3000, 5432, 6379, 8000, 8080, 9090)
- Groups related services logically for easy memorization
- Provides room for future expansion (100 ports per category)
- Makes it easy to identify service types by port number

## Monitoring

### Metrics Available
- Event processing rates and latencies
- Redis connection status
- SurrealDB query performance
- System resource utilization

### Grafana Dashboards
- Pre-configured SurrealDB writer dashboard
- Automatic Prometheus data source provisioning
- Custom panels for pipeline monitoring

### Alerts
- Service health monitoring
- Processing lag detection
- Error rate thresholds

## Data Flow

1. **Blockchain Events**: MultiVault contract emits events on Intuition testnet (starting from block 8092570)
2. **Event Indexing**: Rindexer captures and processes events via RPC
3. **Stream Processing**: Events are pushed to Redis streams by category:
   - `intuition_testnet_atom_created`
   - `intuition_testnet_triple_created`
   - `intuition_testnet_deposited`
   - `intuition_testnet_redeemed`
   - `intuition_testnet_share_price_changed`
4. **Dual-Path Processing**:
   - **SurrealDB Writer**: Consumes and batches events (20/batch, 100ms interval) → SurrealDB
   - **PostgreSQL Writer**: Consumes and batches events (20/batch, 5000ms timeout) → PostgreSQL
5. **Analytics Processing**: PostgreSQL triggers and Rust cascade processor compute real-time aggregations
6. **Data Storage**:
   - SurrealDB stores events in flexible NoSQL format
   - PostgreSQL stores events in relational tables with full transaction context
7. **Visualization**:
   - Web dashboard queries SurrealDB for real-time metrics
   - Grafana visualizes Prometheus metrics from both sync services
8. **Monitoring**: Prometheus collects metrics every 15 seconds from all services


## Technology Stack

### Backend
- **Language**: Rust (Edition 2021)
- **Async Runtime**: Tokio 1.41 (4 worker threads, configurable)
- **Redis**: redis 0.27 with Tokio streams
- **SurrealDB**: surrealdb 2.3.7 with WebSocket & HTTP
- **PostgreSQL**: sqlx 0.8 (async, migrations, TimescaleDB)
- **HTTP Server**: Warp 0.3
- **Metrics**: Prometheus client
- **Serialization**: serde, serde_json
- **Blockchain**: alloy-primitives for Ethereum types

### Frontend
- **Framework**: Next.js 15.1.3
- **Language**: TypeScript
- **UI**: React 18, Tailwind CSS 3.4.1
- **Database Client**: SurrealDB JS SDK

### Infrastructure
- **Container**: Docker with multi-stage builds, cargo-chef optimization
- **Orchestration**: Docker Compose with Dokploy integration
- **Databases**: PostgreSQL 17.5 (TimescaleDB), SurrealDB (RocksDB), Redis 7
- **Monitoring**: Prometheus, Grafana
- **Proxy**: Traefik (production)

## Performance Characteristics

### Batch Processing
- **SurrealDB Sync**: 20 events per batch, 100ms interval
- **PostgreSQL Sync**: 20 events per batch, 5000ms timeout (configurable)

### Concurrency
- 4 Tokio worker threads per sync service (configurable via `TOKIO_WORKER_THREADS`)
- Circuit breaker pattern for fault tolerance
- Configurable max retries (default: 3)

### Reliability
- Health check endpoints on all services
- Automatic reconnection with exponential backoff
- Graceful shutdown handling with signal catching
- Non-root container execution for security

## Development

### Project Structure

```
infrastructure/
├── drizzle/          # PostgreSQL database IDE configuration
├── monitoring/       # Prometheus, Grafana, and Alertmanager configs
└── redis/            # Redis configuration files
```

### Environment Variables

See `.env.example` files in:
- `surreal-writer/.env.example`
- `postgres-writer/.env.example`

Key configuration:
```bash
REDIS_URL=redis://redis:6379
SURREALDB_URL=ws://surrealdb:8000
DATABASE_URL=postgresql://user:pass@postgres:5432/dbname
BATCH_SIZE=100
WORKER_COUNT=4
MAX_RETRIES=3
```

### Local Development

Use `docker-compose.override.yml` for local development with port mappings in the 18xxx range.

### Database Migrations

**PostgreSQL migrations** (13 total: 10 reference + 3 production):
```bash
cd postgres-writer
sqlx migrate run
```

**SurrealDB schema**:
Located in `surreal-writer/migrations/surrealdb-schema.surql`

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Contributing

This is a research project for the Intuition protocol. Contributions should focus on:
- **Critical bug fixes**: See [postgres-writer/README.md](postgres-writer/README.md) for known issues
- Performance optimization
- Monitoring improvements
- Data pipeline reliability
- Analytics table enhancements
- Documentation and testing

**Before Contributing**:
1. Review the [postgres-writer documentation](postgres-writer/README.md) to understand the architecture
2. Check existing issues and planned improvements
3. Ensure changes don't introduce race conditions or data inconsistencies
4. Add tests for any bug fixes or new features
5. Update documentation to reflect changes
6. Run `cargo fmt` and `cargo clippy` before committing Rust code

