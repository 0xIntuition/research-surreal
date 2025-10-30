# Research Surreal

A high-performance blockchain event indexing and data pipeline system for the Intuition protocol, featuring real-time synchronization between Redis streams and SurrealDB with comprehensive monitoring.

## Overview

This project provides a complete data pipeline solution that:

1. **Indexes blockchain events** from the Intuition testnet MultiVault contract using Rindexer
2. **Streams events** to Redis for high-throughput processing
3. **Synchronizes data** to SurrealDB for persistent storage and querying
4. **Monitors pipeline** health with Prometheus and Grafana
5. **Visualizes data** through a Next.js dashboard (Jupiter)

## Architecture

```
┌───────────┐    ┌──────────┐    ┌──────────────┐    ┌───────────────────┐    ┌───────────┐
│ Blockchain│───▶│ Rindexer │───▶│Redis Streams │───▶│Redis-SurrealDB    │───▶│ SurrealDB │
└───────────┘    └──────────┘    └──────────────┘    │      Sync         │    └─────┬─────┘
                                                     └──────────┬────────┘          │
                                                                │                   │
                                                                ▼                   ▼
                                                       ┌─────────────┐    ┌──────────────────┐
                                                       │ Prometheus  │    │Jupiter Dashboard │
                                                       │  (Metrics)  │    └──────────────────┘
                                                       └─────┬───────┘
                                                             ▼
                                                       ┌──────────┐
                                                       │ Grafana  │
                                                       └──────────┘
```

## Components

### 1. Rindexer
- **Purpose**: Blockchain event indexer for Intuition testnet
- **Contract**: MultiVault (0x5a0A023F08dF301DCCE96166F4185Ec77DF6a87a)
- **Events**: AtomCreated, TripleCreated, Deposited, Redeemed
- **Output**: Redis streams for real-time processing

### 2. Redis-SurrealDB Sync (Rust)
- **Purpose**: High-performance event processor and data synchronizer
- **Features**:
  - Multi-stream Redis consumer with batching
  - Circuit breaker for reliability
  - Prometheus metrics integration
  - Health check endpoints
  - Graceful shutdown handling
- **Performance**: Configurable batch processing with 4 worker threads

### 3. SurrealDB
- **Purpose**: Modern multi-model database for data persistence
- **Features**:
  - RocksDB backend for performance
  - WebSocket and HTTP API support
  - Optimized configuration for production workloads
  - CORS and WebSocket middleware via Traefik

### 4. Jupiter Dashboard (Next.js)
- **Purpose**: Real-time data visualization dashboard
- **Features**:
  - Live atom count display
  - SurrealDB connection status
  - Responsive Tailwind CSS design
  - TypeScript implementation

### 5. Monitoring Stack
- **Prometheus**: Metrics collection and storage
- **Grafana**: Dashboard visualization with pre-configured panels
- **Alertmanager**: Alert management (configured but not detailed)

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
docker compose up -d
```

4. Access the services:
- Example web app: `http://localhost:18300/`
- Grafana: `http://localhost:18501/`
- Surrealist (SurrealDB IDE): `http://localhost:18301/`
- SurrealDB API: `http://localhost:18102/`
- Prometheus: `http://localhost:18500/`
- RedisInsight: `http://localhost:18400/`
- Rindexer: `http://localhost:18200/`

## Port Allocation Strategy

This project uses a structured port numbering scheme in the **18000-18999 range** to avoid conflicts with common development tools and other local services. The ports are organized by service type:

| Port Range | Service Type | Services |
|------------|--------------|----------|
| **18100-18199** | Databases | PostgreSQL (18100), Redis (18101), SurrealDB (18102) |
| **18200-18299** | Backend APIs | Rindexer (18200), Redis-Surreal Sync (18210), Redis-Postgres Sync (18211) |
| **18300-18399** | Frontend/UI | Jupiter Dashboard (18300), Surrealist (18301) |
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
- Pre-configured Redis-SurrealDB sync dashboard
- Automatic Prometheus data source provisioning
- Custom panels for pipeline monitoring

### Alerts
- Service health monitoring
- Processing lag detection
- Error rate thresholds

## Data Flow

1. **Blockchain Events**: MultiVault contract emits events on Intuition testnet
2. **Event Indexing**: Rindexer captures and processes events from block 2009988
3. **Stream Processing**: Events are pushed to Redis streams by category
4. **Batch Processing**: Redis-SurrealDB sync consumes and batches events
5. **Data Storage**: Processed events are stored in SurrealDB tables
6. **Visualization**: Jupiter dashboard queries SurrealDB for real-time metrics


## License

MIT License - see [LICENSE](LICENSE) file for details.

## Contributing

This is a research project for the Intuition protocol. Contributions should focus on performance optimization, monitoring improvements, and data pipeline reliability.

