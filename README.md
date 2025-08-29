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
Blockchain → Rindexer → Redis Streams → Redis-SurrealDB Sync → SurrealDB
                                                                    ↓
                                          Jupiter Dashboard ← SurrealDB
                                                ↑
                                     Prometheus ← Metrics
                                                ↓
                                           Grafana
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
git clone <repository-url>
cd research-surreal
```

2. Start all services:
```bash
docker-compose up -d
```

4. Access the services:
- Jupiter Dashboard: `http://localhost:3033/`
- Grafana: `http://localhost:3009/`
- Surrealist: `http://localhost:3006/`



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

