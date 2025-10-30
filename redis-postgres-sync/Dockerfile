# Multi-stage Docker build for Redis-PostgreSQL sync pipeline
# Production-ready build with Debian bookworm-slim runtime
# Optimized for Docker layer caching using cargo-chef

# Chef stage - Install cargo-chef on nightly Rust for edition2024
FROM rustlang/rust:nightly-bookworm AS chef

# Install build dependencies and security updates
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install cargo-chef for dependency caching
RUN cargo install cargo-chef --locked

# Set working directory
WORKDIR /app

# Planner stage - Generate dependency recipe
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Builder stage - Use cargo-chef for optimal dependency caching
FROM chef AS builder

# Set build environment variables for optimal performance
ENV CARGO_INCREMENTAL=0
ENV CARGO_NET_RETRY=10
ENV RUSTUP_MAX_RETRIES=10
ENV RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C link-arg=-s"

# Copy recipe from planner stage
COPY --from=planner /app/recipe.json recipe.json

# Build dependencies - this layer will be cached unless dependencies change
# This is the key optimization that cargo-chef provides
RUN cargo chef cook --release --recipe-path recipe.json

# Copy actual source code
COPY . .

# Build the actual application - this layer rebuilds only when source code changes
# Dependencies are already compiled and cached from the previous layer
RUN cargo build --release --bin redis-postgres-sync

# Verify binary works
RUN ./target/release/redis-postgres-sync --version || echo "Binary verification skipped"

# Runtime stage - Use Debian bookworm-slim for compatibility
FROM debian:bookworm-slim

# Install runtime dependencies and security updates
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user with specific UID/GID for security
RUN groupadd -r -g 1001 sync || true && \
    useradd -r -u 1001 -g sync -d /app -s /bin/bash sync || true

# Create app directory and set ownership
WORKDIR /app
RUN chown sync:sync /app

# Copy binary from builder stage with correct permissions
COPY --from=builder --chown=sync:sync /app/target/release/redis-postgres-sync /usr/local/bin/redis-postgres-sync
RUN chmod +x /usr/local/bin/redis-postgres-sync

# Copy migrations
COPY --from=builder --chown=sync:sync /app/migrations /app/migrations

# Create log directory
RUN mkdir -p /app/logs && chown sync:sync /app/logs

# Switch to non-root user
USER sync

# Set security-focused environment variables
ENV RUST_LOG=redis_postgres_sync=info,tokio=warn
ENV RUST_BACKTRACE=1
ENV REDIS_URL=redis://redis:6379
ENV DATABASE_URL=postgresql://postgres:postgres@postgres:5432/storage

# Performance and reliability settings
ENV TOKIO_WORKER_THREADS=4
ENV BATCH_SIZE=20
ENV BATCH_TIMEOUT_MS=5000
ENV PROCESSING_INTERVAL_MS=100
ENV RECONNECT_DELAY_MS=5000
ENV MAX_RETRIES=3
ENV HTTP_PORT=8081

# Health check with proper timeout and retries
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD curl -f http://localhost:8081/health 2>/dev/null || exit 1

# Set default command with signal handling
ENTRYPOINT ["redis-postgres-sync"]

# Labels for metadata and security scanning
LABEL maintainer="Simonas" \
      version="0.1.0" \
      description="High-performance Redis to PostgreSQL event synchronization pipeline" \
      org.opencontainers.image.source="https://github.com/your-org/redis-postgres-sync" \
      org.opencontainers.image.description="Production-ready Redis streams to PostgreSQL synchronization service" \
      org.opencontainers.image.version="0.1.0" \
      security.capabilities="NET_BIND_SERVICE" \
      security.non-root="true"
