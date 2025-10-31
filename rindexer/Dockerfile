FROM --platform=linux/amd64 debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    bash \
    tar \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Install rindexer version 0.28.1 (amd64 only - no ARM64 build available)
RUN mkdir -p /root/.rindexer/bin && \
    curl -L "https://github.com/joshstevens19/rindexer/releases/download/v0.28.1/rindexer_linux-amd64.tar.gz" \
        -o /tmp/rindexer.tar.gz && \
    tar -xzf /tmp/rindexer.tar.gz -C /tmp && \
    mv /tmp/rindexer_cli /root/.rindexer/bin/rindexer && \
    chmod +x /root/.rindexer/bin/rindexer && \
    rm -f /tmp/rindexer.tar.gz

# Make rindexer available in PATH
ENV PATH="/root/.rindexer/bin:${PATH}"

# Set up working directory
WORKDIR /app

# Set rindexer as entrypoint
ENTRYPOINT ["rindexer"]

# Configuration and ABI files will be mounted via volumes
