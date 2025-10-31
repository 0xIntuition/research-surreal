#!/bin/bash

set -e

echo "🔄 Restarting sync services..."
echo ""

# Generate timestamp-based consumer name and group suffix
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
export CONSUMER_GROUP_SUFFIX="sync-${TIMESTAMP}"
export CONSUMER_NAME="consumer-${TIMESTAMP}"

echo "📋 Using CONSUMER_GROUP_SUFFIX: ${CONSUMER_GROUP_SUFFIX}"
echo "📋 Using CONSUMER_NAME: ${CONSUMER_NAME}"
echo ""

# Stop sync services first to ensure they get recreated
echo "🛑 Stopping sync services..."
docker compose stop redis-postgres-sync redis-surreal-sync

echo ""
# Stop and remove volumes for databases (postgres, surrealdb)
# Redis is kept running to preserve stream data
echo "🗑️  Stopping databases and removing volumes..."
docker compose down -v postgres surrealdb

echo ""
echo "🔨 Starting databases first..."
# Start databases first and wait for them to be fully ready
docker compose up -d postgres surrealdb

echo ""
echo "🔨 Building and starting sync applications..."
# Force recreate sync apps with new consumer name by stopping and removing them first
docker compose rm -f redis-postgres-sync redis-surreal-sync
docker compose up -d redis-postgres-sync redis-surreal-sync --build

echo ""
echo "✅ Sync services restarted successfully!"
echo "   Consumer name: ${CONSUMER_NAME}"
echo ""
echo "📊 Check service status with:"
echo "   docker compose ps"
echo ""
echo "📝 View logs with:"
echo "   docker compose logs -f redis-postgres-sync redis-surreal-sync"
