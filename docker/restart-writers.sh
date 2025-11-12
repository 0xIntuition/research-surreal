#!/bin/bash

set -e

echo "🔄 Restarting writer services with fresh queues..."
echo ""

# Generate timestamp-based queue prefix suffix for performance testing
# This creates unique queues for each test run, allowing clean comparisons
# Example: postgres-20251111-143022.atom_created, surreal-20251111-143022.atom_created
TIMESTAMP=$(date +"%Y%m%d-%H%M%S")
export QUEUE_PREFIX_SUFFIX="-${TIMESTAMP}"

echo "📋 Queue Configuration:"
echo "   postgres queues: postgres${QUEUE_PREFIX_SUFFIX}.*"
echo "   surreal queues:  surreal${QUEUE_PREFIX_SUFFIX}.*"
echo ""
echo "💡 This creates fresh queues for performance testing."
echo "   Each run uses unique queues to avoid interference from previous test data."
echo ""

# Stop writer services first to ensure they get recreated
echo "🛑 Stopping writer services..."
docker compose stop postgres-writer surreal-writer

echo ""
# Stop and remove volumes for databases (postgres, surrealdb)
# RabbitMQ is kept running - exchanges will route to the new queues
echo "🗑️  Stopping databases and removing volumes..."
docker compose down -v postgres surrealdb

echo ""
echo "🔨 Starting databases first..."
# Start databases first and wait for them to be fully ready
docker compose up -d postgres surrealdb

echo ""
echo "🔨 Building and starting writer applications..."
# Force recreate writer apps by stopping and removing them first
# The QUEUE_PREFIX_SUFFIX env var will be used to create timestamped queue names
docker compose rm -f postgres-writer surreal-writer
docker compose up -d postgres-writer surreal-writer --build

echo ""
echo "✅ Writer services restarted successfully!"
echo "   Queue prefix suffix: ${QUEUE_PREFIX_SUFFIX}"
echo ""
echo "🎯 Writers will consume from fresh queues:"
echo "   - postgres${QUEUE_PREFIX_SUFFIX}.atom_created"
echo "   - postgres${QUEUE_PREFIX_SUFFIX}.triple_created"
echo "   - postgres${QUEUE_PREFIX_SUFFIX}.deposited"
echo "   - postgres${QUEUE_PREFIX_SUFFIX}.redeemed"
echo "   - postgres${QUEUE_PREFIX_SUFFIX}.share_price_changed"
echo "   (and equivalent surreal.* queues)"
echo ""
echo "📊 Check service status with:"
echo "   docker compose ps"
echo ""
echo "📝 View logs with:"
echo "   docker compose logs -f postgres-writer surreal-writer"
echo ""
echo "🧹 To clean up old test queues, use RabbitMQ Management UI:"
echo "   http://localhost:18102 (admin/admin)"
echo ""
