# Slow Cascade Processing Runbook

## Overview

This runbook addresses the **SlowCascadeProcessing** alert, which triggers when the 95th percentile cascade processing time for a specific event type exceeds 2 seconds. Cascade processing refers to the database operations that update related entities after an event is processed.

## Alert Details

- **Alert Name**: SlowCascadeProcessing
- **Severity**: Warning
- **Threshold**: P95 cascade latency > 2 seconds for 5 minutes
- **Impact**: Increased overall event processing time, potential data inconsistencies

## Symptoms

- Cascade operations (database updates) taking longer than expected
- High cascade overhead percentage in Grafana dashboards
- Overall event processing time elevated due to cascade slowness
- Database write operations slow

## Cascade Operations by Event Type

Different event types trigger different cascade operations:

- **Deposited**: position_update, vault_aggregation, term_aggregation
- **SharePriceChanged**: vault_update, term_aggregation
- **AtomCreated**: term_initialization
- **Other events**: Various database updates and aggregations

## Possible Causes

1. **Database Performance Issues**
   - Slow UPDATE/INSERT queries
   - Lock contention on frequently updated tables (positions, vaults, terms)
   - Missing indexes on foreign keys
   - Table bloat requiring maintenance

2. **Transaction Issues**
   - Long transaction chains
   - Multiple separate transactions instead of batching (see postgres_client.rs:149-158)
   - Transaction rollbacks and retries
   - Deadlock detection and resolution overhead

3. **Data Volume**
   - Large number of dependent entities to update
   - Complex aggregation calculations
   - Cascade fan-out to many related records

4. **Consistency Issues**
   - Cascade failures after successful event commit (TODO at postgres_client.rs:105-108)
   - Retry logic causing duplicate work
   - Race conditions between concurrent updates

5. **Resource Constraints**
   - Database CPU/memory saturation
   - I/O bottlenecks
   - Network latency to database

## Investigation Steps

1. **Check cascade latency metrics**
   ```promql
   # P95 cascade duration by event type
   redis_postgres_sync:cascade_duration_seconds:p95:by_type

   # Cascade overhead as percentage of total processing
   redis_postgres_sync:cascade_overhead_percent:by_type
   ```

2. **Identify slow cascade operations**
   ```bash
   # Check logs for cascade-specific timing
   docker logs redis-postgres-sync --tail 1000 | grep -i "cascade"
   ```

3. **Check database operation metrics**
   ```promql
   # Database operations rate by type and operation
   redis_postgres_sync:database_operations_rate:by_type
   ```

4. **Analyze slow queries**
   ```bash
   # Check for slow UPDATE/INSERT operations
   docker exec -it postgres psql -U intuition -d intuition -c "
   SELECT query, mean_exec_time, calls, total_exec_time
   FROM pg_stat_statements
   WHERE query LIKE '%UPDATE%' OR query LIKE '%INSERT%'
   ORDER BY mean_exec_time DESC
   LIMIT 10;"
   ```

5. **Check for lock contention**
   ```bash
   docker exec -it postgres psql -U intuition -d intuition -c "
   SELECT blocked_locks.pid AS blocked_pid,
          blocked_activity.usename AS blocked_user,
          blocking_locks.pid AS blocking_pid,
          blocking_activity.usename AS blocking_user,
          blocked_activity.query AS blocked_statement
   FROM pg_catalog.pg_locks blocked_locks
   JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
   JOIN pg_catalog.pg_locks blocking_locks ON blocking_locks.locktype = blocked_locks.locktype
   JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
   WHERE NOT blocked_locks.granted
   AND blocked_locks.pid != blocking_locks.pid;"
   ```

6. **Review specific event type patterns**
   - For **Deposited**: Check position, vault, and term table performance
   - For **SharePriceChanged**: Check vault and term update performance
   - For **AtomCreated**: Check term initialization queries

## Resolution Steps

### For Database Performance Issues

1. **Add missing indexes on cascade target tables:**
   ```sql
   -- Example: If position updates are slow
   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_positions_vault_term
   ON positions(vault_id, term_id);
   ```

2. **Vacuum and analyze affected tables:**
   ```bash
   docker exec -it postgres psql -U intuition -d intuition -c "
   VACUUM ANALYZE positions;
   VACUUM ANALYZE vaults;
   VACUUM ANALYZE terms;"
   ```

3. **Review table statistics:**
   ```bash
   docker exec -it postgres psql -U intuition -d intuition -c "
   SELECT relname, n_live_tup, n_dead_tup,
          round(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2) AS dead_pct
   FROM pg_stat_user_tables
   WHERE n_dead_tup > 0
   ORDER BY dead_pct DESC;"
   ```

### For Transaction Issues

1. **Optimize transaction batching** (postgres_client.rs:149-158):
   - Batch counter_term_id lookups instead of N separate transactions
   - Reduce transaction scope where possible

2. **Implement single transaction for event + cascade** (TODO):
   - Address the TODO at postgres_client.rs:105-108
   - Ensures atomicity and reduces transaction overhead

3. **Check for deadlocks:**
   ```bash
   docker exec -it postgres psql -U intuition -d intuition -c "
   SELECT * FROM pg_stat_database WHERE datname = 'intuition';"
   # Check deadlocks column
   ```

### For Data Volume Issues

1. **Optimize aggregation queries:**
   - Pre-compute frequently accessed aggregations
   - Use materialized views if appropriate
   - Add partial indexes for common query patterns

2. **Consider async cascade operations:**
   - For non-critical cascades, queue for later processing
   - Implement eventual consistency where acceptable

### For Resource Constraints

1. **Scale database resources:**
   - Increase connection pool size if needed
   - Add read replicas for read-heavy cascades
   - Optimize database configuration (shared_buffers, work_mem)

2. **Monitor and adjust:**
   ```bash
   # Check current PostgreSQL settings
   docker exec -it postgres psql -U intuition -d intuition -c "
   SHOW shared_buffers;
   SHOW work_mem;
   SHOW maintenance_work_mem;"
   ```

## Monitoring

After applying fixes, monitor:
- Cascade P95 latency returning to <2s
- Cascade overhead percentage decreasing
- Database operation rates stabilizing
- No increase in cascade failures

Key metrics to watch:
```promql
redis_postgres_sync:cascade_duration_seconds:p95:by_type < 2
redis_postgres_sync:cascade_overhead_percent:by_type < 50
rate(redis_postgres_sync_database_operations_total[5m])
```

## Prevention

1. **Regular database maintenance:**
   - Scheduled VACUUM operations on cascade target tables
   - Monitor and address table bloat proactively
   - Review and optimize slow cascade queries

2. **Code improvements:**
   - Implement batch query optimization
   - Use single transactions for event + cascade
   - Add cascade failure tracking metrics

3. **Capacity planning:**
   - Monitor cascade latency trends
   - Load test with realistic cascade patterns
   - Scale database resources before hitting limits

4. **Monitoring enhancements:**
   - Add metrics for cascade failures after event success
   - Track cascade operation counts by type
   - Alert on cascade overhead > 80%

## Known Issues

- **Transaction consistency** (postgres_client.rs:105-108): Event insert and cascade updates use separate transactions, which can lead to cascades failing after successful event commits. Consider this when investigating cascade failures.

## Escalation

Escalate if:
- Cascade latency continues to increase despite optimizations
- Database shows signs of critical resource exhaustion
- Cascade failures start occurring (data consistency risk)
- Multiple event types show slow cascade processing

## Related Documentation

- [Slow Event Processing](slow-event-processing.md)
- [Event Type Failures](event-type-failures.md)
- [Database Operations](../architecture/database-operations.md)
