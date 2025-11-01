# Slow Event Processing Runbook

## Overview

This runbook addresses the **SlowEventTypeProcessing** alert, which triggers when the 95th percentile processing time for a specific event type exceeds 5 seconds.

## Alert Details

- **Alert Name**: SlowEventTypeProcessing
- **Severity**: Warning
- **Threshold**: P95 latency > 5 seconds for 5 minutes
- **Impact**: Increased event processing lag, potential backlog buildup

## Symptoms

- Events are being processed successfully but taking longer than expected
- Grafana dashboards show elevated P95/P99 latency for specific event types
- Redis queue length may be increasing
- No significant failure rate increase

## Possible Causes

1. **Database Performance Issues**
   - Slow queries due to missing indexes
   - Database resource saturation (CPU, memory, I/O)
   - Long-running transactions blocking queries
   - Table bloat requiring vacuuming

2. **Network Issues**
   - High latency between sync service and database
   - Network congestion or packet loss
   - DNS resolution delays

3. **Application Issues**
   - Inefficient query patterns (N+1 queries)
   - Large transaction sizes
   - Memory pressure causing garbage collection pauses
   - Unnecessary database queries in hot path (see postgres_client.rs:149-158)

4. **Resource Contention**
   - High CPU usage on sync service
   - Memory pressure or swapping
   - Disk I/O saturation
   - Too many concurrent operations

5. **Data Volume**
   - Unusually large events requiring more processing
   - Spike in event volume overwhelming capacity
   - Complex cascade operations for certain event types

## Investigation Steps

1. **Check current processing latency**
   ```promql
   # Query Prometheus for P95 latency by event type
   redis_postgres_sync:event_processing_duration_seconds:p95:by_type
   ```

2. **Identify which event type is slow**
   ```bash
   # Check logs for slow operations
   docker logs redis-postgres-sync --tail 1000 | grep -i "slow\|duration"
   ```

3. **Check database query performance**
   ```bash
   # Check for slow queries in PostgreSQL
   docker exec -it postgres psql -U intuition -d intuition -c "
   SELECT query, mean_exec_time, calls
   FROM pg_stat_statements
   ORDER BY mean_exec_time DESC
   LIMIT 10;"
   ```

4. **Check database resource utilization**
   ```bash
   # Check database stats
   docker exec -it postgres psql -U intuition -d intuition -c "
   SELECT datname,
          pg_size_pretty(pg_database_size(datname)) as size,
          (SELECT count(*) FROM pg_stat_activity WHERE datname = d.datname) as connections
   FROM pg_database d;"
   ```

5. **Check application resource usage**
   ```bash
   docker stats redis-postgres-sync --no-stream
   ```

6. **Review cascade operation metrics**
   ```promql
   # Check if cascade processing is contributing to slowness
   redis_postgres_sync:cascade_overhead_percent:by_type
   redis_postgres_sync:cascade_duration_seconds:p95:by_type
   ```

## Resolution Steps

### For Database Performance Issues

1. **Identify missing indexes:**
   ```bash
   # Check for sequential scans on large tables
   docker exec -it postgres psql -U intuition -d intuition -c "
   SELECT schemaname, tablename, seq_scan, idx_scan,
          seq_scan - idx_scan AS too_much_seq
   FROM pg_stat_user_tables
   WHERE seq_scan - idx_scan > 0
   ORDER BY too_much_seq DESC
   LIMIT 10;"
   ```

2. **Add indexes if needed** (after analysis)

3. **Vacuum tables if bloated:**
   ```bash
   docker exec -it postgres psql -U intuition -d intuition -c "VACUUM ANALYZE;"
   ```

4. **Kill long-running queries if safe:**
   ```bash
   docker exec -it postgres psql -U intuition -d intuition -c "
   SELECT pg_terminate_backend(pid)
   FROM pg_stat_activity
   WHERE state = 'active' AND query_start < now() - interval '5 minutes';"
   ```

### For Application Issues

1. **Optimize batching** (postgres_client.rs:149-158):
   - Implement batch query optimization
   - Reduce number of separate transactions

2. **Reduce transaction scope:**
   - Implement single transaction for event + cascade (TODO at postgres_client.rs:105-108)

3. **Check for memory issues:**
   ```bash
   # Restart service if memory leak suspected
   docker restart redis-postgres-sync
   ```

### For Resource Contention

1. **Scale horizontally** (if architecture supports):
   - Add more sync service instances
   - Partition event processing by type

2. **Increase resource limits:**
   ```yaml
   # In docker-compose.yml, adjust resources
   resources:
     limits:
       cpus: '2'
       memory: 2G
   ```

3. **Reduce concurrent operations** if overwhelming database

## Monitoring

After applying fixes, monitor:
- P95/P99 latency trends returning to normal (<5s)
- Queue depth stabilizing or decreasing
- Resource utilization within acceptable ranges
- No increase in failure rates

## Prevention

1. **Regular database maintenance:**
   - Schedule regular VACUUM operations
   - Monitor table bloat
   - Review and optimize slow queries

2. **Performance testing:**
   - Load test with realistic event volumes
   - Benchmark individual event type processing
   - Identify performance bottlenecks proactively

3. **Code optimization:**
   - Implement batch query optimization (postgres_client.rs:149-158)
   - Use connection pooling effectively
   - Profile hot paths regularly

## Escalation

Escalate if:
- Latency continues to increase despite interventions
- Multiple event types become slow simultaneously
- Database or application shows signs of critical resource exhaustion
- Event backlog grows to unmanageable levels

## Related Documentation

- [Slow Cascade Processing](slow-cascade-processing.md)
- [Event Type Failures](event-type-failures.md)
- [Performance Tuning Guide](../guides/performance-tuning.md)
