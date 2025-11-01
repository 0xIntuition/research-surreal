# Event Type Failures Runbook

## Overview

This runbook addresses alerts related to high failure rates for specific event types in the Redis-PostgreSQL sync pipeline.

## Related Alerts

- **AllEventsFailingForType** (Critical): >95% failure rate for an event type
- **HighEventTypeFailureRate** (Warning): >5% failure rate for an event type

## Symptoms

- High failure rate for a specific event type (AtomCreated, Deposited, SharePriceChanged, etc.)
- Events are being processed but failing during database operations or cascade updates
- Grafana dashboards show elevated failure metrics for specific event types

## Possible Causes

1. **Database Issues**
   - Database connection pool exhausted
   - Database deadlocks or lock timeouts
   - Database schema changes not compatible with event processing
   - Database performance degradation

2. **Data Quality Issues**
   - Invalid or malformed event data from Redis
   - Missing required fields in events
   - Data type mismatches

3. **Application Logic Issues**
   - Bug in event-type-specific processing logic
   - Error in cascade update logic
   - Transaction consistency issues (see TODO in postgres_client.rs:105-108)

4. **Resource Constraints**
   - Out of memory conditions
   - High CPU usage causing timeouts
   - Network issues between sync service and database

## Investigation Steps

1. **Check the specific event type failing**
   ```bash
   # View recent logs filtered by event type
   docker logs redis-postgres-sync --tail 1000 | grep "event_type"
   ```

2. **Check database connectivity and health**
   ```bash
   docker exec -it postgres psql -U intuition -d intuition -c "SELECT 1;"

   # Check for active locks
   docker exec -it postgres psql -U intuition -d intuition -c "
   SELECT pid, usename, pg_blocking_pids(pid) as blocked_by, query
   FROM pg_stat_activity
   WHERE cardinality(pg_blocking_pids(pid)) > 0;"
   ```

3. **Review Prometheus metrics**
   - Check `redis_postgres_sync_events_failed_by_type_total` for specific event type
   - Review `redis_postgres_sync:failure_rate_percent:by_type`
   - Look at `redis_postgres_sync_database_operations_total` for the event type

4. **Check application logs for stack traces**
   ```bash
   docker logs redis-postgres-sync --tail 500 | grep -A 20 "Error\|ERROR\|panic"
   ```

5. **Verify event data format**
   ```bash
   # Sample events from Redis for the failing event type
   docker exec -it redis redis-cli LRANGE events:queue 0 10
   ```

## Resolution Steps

### For Database Issues

1. **If connection pool exhausted:**
   - Increase `max_connections` in PostgreSQL config
   - Adjust pool size in application configuration
   - Restart the sync service

2. **If database locks detected:**
   - Identify and kill blocking queries if safe to do so
   - Consider adjusting transaction isolation level
   - Implement the TODO for single transaction (postgres_client.rs:105-108)

3. **If database performance degraded:**
   - Check database resource usage (CPU, memory, disk I/O)
   - Review slow query logs
   - Consider adding indexes if missing

### For Data Quality Issues

1. **Validate event schema:**
   - Check event structure matches expected format
   - Add validation logging if needed
   - Consider adding event schema validation

2. **Handle malformed events:**
   - Move problematic events to a dead letter queue
   - Fix upstream event generation if possible

### For Application Issues

1. **If it's a known bug:**
   - Apply hotfix or roll back to previous version
   - Monitor after deployment

2. **If it's a new issue:**
   - Collect stack traces and error messages
   - Create bug report with reproduction steps
   - Consider temporarily disabling processing for that event type if non-critical

## Escalation

Escalate to the on-call engineer if:
- Issue persists after following resolution steps
- Multiple event types are failing simultaneously
- Database shows signs of corruption or major issues
- Recovery steps cause additional problems

## Post-Incident

1. Document root cause in incident report
2. Add additional monitoring if gaps were identified
3. Update this runbook with new learnings
4. Consider implementing the transaction consistency TODO if related
5. Add unit tests to prevent regression

## Related Documentation

- [Database Operations](../architecture/database-operations.md)
- [Event Processing Pipeline](../architecture/event-pipeline.md)
- [Monitoring Overview](../monitoring/overview.md)
