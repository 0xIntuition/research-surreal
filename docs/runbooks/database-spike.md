# Database Operation Spike Runbook

## Overview

This runbook addresses the **DatabaseOperationSpike** alert, which triggers when the rate of database operations for a specific event type and operation exceeds 2x the 1-hour average for 5 minutes. This indicates an unusual increase in database activity.

## Alert Details

- **Alert Name**: DatabaseOperationSpike
- **Severity**: Info
- **Threshold**: Database operation rate > 2x 1-hour average for 5 minutes
- **Impact**: Increased database load, potential performance degradation

## Database Operations by Event Type

Different event types perform different database operations:

- **Deposited**:
  - `position_update`: Updates user positions
  - `vault_aggregation`: Aggregates vault metrics
  - `term_aggregation`: Aggregates term metrics

- **SharePriceChanged**:
  - `vault_update`: Updates vault share price
  - `term_aggregation`: Aggregates term metrics

- **AtomCreated**:
  - `term_initialization`: Initializes new term

- **Other event types**: Various operations depending on event logic

## Possible Causes

### 1. Legitimate Activity Spikes (Not an Issue)
- Sudden increase in user activity
- Large batch of events processed after downtime
- Legitimate viral activity or campaign
- Expected periodic batch operations

### 2. Event Volume Increase
- Unusual on-chain activity
- Event replay or reprocessing
- Backlog processing after service restart
- Multiple similar events in quick succession

### 3. Performance Issues
- Inefficient query causing multiple operations
- Loop executing unexpected iterations
- Retry logic triggering excessive operations
- N+1 query problem (postgres_client.rs:149-158)

### 4. Data Issues
- Events requiring more cascade operations than usual
- Large fan-out to many dependent entities
- Complex aggregations with many related records

### 5. Application Bugs
- Duplicate event processing
- Infinite loop or recursion
- Logic error causing extra database calls
- Missing caching or memoization

## Investigation Steps

1. **Check current operation rates**
   ```promql
   # Current rate vs baseline
   postgres_writer:database_operations_rate:by_type

   # Historical comparison
   avg_over_time(postgres_writer:database_operations_rate:by_type[1h])
   ```

2. **Identify affected event type and operation**
   ```bash
   # Check logs for the specific event type and operation
   docker logs postgres-writer --tail 500 | grep -i "<event_type>\|<operation>"
   ```

3. **Check event processing rate**
   ```promql
   # Event processing rate for the affected type
   rate(postgres_writer_events_processed_by_type_total{event_type="<TYPE>"}[5m])
   ```

4. **Verify if it's proportional to event volume**
   - If events increased 2x, database operations should also increase ~2x
   - If operations increased disproportionately, investigate why

5. **Check for errors or retries**
   ```bash
   # Look for retry patterns
   docker logs postgres-writer --tail 1000 | grep -i "retry\|error\|fail"
   ```

6. **Review database performance impact**
   ```bash
   # Check database load
   docker exec -it postgres psql -U intuition -d intuition -c "
   SELECT count(*), state
   FROM pg_stat_activity
   GROUP BY state;"

   # Check slow queries
   docker exec -it postgres psql -U intuition -d intuition -c "
   SELECT query, calls, mean_exec_time
   FROM pg_stat_statements
   ORDER BY calls DESC
   LIMIT 10;"
   ```

7. **Check for duplicate processing**
   ```bash
   # Look for duplicate event IDs in logs
   docker logs postgres-writer --tail 1000 | grep "event_id" | sort | uniq -d
   ```

## Resolution Steps

### For Legitimate Activity Spikes

1. **Verify it's expected:**
   - Check if marketing campaign or feature launch is ongoing
   - Verify on-chain activity matches event volume
   - Confirm with team if increase is expected

2. **Monitor capacity:**
   - Ensure database can handle the load
   - Watch for performance degradation
   - Scale resources if needed

3. **No action needed if:**
   - Event volume increase is proportional
   - Database performance remains healthy
   - Operations complete successfully

### For Performance Issues

1. **Optimize N+1 queries** (postgres_client.rs:149-158):
   ```bash
   # If counter_term_id lookups are causing spike
   # This needs code optimization to batch queries
   ```

2. **Review cascade operations:**
   - Check if cascades are creating more work than expected
   - Optimize aggregation queries
   - Consider caching frequently accessed data

3. **Add query result caching:**
   - Cache counter_term_id lookups
   - Memoize frequently called functions
   - Use Redis for temporary caching if appropriate

### For Application Bugs

1. **Check for duplicate processing:**
   - Review event deduplication logic
   - Verify event IDs are unique
   - Check if events are being reprocessed

2. **Review loop logic:**
   - Verify iterations match expected count
   - Check loop termination conditions
   - Look for accidental recursion

3. **Deploy hotfix if bug identified:**
   - Roll back if recent deployment introduced issue
   - Deploy fix for identified bug
   - Monitor after deployment

### For Data Issues

1. **Investigate outlier events:**
   - Identify events causing excessive operations
   - Check if data is malformed or unusual
   - Validate cascade logic for edge cases

2. **Optimize aggregation logic:**
   - Limit fan-out where appropriate
   - Use more efficient aggregation methods
   - Consider async processing for expensive operations

## Monitoring

After investigation/resolution, monitor:

1. **Operation rate stabilization:**
   ```promql
   postgres_writer:database_operations_rate:by_type
   ```

2. **Database performance:**
   - Query execution times
   - Connection pool usage
   - Resource utilization (CPU, memory, I/O)

3. **Event processing health:**
   - Processing latency remains normal
   - No increase in failure rates
   - Queue depth stable

## Thresholds by Operation Type

Some operations naturally have different baseline rates:

| Operation | Typical Rate | Spike Threshold | Notes |
|-----------|-------------|-----------------|-------|
| position_update | High | >2x average | Common for Deposited events |
| vault_aggregation | Medium | >2x average | Triggered by various events |
| term_aggregation | Medium-High | >2x average | Multiple events trigger this |
| vault_update | Medium | >2x average | SharePriceChanged events |
| term_initialization | Low | >2x average | Only AtomCreated events |

*Note: Adjust thresholds based on observed patterns*

## Prevention

1. **Query optimization:**
   - Implement batch query optimization (postgres_client.rs:149-158)
   - Reduce unnecessary database round trips
   - Use connection pooling effectively

2. **Better monitoring:**
   - Track operation-to-event ratios
   - Alert on unusual ratios (e.g., >10 ops per event)
   - Monitor query efficiency metrics

3. **Code review focus:**
   - Review database operations in loops
   - Check for proper batching
   - Ensure efficient cascade patterns

4. **Capacity planning:**
   - Load test with realistic spike scenarios
   - Plan for 5-10x normal load capacity
   - Set up auto-scaling if possible

## When to Escalate

Escalate if:
- Database performance significantly degraded
- Operations spike is unexplained and persistent
- Multiple operation types spiking simultaneously
- Application logic appears to be in infinite loop
- Database resources approaching limits

## Related Documentation

- [Slow Cascade Processing](slow-cascade-processing.md)
- [Slow Event Processing](slow-event-processing.md)
- [Database Operations Architecture](../architecture/database-operations.md)
- [Performance Tuning Guide](../guides/performance-tuning.md)
