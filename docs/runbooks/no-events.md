# No Events Processed Runbook

## Overview

This runbook addresses the **NoEventsProcessedForType** alert, which triggers when no events of a specific type have been processed in the last 10 minutes. This may be expected for rare event types or indicate a problem with event ingestion or processing.

## Alert Details

- **Alert Name**: NoEventsProcessedForType
- **Severity**: Info
- **Threshold**: No events processed for 10 minutes
- **Impact**: Potential data staleness, missed events

## Expected Behavior

Some event types are naturally infrequent and this alert may be expected:
- Rare blockchain events that occur sporadically
- Administrative or configuration events
- Events that only occur during specific system states

Always verify if the event type *should* be occurring before treating this as an incident.

## Possible Causes

### 1. Expected Silence (Not an Issue)
- Event type is naturally rare or infrequent
- No on-chain activity generating this event type
- System in idle state with no relevant events

### 2. Event Ingestion Issues
- Redis queue not receiving events from source
- Event producer (e.g., rindexer) stopped or failing
- Network issues between event source and Redis
- Event source (blockchain node) connection issues

### 3. Event Filter/Routing Issues
- Events being filtered out upstream
- Event type mapping or classification issues
- Wrong event type labels being applied

### 4. Processing Issues
- Sync service stopped or restarted
- Events stuck in queue for this specific type
- Processing logic failing silently for this event type
- Queue corruption or Redis issues

## Investigation Steps

1. **Determine if silence is expected**
   ```bash
   # Check historical event frequency for this type
   # Query Prometheus for historical data
   ```
   ```promql
   # Check event rate over last 24 hours
   rate(postgres_writer_events_processed_by_type_total{event_type="<TYPE>"}[24h])
   ```

2. **Check if events are in Redis queue**
   ```bash
   # Check Redis queue length
   docker exec -it redisdb redis-cli LLEN events:queue

   # Sample events to see types present
   docker exec -it redisdb redis-cli LRANGE events:queue 0 100 | grep -i "<event_type>"
   ```

3. **Check event producer status**
   ```bash
   # Check if rindexer (or other event producer) is running
   docker ps | grep rindexer

   # Check rindexer logs for errors
   docker logs rindexer --tail 100
   ```

4. **Check sync service status**
   ```bash
   # Verify sync service is running
   docker ps | grep postgres-writer

   # Check for errors in logs
   docker logs postgres-writer --tail 100 | grep -i "error\|fail"
   ```

5. **Check blockchain/event source connectivity**
   ```bash
   # If using RPC endpoint, test connectivity
   curl -X POST <RPC_ENDPOINT> \
     -H "Content-Type: application/json" \
     -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'
   ```

6. **Review failure metrics for this event type**
   ```promql
   # Check if events are failing instead of being processed
   rate(postgres_writer_events_failed_by_type_total{event_type="<TYPE>"}[10m])
   ```

## Resolution Steps

### For Event Ingestion Issues

1. **Restart event producer if needed:**
   ```bash
   docker restart rindexer
   ```

2. **Check event producer configuration:**
   - Verify contract addresses
   - Verify event signatures
   - Check RPC endpoint configuration

3. **Test event source connectivity:**
   - Verify blockchain node is responsive
   - Check network connectivity
   - Verify authentication/API keys if required

### For Processing Issues

1. **Check Redis health:**
   ```bash
   docker exec -it redisdb redis-cli PING
   # Should return PONG

   # Check Redis memory usage
   docker exec -it redisdb redis-cli INFO memory
   ```

2. **Restart sync service if needed:**
   ```bash
   docker restart postgres-writer
   ```

3. **Check for specific event type failures:**
   - Review logs for errors related to this event type
   - Check if database operations are failing
   - Verify database schema supports this event type

### For Filter/Routing Issues

1. **Verify event type mapping:**
   - Check event type definitions in code
   - Verify event type labels are correct
   - Review any filtering or routing logic

2. **Check for configuration changes:**
   - Review recent config changes
   - Verify event type is not excluded/disabled

## Monitoring

After investigation/resolution:

1. **Watch for event resumption:**
   ```promql
   rate(postgres_writer_events_processed_by_type_total{event_type="<TYPE>"}[5m])
   ```

2. **Verify normal processing:**
   - Check event processing latency is normal
   - Verify no increase in failure rate
   - Monitor queue depth returning to baseline

## Common Event Types and Expected Frequencies

| Event Type | Expected Frequency | Notes |
|------------|-------------------|-------|
| AtomCreated | Varies | Depends on user activity |
| Deposited | High | Frequent user deposits |
| SharePriceChanged | Medium | Periodic vault updates |
| TripleCreated | Varies | User-initiated |
| AccountCreated | Low-Medium | New user signups |

*Note: Update this table based on your system's actual event patterns*

## False Positive Scenarios

This alert may be informational only in these cases:
- Event type is known to be rare
- System is in maintenance mode
- Off-peak hours with low activity
- New event type recently added with no occurrences yet

## Escalation

Escalate if:
- Multiple event types simultaneously stop being processed
- Event producer is failing and cannot be restarted
- Critical event types (Deposited, Withdrawn) stop processing
- Events are in queue but not being processed
- Issue persists after standard troubleshooting

## Prevention

1. **Better event frequency monitoring:**
   - Set different thresholds for different event types
   - Track expected vs actual event rates
   - Alert on deviations from historical patterns

2. **Improved observability:**
   - Add metrics for event producer health
   - Track event ingestion pipeline end-to-end
   - Monitor blockchain node/RPC connectivity

3. **Alerting improvements:**
   - Use `unless` clauses to exclude known-rare event types
   - Set event-type-specific thresholds
   - Add context about expected frequency to alerts

## Related Documentation

- [Event Type Failures](event-type-failures.md)
- [Event Processing Architecture](../architecture/event-pipeline.md)
- [Redis Queue Management](../guides/redis-operations.md)
