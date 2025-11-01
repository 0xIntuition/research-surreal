# Monitoring Infrastructure

This directory contains the monitoring configuration for the Redis-PostgreSQL sync service using Prometheus, Alertmanager, and Grafana.

## Overview

The monitoring stack provides:
- **Metrics collection** via Prometheus
- **Alerting** via Alertmanager
- **Visualization** via Grafana dashboards
- **Event-type-specific monitoring** for granular observability

## Components

### Prometheus

Prometheus scrapes metrics from the sync service and evaluates recording rules and alert rules.

**Configuration files:**
- `prometheus.yml` - Main configuration
- `recording_rules.yml` - Pre-computed metrics for performance
- `alert_rules.yml` - Alert definitions

### Alertmanager

Alertmanager handles alert routing, grouping, and notification delivery.

**Configuration file:** `alertmanager.yml`

⚠️ **IMPORTANT: Receivers must be configured before alerts will be delivered!**

Currently, all receivers are placeholders with no actual notification methods configured. This means **alerts will be silently dropped** until you configure at least one receiver.

### Grafana

Grafana provides visualization dashboards for metrics.

**Dashboards:**
- `grafana/dashboards/redis-postgres-sync-dashboard.json` - Main monitoring dashboard

## Alertmanager Receiver Configuration

### Current State

The `alertmanager.yml` file includes four receivers:
1. `default-receiver` - Catches all unmatched alerts (currently empty)
2. `critical-alerts` - For critical severity alerts (currently empty)
3. `warning-alerts` - For warning severity alerts (currently empty)
4. `info-alerts` - For info severity alerts (currently empty)
5. `web.hook` - Legacy webhook (active, sends to http://localhost:5001/)

**All receivers except `web.hook` are NO-OPs** - they receive alerts but don't send notifications anywhere.

### Configuration Requirements

To receive alert notifications, you must configure at least one receiver. Here are common options:

#### Option 1: Slack Integration

1. **Create a Slack Incoming Webhook:**
   - Go to https://api.slack.com/messaging/webhooks
   - Create an app and enable incoming webhooks
   - Copy the webhook URL

2. **Update `alertmanager.yml`:**

```yaml
receivers:
  - name: 'critical-alerts'
    slack_configs:
      - api_url: 'https://hooks.slack.com/services/YOUR/WEBHOOK/URL'
        channel: '#critical-alerts'
        title: 'CRITICAL: {{ .GroupLabels.alertname }}'
        text: |
          {{ range .Alerts }}
          *Event Type:* {{ .Labels.event_type }}
          *Summary:* {{ .Annotations.summary }}
          *Description:* {{ .Annotations.description }}
          *Runbook:* {{ .Annotations.runbook_url }}
          {{ end }}
        send_resolved: true
```

3. **Repeat for other severity levels** (warning-alerts, info-alerts) with different channels if desired.

#### Option 2: PagerDuty Integration

1. **Get PagerDuty Service Integration Key:**
   - In PagerDuty, go to Services → Your Service → Integrations
   - Add "Prometheus" integration
   - Copy the Integration Key

2. **Update `alertmanager.yml`:**

```yaml
receivers:
  - name: 'critical-alerts'
    pagerduty_configs:
      - service_key: 'YOUR_PAGERDUTY_INTEGRATION_KEY'
        description: '{{ .GroupLabels.alertname }}: {{ .GroupLabels.event_type }}'
        details:
          summary: '{{ range .Alerts }}{{ .Annotations.summary }}{{ end }}'
          severity: '{{ .GroupLabels.severity }}'
          event_type: '{{ .GroupLabels.event_type }}'
```

#### Option 3: Email Notifications

1. **Configure SMTP settings in `global` section:**

```yaml
global:
  smtp_smarthost: 'smtp.gmail.com:587'
  smtp_from: 'alertmanager@yourdomain.com'
  smtp_auth_username: 'your-email@gmail.com'
  smtp_auth_password: 'your-app-password'
  smtp_require_tls: true
```

2. **Configure email receiver:**

```yaml
receivers:
  - name: 'critical-alerts'
    email_configs:
      - to: 'oncall@yourdomain.com'
        from: 'alertmanager@yourdomain.com'
        subject: 'CRITICAL ALERT: {{ .GroupLabels.alertname }}'
        html: |
          <h2>{{ .GroupLabels.alertname }}</h2>
          {{ range .Alerts }}
          <p><strong>Event Type:</strong> {{ .Labels.event_type }}</p>
          <p><strong>Summary:</strong> {{ .Annotations.summary }}</p>
          <p><strong>Description:</strong> {{ .Annotations.description }}</p>
          <p><a href="{{ .Annotations.runbook_url }}">View Runbook</a></p>
          {{ end }}
```

#### Option 4: Webhook Integration

For custom integrations:

```yaml
receivers:
  - name: 'critical-alerts'
    webhook_configs:
      - url: 'https://your-webhook-endpoint.com/alerts'
        send_resolved: true
        http_config:
          # Optional: Add authentication
          basic_auth:
            username: 'webhook_user'
            password: 'webhook_password'
```

#### Option 5: Stdout Logging (Development/Testing)

For development, you can configure Alertmanager to log alerts:

```yaml
receivers:
  - name: 'default-receiver'
    webhook_configs:
      - url: 'http://localhost:5001/debug'
        send_resolved: true
```

Then run a simple webhook receiver:
```bash
# Simple webhook receiver for testing
python3 -m http.server 5001
```

### Recommended Configuration

For production use, we recommend:

1. **Critical alerts** → PagerDuty (or equivalent on-call system)
2. **Warning alerts** → Slack (team channel)
3. **Info alerts** → Slack (monitoring channel, muted)
4. **Default receiver** → Email or webhook for catchall

### Testing Your Configuration

After configuring receivers:

1. **Validate configuration:**
```bash
docker exec -it alertmanager amtool check-config /etc/alertmanager/alertmanager.yml
```

2. **Reload configuration:**
```bash
docker exec -it alertmanager kill -HUP 1
# or restart the container
docker restart alertmanager
```

3. **Send a test alert:**
```bash
curl -X POST http://localhost:9093/api/v1/alerts -d '[{
  "labels": {
    "alertname": "TestAlert",
    "severity": "warning",
    "event_type": "test"
  },
  "annotations": {
    "summary": "Test alert",
    "description": "This is a test alert to verify receiver configuration"
  }
}]'
```

4. **Check Alertmanager UI:**
   - Open http://localhost:9093
   - Verify the test alert appears
   - Check that it was sent to configured receivers

## Alert Routing

Alerts are routed based on severity:

- **Critical** (`severity: critical`):
  - Sent immediately (no grouping delay)
  - Repeats every 5 minutes until resolved
  - Use for issues requiring immediate attention

- **Warning** (`severity: warning`):
  - Grouped with 30s delay
  - Repeats every 1 hour
  - Use for issues that need attention but aren't urgent

- **Info** (`severity: info`):
  - Grouped with 5m delay
  - Repeats every 24 hours
  - Use for informational notifications

### Legacy Webhook

The `web.hook` receiver at http://localhost:5001/ is configured with `continue: true`, meaning alerts will **always** be sent here in addition to the severity-based receiver.

**To disable:**
Remove or comment out the legacy webhook route in `alertmanager.yml`:
```yaml
# routes:
#   - receiver: 'web.hook'
#     continue: true
```

## Available Alerts

| Alert Name | Severity | Threshold | Description |
|------------|----------|-----------|-------------|
| AllEventsFailingForType | Critical | >95% failure rate | Nearly all events of a type are failing |
| HighEventTypeFailureRate | Warning | >5% failure rate | Elevated failure rate for event type |
| SlowEventTypeProcessing | Warning | P95 > 5s | Event processing is slow |
| SlowCascadeProcessing | Warning | P95 > 2s | Cascade updates are slow |
| NoEventsProcessedForType | Info | 0 events for 10m | No events processed (may be expected) |
| DatabaseOperationSpike | Info | >2x average | Unusual increase in database operations |

See individual runbooks in `docs/runbooks/` for detailed information on each alert.

## Metrics

### Event Type Metrics

- `redis_postgres_sync_events_processed_by_type_total` - Events successfully processed
- `redis_postgres_sync_events_failed_by_type_total` - Events that failed
- `redis_postgres_sync_cascade_failures_total` - Cascade failures after successful event commit
- `redis_postgres_sync_event_processing_duration_by_type_seconds` - Processing latency
- `redis_postgres_sync_cascade_processing_duration_seconds` - Cascade latency
  - **Note**: Measures ONLY cascade update queries (vault/term aggregations), excludes Redis publishing
- `redis_postgres_sync_database_operations_total` - Database operations by type and operation

### Recording Rules

Pre-computed metrics for efficient querying:
- `redis_postgres_sync:failure_rate_percent:by_type` - Failure rate percentage
- `redis_postgres_sync:success_rate_percent:by_type` - Success rate percentage
- `redis_postgres_sync:cascade_overhead_percent:by_type` - Cascade time as % of total
- `redis_postgres_sync:event_processing_duration_seconds:p95:by_type` - P95 latency
- And more...

See `recording_rules.yml` for the complete list.

## Grafana Dashboards

Access Grafana at http://localhost:3000 (default credentials: admin/admin).

### Main Dashboard: Redis-PostgreSQL Sync Monitoring

The dashboard provides comprehensive visibility into event processing pipeline performance with the following panels:

**Event Processing Overview:**
- **Event Processing Rate by Type** - Real-time throughput for each blockchain event type (AtomCreated, TripleCreated, Deposited, Redeemed, SharePriceChanged)
- **Success vs Failure Rates** - Side-by-side comparison showing healthy processing vs errors
- **Event Distribution** - Pie chart showing the breakdown of event types being processed

**Performance Metrics:**
- **Event Processing Duration (P50, P95, P99)** - Latency percentiles by event type
  - P50: Median processing time (typical case)
  - P95: 95th percentile (catches slower outliers)
  - P99: 99th percentile (catches worst-case scenarios)
- **Cascade Processing Duration** - Time spent in database aggregation updates
  - Note: Excludes Redis publishing, focuses on database performance

**Database Operations:**
- **Database Operations Rate by Type** - Operations per second for each event type and operation
- **Operations Breakdown** - Shows distribution across position_update, vault_aggregation, term_aggregation, etc.

**Health Indicators:**
- **Failure Rate Percentage** - Calculated failure rate with threshold indicators
- **Cascade Overhead Percentage** - Shows how much of total processing time is spent in cascade updates
- **Cascade Failures** - Tracks failures that occur after successful event commits

**Additional Features:**
- Auto-refresh every 5 seconds for real-time monitoring
- Time range selector (default: last 1 hour)
- Event type filtering on all panels
- Hover tooltips with exact values
- Alert status indicators linked to Prometheus alerts

## Troubleshooting

### Alerts not appearing in Alertmanager

1. Check Prometheus is scraping metrics: http://localhost:9090/targets
2. Verify alert rules are loaded: http://localhost:9090/alerts
3. Check if alerts are firing in Prometheus before checking Alertmanager

### Alerts appearing but no notifications

1. **Most likely**: Receivers are not configured (see "Alertmanager Receiver Configuration" above)
2. Check Alertmanager logs: `docker logs alertmanager`
3. Verify receiver configuration with: `docker exec -it alertmanager amtool check-config`
4. Check Alertmanager UI for errors: http://localhost:9093

### Metrics not showing in Grafana

1. Verify Prometheus datasource is configured in Grafana
2. Check Prometheus is collecting metrics: http://localhost:9090/graph
3. Verify the sync service `/metrics` endpoint is accessible

### Recording rules not working

1. Check Prometheus logs for evaluation errors: `docker logs prometheus`
2. Verify recording rule syntax: `promtool check rules recording_rules.yml`
3. Check for division-by-zero or other expression errors in Prometheus logs

## Security Considerations

⚠️ **Before deploying to production:**

1. **Alertmanager Authentication**: The current configuration has no authentication. Consider:
   - Using a reverse proxy with authentication
   - Restricting network access with firewall rules
   - Using Alertmanager's built-in TLS support

2. **Webhook Security**: If using the legacy webhook, ensure:
   - The endpoint is secured with authentication
   - Network access is restricted
   - Consider removing if not needed

3. **Sensitive Data**:
   - Avoid including sensitive data in alert descriptions
   - Review runbook URLs to ensure they don't expose sensitive infrastructure details
   - Use secrets management for SMTP passwords, API keys, etc.

4. **Grafana Security**:
   - Change default admin password
   - Set up proper user authentication
   - Use HTTPS in production

## Next Steps

1. **Configure at least one receiver** in `alertmanager.yml`
2. **Test alert delivery** using the testing instructions above
3. **Review and adjust alert thresholds** based on your system's baseline
4. **Set up Grafana authentication** and change default passwords
5. **Create custom dashboards** for your specific monitoring needs

## Additional Resources

- [Prometheus Documentation](https://prometheus.io/docs/)
- [Alertmanager Configuration](https://prometheus.io/docs/alerting/latest/configuration/)
- [Grafana Documentation](https://grafana.com/docs/)
- [Runbooks](../../docs/runbooks/)
