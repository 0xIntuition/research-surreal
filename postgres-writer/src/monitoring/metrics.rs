use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, IntGaugeVec, Opts, Registry,
    TextEncoder,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::warn;

lazy_static! {
    static ref REGISTRY: Registry = Registry::new();
    static ref EVENTS_PROCESSED_COUNTER: Counter = Counter::new(
        "postgres_writer_events_processed_total",
        "Total number of events processed"
    ).unwrap();
    static ref EVENTS_FAILED_COUNTER: Counter = Counter::new(
        "postgres_writer_events_failed_total",
        "Total number of events that failed to process"
    ).unwrap();
    static ref BATCHES_PROCESSED_COUNTER: Counter = Counter::new(
        "postgres_writer_batches_processed_total",
        "Total number of batches processed"
    ).unwrap();
    static ref PROCESSING_DURATION_HISTOGRAM: Histogram = Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "postgres_writer_processing_duration_seconds",
            "Time spent processing events in seconds"
        ).buckets(vec![0.001, 0.01, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0])
    ).unwrap();
    static ref EVENTS_PER_SECOND_GAUGE: Gauge = Gauge::new(
        "postgres_writer_events_per_second",
        "Current events processing rate per second"
    ).unwrap();
    static ref PEAK_EVENTS_PER_SECOND_GAUGE: Gauge = Gauge::new(
        "postgres_writer_peak_events_per_second",
        "Peak events processing rate per second"
    ).unwrap();
    static ref RABBITMQ_HEALTHY_GAUGE: Gauge = Gauge::new(
        "postgres_writer_rabbitmq_healthy",
        "RabbitMQ connection health status (1=healthy, 0=unhealthy)"
    ).unwrap();
    static ref POSTGRES_HEALTHY_GAUGE: Gauge = Gauge::new(
        "postgres_writer_postgres_healthy",
        "PostgreSQL connection health status (1=healthy, 0=unhealthy)"
    ).unwrap();
    static ref UPTIME_GAUGE: Gauge = Gauge::new(
        "postgres_writer_uptime_seconds",
        "Application uptime in seconds"
    ).unwrap();

    // Connection pool metrics
    static ref POOL_CONNECTIONS_TOTAL_GAUGE: Gauge = Gauge::new(
        "postgres_writer_pool_connections_total",
        "Total number of connections in the PostgreSQL connection pool"
    ).unwrap();
    static ref POOL_CONNECTIONS_ACTIVE_GAUGE: Gauge = Gauge::new(
        "postgres_writer_pool_connections_active",
        "Number of active connections in the PostgreSQL connection pool"
    ).unwrap();
    static ref POOL_CONNECTIONS_IDLE_GAUGE: Gauge = Gauge::new(
        "postgres_writer_pool_connections_idle",
        "Number of idle connections in the PostgreSQL connection pool"
    ).unwrap();
    static ref POOL_UTILIZATION_GAUGE: Gauge = Gauge::new(
        "postgres_writer_pool_utilization_percent",
        "Connection pool utilization as a percentage (0-100)"
    ).unwrap();

    // RabbitMQ queue metrics
    static ref QUEUE_DEPTH_GAUGE: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "postgres_writer_queue_depth",
            "Number of messages in queue"
        ),
        &["queue"]
    ).unwrap();
    static ref MESSAGES_CONSUMED_COUNTER: CounterVec = CounterVec::new(
        Opts::new(
            "postgres_writer_messages_consumed_total",
            "Total messages consumed from queue"
        ),
        &["queue"]
    ).unwrap();
    static ref MESSAGES_ACKED_COUNTER: CounterVec = CounterVec::new(
        Opts::new(
            "postgres_writer_messages_acked_total",
            "Total messages acknowledged"
        ),
        &["queue"]
    ).unwrap();
    static ref MESSAGES_NACKED_COUNTER: CounterVec = CounterVec::new(
        Opts::new(
            "postgres_writer_messages_nacked_total",
            "Total messages negatively acknowledged"
        ),
        &["queue"]
    ).unwrap();
    static ref QUEUE_BATCH_SIZE_GAUGE: GaugeVec = GaugeVec::new(
        Opts::new(
            "postgres_writer_queue_batch_size",
            "Current batch size being processed per queue"
        ),
        &["queue"]
    ).unwrap();
    static ref QUEUE_LAST_MESSAGE_TIMESTAMP_GAUGE: GaugeVec = GaugeVec::new(
        Opts::new(
            "postgres_writer_queue_last_message_timestamp",
            "Unix timestamp of last processed message per queue"
        ),
        &["queue"]
    ).unwrap();

    // Event type-specific metrics
    //
    // IMPORTANT: Event types are bounded to prevent cardinality explosion in Prometheus.
    // The valid event types are defined by the blockchain contract and are:
    // - AtomCreated
    // - TripleCreated
    // - Deposited
    // - Redeemed
    // - SharePriceChanged
    //
    // These event types are hardcoded in the smart contract and cannot change without
    // a contract upgrade. This bounded set ensures safe usage of event_type as a label
    // dimension in metrics without risking unbounded cardinality growth.
    static ref EVENTS_PROCESSED_BY_TYPE_COUNTER: CounterVec = CounterVec::new(
        Opts::new(
            "postgres_writer_events_processed_by_type_total",
            "Total number of events processed by event type"
        ),
        &["event_type"]
    ).unwrap();
    static ref EVENTS_FAILED_BY_TYPE_COUNTER: CounterVec = CounterVec::new(
        Opts::new(
            "postgres_writer_events_failed_by_type_total",
            "Total number of events that failed to process by event type"
        ),
        &["event_type"]
    ).unwrap();
    static ref EVENT_PROCESSING_DURATION_BY_TYPE_HISTOGRAM: prometheus::HistogramVec = prometheus::HistogramVec::new(
        prometheus::HistogramOpts::new(
            "postgres_writer_event_processing_duration_by_type_seconds",
            "Time spent processing individual events by type in seconds"
        ).buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
        &["event_type"]
    ).unwrap();
    static ref CASCADE_PROCESSING_DURATION_HISTOGRAM: prometheus::HistogramVec = prometheus::HistogramVec::new(
        prometheus::HistogramOpts::new(
            "postgres_writer_cascade_processing_duration_seconds",
            "Time spent in cascade processing by event type in seconds"
        ).buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
        &["event_type"]
    ).unwrap();
    static ref DATABASE_OPERATIONS_COUNTER: CounterVec = CounterVec::new(
        Opts::new(
            "postgres_writer_database_operations_total",
            "Total database operations by event type and operation name"
        ),
        &["event_type", "operation"]
    ).unwrap();
    static ref CASCADE_FAILURES_COUNTER: CounterVec = CounterVec::new(
        Opts::new(
            "postgres_writer_cascade_failures_total",
            "Total cascade failures after successful event processing by event type"
        ),
        &["event_type"]
    ).unwrap();

    // Analytics worker metrics
    static ref ANALYTICS_MESSAGES_CONSUMED_COUNTER: Counter = Counter::new(
        "postgres_writer_analytics_messages_consumed_total",
        "Total number of term update messages consumed by analytics worker"
    ).unwrap();
    static ref ANALYTICS_MESSAGES_PENDING_GAUGE: Gauge = Gauge::new(
        "postgres_writer_analytics_messages_pending",
        "Number of pending messages in analytics worker stream"
    ).unwrap();
    static ref ANALYTICS_PROCESSING_DURATION_HISTOGRAM: Histogram = Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "postgres_writer_analytics_processing_duration_seconds",
            "Time spent processing analytics updates in seconds"
        ).buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0])
    ).unwrap();
    static ref ANALYTICS_BATCH_SIZE_GAUGE: Gauge = Gauge::new(
        "postgres_writer_analytics_batch_size",
        "Current batch size being processed by analytics worker"
    ).unwrap();
    static ref ANALYTICS_AFFECTED_TRIPLES_HISTOGRAM: Histogram = Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "postgres_writer_analytics_affected_triples",
            "Number of triples affected by each term update"
        ).buckets(vec![0.0, 1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 5000.0])
    ).unwrap();
    static ref ANALYTICS_MESSAGES_FAILED_COUNTER: Counter = Counter::new(
        "postgres_writer_analytics_messages_failed_total",
        "Total number of term update messages that failed to process"
    ).unwrap();

    // Term updates publishing metrics
    static ref TERM_UPDATES_PUBLISHED_COUNTER: Counter = Counter::new(
        "postgres_writer_term_updates_published_total",
        "Total number of term update messages published to RabbitMQ"
    ).unwrap();
    static ref TERM_UPDATES_PUBLISH_DURATION_HISTOGRAM: Histogram = Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "postgres_writer_term_updates_publish_duration_seconds",
            "Time spent publishing term updates to RabbitMQ"
        ).buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0])
    ).unwrap();
}

#[derive(Debug, Clone)]
pub struct Metrics {
    // Core counters
    events_processed: Arc<AtomicU64>,
    events_failed: Arc<AtomicU64>,
    batches_processed: Arc<AtomicU64>,

    // Timing and rates
    start_time: DateTime<Utc>,
    last_event_time: Arc<RwLock<Option<DateTime<Utc>>>>,
    events_per_second: Arc<RwLock<f64>>,
    peak_events_per_second: Arc<RwLock<f64>>,

    // Health status
    rabbitmq_healthy: Arc<RwLock<bool>>,
    postgres_healthy: Arc<RwLock<bool>>,
}

impl Metrics {
    pub fn new() -> Self {
        // Register metrics with Prometheus registry
        // Note: Registration failures are logged but not fatal, allowing the service to continue
        REGISTRY
            .register(Box::new(EVENTS_PROCESSED_COUNTER.clone()))
            .unwrap_or_else(|e| warn!("Failed to register EVENTS_PROCESSED_COUNTER: {}", e));
        REGISTRY
            .register(Box::new(EVENTS_FAILED_COUNTER.clone()))
            .unwrap_or_else(|e| warn!("Failed to register EVENTS_FAILED_COUNTER: {}", e));
        REGISTRY
            .register(Box::new(BATCHES_PROCESSED_COUNTER.clone()))
            .unwrap_or_else(|e| warn!("Failed to register BATCHES_PROCESSED_COUNTER: {}", e));
        REGISTRY
            .register(Box::new(PROCESSING_DURATION_HISTOGRAM.clone()))
            .unwrap_or_else(|e| warn!("Failed to register PROCESSING_DURATION_HISTOGRAM: {}", e));
        REGISTRY
            .register(Box::new(EVENTS_PER_SECOND_GAUGE.clone()))
            .unwrap_or_else(|e| warn!("Failed to register EVENTS_PER_SECOND_GAUGE: {}", e));
        REGISTRY
            .register(Box::new(PEAK_EVENTS_PER_SECOND_GAUGE.clone()))
            .unwrap_or_else(|e| warn!("Failed to register PEAK_EVENTS_PER_SECOND_GAUGE: {}", e));
        REGISTRY
            .register(Box::new(RABBITMQ_HEALTHY_GAUGE.clone()))
            .unwrap_or_else(|e| warn!("Failed to register RABBITMQ_HEALTHY_GAUGE: {}", e));
        REGISTRY
            .register(Box::new(POSTGRES_HEALTHY_GAUGE.clone()))
            .unwrap_or_else(|e| warn!("Failed to register POSTGRES_HEALTHY_GAUGE: {}", e));
        REGISTRY
            .register(Box::new(UPTIME_GAUGE.clone()))
            .unwrap_or_else(|e| warn!("Failed to register UPTIME_GAUGE: {}", e));

        // Register connection pool metrics
        REGISTRY
            .register(Box::new(POOL_CONNECTIONS_TOTAL_GAUGE.clone()))
            .unwrap_or_else(|e| warn!("Failed to register POOL_CONNECTIONS_TOTAL_GAUGE: {}", e));
        REGISTRY
            .register(Box::new(POOL_CONNECTIONS_ACTIVE_GAUGE.clone()))
            .unwrap_or_else(|e| warn!("Failed to register POOL_CONNECTIONS_ACTIVE_GAUGE: {}", e));
        REGISTRY
            .register(Box::new(POOL_CONNECTIONS_IDLE_GAUGE.clone()))
            .unwrap_or_else(|e| warn!("Failed to register POOL_CONNECTIONS_IDLE_GAUGE: {}", e));
        REGISTRY
            .register(Box::new(POOL_UTILIZATION_GAUGE.clone()))
            .unwrap_or_else(|e| warn!("Failed to register POOL_UTILIZATION_GAUGE: {}", e));

        // Register RabbitMQ queue metrics
        REGISTRY
            .register(Box::new(QUEUE_DEPTH_GAUGE.clone()))
            .unwrap_or_else(|e| warn!("Failed to register QUEUE_DEPTH_GAUGE: {}", e));
        REGISTRY
            .register(Box::new(MESSAGES_CONSUMED_COUNTER.clone()))
            .unwrap_or_else(|e| warn!("Failed to register MESSAGES_CONSUMED_COUNTER: {}", e));
        REGISTRY
            .register(Box::new(MESSAGES_ACKED_COUNTER.clone()))
            .unwrap_or_else(|e| warn!("Failed to register MESSAGES_ACKED_COUNTER: {}", e));
        REGISTRY
            .register(Box::new(MESSAGES_NACKED_COUNTER.clone()))
            .unwrap_or_else(|e| warn!("Failed to register MESSAGES_NACKED_COUNTER: {}", e));
        REGISTRY
            .register(Box::new(QUEUE_BATCH_SIZE_GAUGE.clone()))
            .unwrap_or_else(|e| warn!("Failed to register QUEUE_BATCH_SIZE_GAUGE: {}", e));
        REGISTRY
            .register(Box::new(QUEUE_LAST_MESSAGE_TIMESTAMP_GAUGE.clone()))
            .unwrap_or_else(|e| {
                warn!(
                    "Failed to register QUEUE_LAST_MESSAGE_TIMESTAMP_GAUGE: {}",
                    e
                )
            });

        // Register event type-specific metrics
        REGISTRY
            .register(Box::new(EVENTS_PROCESSED_BY_TYPE_COUNTER.clone()))
            .unwrap_or_else(|e| {
                warn!("Failed to register EVENTS_PROCESSED_BY_TYPE_COUNTER: {}", e)
            });
        REGISTRY
            .register(Box::new(EVENTS_FAILED_BY_TYPE_COUNTER.clone()))
            .unwrap_or_else(|e| warn!("Failed to register EVENTS_FAILED_BY_TYPE_COUNTER: {}", e));
        REGISTRY
            .register(Box::new(
                EVENT_PROCESSING_DURATION_BY_TYPE_HISTOGRAM.clone(),
            ))
            .unwrap_or_else(|e| {
                warn!(
                    "Failed to register EVENT_PROCESSING_DURATION_BY_TYPE_HISTOGRAM: {}",
                    e
                )
            });
        REGISTRY
            .register(Box::new(CASCADE_PROCESSING_DURATION_HISTOGRAM.clone()))
            .unwrap_or_else(|e| {
                warn!(
                    "Failed to register CASCADE_PROCESSING_DURATION_HISTOGRAM: {}",
                    e
                )
            });
        REGISTRY
            .register(Box::new(DATABASE_OPERATIONS_COUNTER.clone()))
            .unwrap_or_else(|e| warn!("Failed to register DATABASE_OPERATIONS_COUNTER: {}", e));
        REGISTRY
            .register(Box::new(CASCADE_FAILURES_COUNTER.clone()))
            .unwrap_or_else(|e| warn!("Failed to register CASCADE_FAILURES_COUNTER: {}", e));

        // Register analytics worker metrics
        REGISTRY
            .register(Box::new(ANALYTICS_MESSAGES_CONSUMED_COUNTER.clone()))
            .unwrap_or_else(|e| {
                warn!(
                    "Failed to register ANALYTICS_MESSAGES_CONSUMED_COUNTER: {}",
                    e
                )
            });
        REGISTRY
            .register(Box::new(ANALYTICS_MESSAGES_PENDING_GAUGE.clone()))
            .unwrap_or_else(|e| {
                warn!("Failed to register ANALYTICS_MESSAGES_PENDING_GAUGE: {}", e)
            });
        REGISTRY
            .register(Box::new(ANALYTICS_PROCESSING_DURATION_HISTOGRAM.clone()))
            .unwrap_or_else(|e| {
                warn!(
                    "Failed to register ANALYTICS_PROCESSING_DURATION_HISTOGRAM: {}",
                    e
                )
            });
        REGISTRY
            .register(Box::new(ANALYTICS_BATCH_SIZE_GAUGE.clone()))
            .unwrap_or_else(|e| warn!("Failed to register ANALYTICS_BATCH_SIZE_GAUGE: {}", e));
        REGISTRY
            .register(Box::new(ANALYTICS_AFFECTED_TRIPLES_HISTOGRAM.clone()))
            .unwrap_or_else(|e| {
                warn!(
                    "Failed to register ANALYTICS_AFFECTED_TRIPLES_HISTOGRAM: {}",
                    e
                )
            });
        REGISTRY
            .register(Box::new(ANALYTICS_MESSAGES_FAILED_COUNTER.clone()))
            .unwrap_or_else(|e| {
                warn!(
                    "Failed to register ANALYTICS_MESSAGES_FAILED_COUNTER: {}",
                    e
                )
            });

        // Register term updates publishing metrics
        REGISTRY
            .register(Box::new(TERM_UPDATES_PUBLISHED_COUNTER.clone()))
            .unwrap_or_else(|e| warn!("Failed to register TERM_UPDATES_PUBLISHED_COUNTER: {}", e));
        REGISTRY
            .register(Box::new(TERM_UPDATES_PUBLISH_DURATION_HISTOGRAM.clone()))
            .unwrap_or_else(|e| {
                warn!(
                    "Failed to register TERM_UPDATES_PUBLISH_DURATION_HISTOGRAM: {}",
                    e
                )
            });

        Self {
            events_processed: Arc::new(AtomicU64::new(0)),
            events_failed: Arc::new(AtomicU64::new(0)),
            batches_processed: Arc::new(AtomicU64::new(0)),
            start_time: Utc::now(),
            last_event_time: Arc::new(RwLock::new(None)),
            events_per_second: Arc::new(RwLock::new(0.0)),
            peak_events_per_second: Arc::new(RwLock::new(0.0)),
            rabbitmq_healthy: Arc::new(RwLock::new(false)),
            postgres_healthy: Arc::new(RwLock::new(false)),
        }
    }

    pub fn record_event_success(&self, count: u64) {
        self.events_processed.fetch_add(count, Ordering::Relaxed);
        EVENTS_PROCESSED_COUNTER.inc_by(count as f64);
    }

    pub fn record_event_failure(&self, count: u64) {
        self.events_failed.fetch_add(count, Ordering::Relaxed);
        EVENTS_FAILED_COUNTER.inc_by(count as f64);
    }

    pub fn record_batch(&self) {
        self.batches_processed.fetch_add(1, Ordering::Relaxed);
        BATCHES_PROCESSED_COUNTER.inc();
    }

    pub fn record_processing_time(&self, duration: std::time::Duration) {
        PROCESSING_DURATION_HISTOGRAM.observe(duration.as_secs_f64());
    }

    pub fn time_operation<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let start = Instant::now();
        let result = f();
        self.record_processing_time(start.elapsed());
        result
    }

    pub async fn time_async_operation<F, Fut, R>(&self, f: F) -> R
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = R>,
    {
        let start = Instant::now();
        let result = f().await;
        self.record_processing_time(start.elapsed());
        result
    }

    pub async fn update_event_rate(&self, rate: f64) {
        let mut current_rate = self.events_per_second.write().await;
        *current_rate = rate;
        EVENTS_PER_SECOND_GAUGE.set(rate);

        let mut peak = self.peak_events_per_second.write().await;
        if rate > *peak {
            *peak = rate;
            PEAK_EVENTS_PER_SECOND_GAUGE.set(rate);
        }
    }

    pub async fn set_rabbitmq_health(&self, healthy: bool) {
        *self.rabbitmq_healthy.write().await = healthy;
        RABBITMQ_HEALTHY_GAUGE.set(if healthy { 1.0 } else { 0.0 });
    }

    pub async fn set_postgres_health(&self, healthy: bool) {
        *self.postgres_healthy.write().await = healthy;
        POSTGRES_HEALTHY_GAUGE.set(if healthy { 1.0 } else { 0.0 });
    }

    pub fn record_connection_pool_stats(
        &self,
        stats: &crate::monitoring::health::ConnectionPoolStats,
    ) {
        POOL_CONNECTIONS_TOTAL_GAUGE.set(stats.total_connections as f64);
        POOL_CONNECTIONS_ACTIVE_GAUGE.set(stats.active_connections as f64);
        POOL_CONNECTIONS_IDLE_GAUGE.set(stats.idle_connections as f64);
        POOL_UTILIZATION_GAUGE.set(stats.pool_utilization);
    }

    pub async fn update_last_event_time(&self) {
        *self.last_event_time.write().await = Some(Utc::now());
    }

    // RabbitMQ queue metrics methods
    pub fn record_queue_depth(&self, queue: &str, depth: i64) {
        QUEUE_DEPTH_GAUGE.with_label_values(&[queue]).set(depth);
    }

    pub fn record_queue_messages_consumed(&self, queue: &str, count: usize) {
        MESSAGES_CONSUMED_COUNTER
            .with_label_values(&[queue])
            .inc_by(count as f64);
    }

    pub fn record_queue_message_acked(&self, queue: &str) {
        MESSAGES_ACKED_COUNTER.with_label_values(&[queue]).inc();
    }

    pub fn record_queue_message_nacked(&self, queue: &str) {
        MESSAGES_NACKED_COUNTER.with_label_values(&[queue]).inc();
    }

    pub fn record_queue_batch_size(&self, queue: &str, size: usize) {
        QUEUE_BATCH_SIZE_GAUGE
            .with_label_values(&[queue])
            .set(size as f64);
    }

    pub fn record_queue_last_message_timestamp(&self, queue: &str) {
        let timestamp = Utc::now().timestamp() as f64;
        QUEUE_LAST_MESSAGE_TIMESTAMP_GAUGE
            .with_label_values(&[queue])
            .set(timestamp);
    }

    // Event type-specific metrics methods
    pub fn record_event_by_type_success(&self, event_type: &str) {
        EVENTS_PROCESSED_BY_TYPE_COUNTER
            .with_label_values(&[event_type])
            .inc();
    }

    pub fn record_event_by_type_failure(&self, event_type: &str) {
        EVENTS_FAILED_BY_TYPE_COUNTER
            .with_label_values(&[event_type])
            .inc();
    }

    pub fn record_event_processing_duration(
        &self,
        event_type: &str,
        duration: std::time::Duration,
    ) {
        EVENT_PROCESSING_DURATION_BY_TYPE_HISTOGRAM
            .with_label_values(&[event_type])
            .observe(duration.as_secs_f64());
    }

    pub fn record_cascade_duration(&self, event_type: &str, duration: std::time::Duration) {
        CASCADE_PROCESSING_DURATION_HISTOGRAM
            .with_label_values(&[event_type])
            .observe(duration.as_secs_f64());
    }

    pub fn record_database_operation(&self, event_type: &str, operation: &str) {
        DATABASE_OPERATIONS_COUNTER
            .with_label_values(&[event_type, operation])
            .inc();
    }

    pub fn record_cascade_failure(&self, event_type: &str) {
        CASCADE_FAILURES_COUNTER
            .with_label_values(&[event_type])
            .inc();
    }

    // Analytics worker metrics methods
    pub fn record_analytics_message_consumed(&self) {
        ANALYTICS_MESSAGES_CONSUMED_COUNTER.inc();
    }

    pub fn record_analytics_messages_pending(&self, count: i64) {
        ANALYTICS_MESSAGES_PENDING_GAUGE.set(count as f64);
    }

    pub fn record_analytics_processing_duration(&self, duration: std::time::Duration) {
        ANALYTICS_PROCESSING_DURATION_HISTOGRAM.observe(duration.as_secs_f64());
    }

    pub fn record_analytics_batch_size(&self, size: usize) {
        ANALYTICS_BATCH_SIZE_GAUGE.set(size as f64);
    }

    pub fn record_analytics_affected_triples(&self, count: usize) {
        ANALYTICS_AFFECTED_TRIPLES_HISTOGRAM.observe(count as f64);
    }

    pub fn record_analytics_message_failure(&self) {
        ANALYTICS_MESSAGES_FAILED_COUNTER.inc();
    }

    // Term updates publishing metrics methods
    pub fn record_term_update_published(&self) {
        TERM_UPDATES_PUBLISHED_COUNTER.inc();
    }

    pub fn record_term_updates_publish_duration(&self, duration: std::time::Duration) {
        TERM_UPDATES_PUBLISH_DURATION_HISTOGRAM.observe(duration.as_secs_f64());
    }

    pub async fn get_snapshot(&self) -> MetricsSnapshot {
        let uptime = (Utc::now() - self.start_time).num_seconds() as u64;
        UPTIME_GAUGE.set(uptime as f64);

        MetricsSnapshot {
            total_events_processed: self.events_processed.load(Ordering::Relaxed),
            total_events_failed: self.events_failed.load(Ordering::Relaxed),
            total_batches_processed: self.batches_processed.load(Ordering::Relaxed),
            events_per_second: *self.events_per_second.read().await,
            peak_events_per_second: *self.peak_events_per_second.read().await,
            rabbitmq_healthy: *self.rabbitmq_healthy.read().await,
            postgres_healthy: *self.postgres_healthy.read().await,
            uptime_seconds: uptime,
            start_time: self.start_time,
            last_event_time: *self.last_event_time.read().await,
        }
    }

    pub fn get_prometheus_metrics() -> Result<String, Box<dyn std::error::Error>> {
        let encoder = TextEncoder::new();
        let metric_families = REGISTRY.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer)?;
        Ok(String::from_utf8(buffer)?)
    }

    #[cfg(test)]
    pub fn reset_for_tests() {
        // Reset all simple counters
        EVENTS_PROCESSED_COUNTER.reset();
        EVENTS_FAILED_COUNTER.reset();
        BATCHES_PROCESSED_COUNTER.reset();

        // Reset event type-specific counters (Vec types)
        EVENTS_PROCESSED_BY_TYPE_COUNTER.reset();
        EVENTS_FAILED_BY_TYPE_COUNTER.reset();
        DATABASE_OPERATIONS_COUNTER.reset();
        CASCADE_FAILURES_COUNTER.reset();

        // Reset RabbitMQ queue metrics (Vec types)
        MESSAGES_CONSUMED_COUNTER.reset();
        MESSAGES_ACKED_COUNTER.reset();
        MESSAGES_NACKED_COUNTER.reset();

        // Reset histogram vectors
        EVENT_PROCESSING_DURATION_BY_TYPE_HISTOGRAM.reset();
        CASCADE_PROCESSING_DURATION_HISTOGRAM.reset();

        // Reset simple gauges
        EVENTS_PER_SECOND_GAUGE.set(0.0);
        PEAK_EVENTS_PER_SECOND_GAUGE.set(0.0);
        RABBITMQ_HEALTHY_GAUGE.set(0.0);
        POSTGRES_HEALTHY_GAUGE.set(0.0);
        UPTIME_GAUGE.set(0.0);

        // Reset connection pool gauges
        POOL_CONNECTIONS_TOTAL_GAUGE.set(0.0);
        POOL_CONNECTIONS_ACTIVE_GAUGE.set(0.0);
        POOL_CONNECTIONS_IDLE_GAUGE.set(0.0);
        POOL_UTILIZATION_GAUGE.set(0.0);

        // Reset RabbitMQ queue gauge vectors
        QUEUE_DEPTH_GAUGE.reset();
        QUEUE_BATCH_SIZE_GAUGE.reset();
        QUEUE_LAST_MESSAGE_TIMESTAMP_GAUGE.reset();

        // Reset analytics worker metrics
        ANALYTICS_MESSAGES_CONSUMED_COUNTER.reset();
        ANALYTICS_MESSAGES_PENDING_GAUGE.set(0.0);
        // Note: Histogram types don't support reset()
        // ANALYTICS_PROCESSING_DURATION_HISTOGRAM.reset();
        ANALYTICS_BATCH_SIZE_GAUGE.set(0.0);
        // ANALYTICS_AFFECTED_TRIPLES_HISTOGRAM.reset();
        ANALYTICS_MESSAGES_FAILED_COUNTER.reset();

        // Reset term updates publishing metrics
        TERM_UPDATES_PUBLISHED_COUNTER.reset();
        // TERM_UPDATES_PUBLISH_DURATION_HISTOGRAM.reset();
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub total_events_processed: u64,
    pub total_events_failed: u64,
    pub total_batches_processed: u64,
    pub events_per_second: f64,
    pub peak_events_per_second: f64,
    pub rabbitmq_healthy: bool,
    pub postgres_healthy: bool,
    pub uptime_seconds: u64,
    pub start_time: DateTime<Utc>,
    pub last_event_time: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Mutex to ensure metrics tests run serially
    static TEST_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn test_prometheus_metrics_format() {
        let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        Metrics::reset_for_tests();
        let metrics = Metrics::new();

        // Record some test data
        metrics.record_event_success(100);
        metrics.record_event_failure(5);
        metrics.record_batch();

        // Test Prometheus output
        let output = Metrics::get_prometheus_metrics().expect("Should generate metrics");

        // Check that it contains expected metrics
        assert!(output.contains("postgres_writer_events_processed_total"));
        assert!(output.contains("postgres_writer_events_failed_total"));
        assert!(output.contains("postgres_writer_batches_processed_total"));
        assert!(output.contains("postgres_writer_processing_duration_seconds"));

        // Print for manual verification
        println!("Prometheus metrics output:");
        println!("{output}");
    }

    #[test]
    fn test_event_by_type_success() {
        let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        Metrics::reset_for_tests();
        let metrics = Metrics::new();

        // Record successful events by type
        metrics.record_event_by_type_success("AtomCreated");
        metrics.record_event_by_type_success("AtomCreated");
        metrics.record_event_by_type_success("Deposited");

        // Get Prometheus output
        let output = Metrics::get_prometheus_metrics().expect("Should generate metrics");

        // Verify event-type-specific metrics are present
        assert!(output.contains("postgres_writer_events_processed_by_type_total"));
        assert!(output.contains("event_type=\"AtomCreated\""));
        assert!(output.contains("event_type=\"Deposited\""));

        // Verify counts are recorded (checking that the metric line exists)
        // Note: Exact formatting may vary, so we check for presence of event types
        let atom_created_lines: Vec<&str> = output
            .lines()
            .filter(|line| {
                line.contains("postgres_writer_events_processed_by_type_total")
                    && line.contains("event_type=\"AtomCreated\"")
            })
            .collect();
        assert!(
            !atom_created_lines.is_empty(),
            "AtomCreated metrics should be present"
        );
    }

    #[test]
    fn test_event_by_type_failure() {
        let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        Metrics::reset_for_tests();
        let metrics = Metrics::new();

        // Record failed events by type
        metrics.record_event_by_type_failure("SharePriceChanged");
        metrics.record_event_by_type_failure("SharePriceChanged");
        metrics.record_event_by_type_failure("TripleCreated");

        // Get Prometheus output
        let output = Metrics::get_prometheus_metrics().expect("Should generate metrics");

        // Verify failure metrics are present
        assert!(output.contains("postgres_writer_events_failed_by_type_total"));
        assert!(output.contains("event_type=\"SharePriceChanged\""));
        assert!(output.contains("event_type=\"TripleCreated\""));

        // Verify counts
        assert!(output.contains(
            "postgres_writer_events_failed_by_type_total{event_type=\"SharePriceChanged\"} 2"
        ));
        assert!(output.contains(
            "postgres_writer_events_failed_by_type_total{event_type=\"TripleCreated\"} 1"
        ));
    }

    #[test]
    fn test_event_processing_duration() {
        let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        Metrics::reset_for_tests();
        let metrics = Metrics::new();

        // Record processing durations for different event types
        metrics
            .record_event_processing_duration("AtomCreated", std::time::Duration::from_millis(100));
        metrics
            .record_event_processing_duration("AtomCreated", std::time::Duration::from_millis(150));
        metrics
            .record_event_processing_duration("Deposited", std::time::Duration::from_millis(200));

        // Get Prometheus output
        let output = Metrics::get_prometheus_metrics().expect("Should generate metrics");

        // Verify histogram metrics are present
        assert!(output.contains("postgres_writer_event_processing_duration_by_type_seconds"));
        assert!(output.contains("event_type=\"AtomCreated\""));
        assert!(output.contains("event_type=\"Deposited\""));

        // Verify histogram has count (checking that the metric exists)
        let atom_created_count: Vec<&str> = output
            .lines()
            .filter(|line| {
                line.contains("postgres_writer_event_processing_duration_by_type_seconds_count")
                    && line.contains("event_type=\"AtomCreated\"")
            })
            .collect();
        assert!(
            !atom_created_count.is_empty(),
            "AtomCreated duration metrics should be present"
        );
    }

    #[test]
    fn test_cascade_duration() {
        let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        Metrics::reset_for_tests();
        let metrics = Metrics::new();

        // Record cascade durations
        metrics.record_cascade_duration("Deposited", std::time::Duration::from_millis(50));
        metrics.record_cascade_duration("Deposited", std::time::Duration::from_millis(75));
        metrics.record_cascade_duration("SharePriceChanged", std::time::Duration::from_millis(30));

        // Get Prometheus output
        let output = Metrics::get_prometheus_metrics().expect("Should generate metrics");

        // Verify cascade metrics are present
        assert!(output.contains("postgres_writer_cascade_processing_duration_seconds"));
        assert!(output.contains("event_type=\"Deposited\""));
        assert!(output.contains("event_type=\"SharePriceChanged\""));

        // Verify histogram counts
        assert!(output.contains(
            "postgres_writer_cascade_processing_duration_seconds_count{event_type=\"Deposited\"} 2"
        ));
        assert!(output.contains("postgres_writer_cascade_processing_duration_seconds_count{event_type=\"SharePriceChanged\"} 1"));
    }

    #[test]
    fn test_database_operations() {
        let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        Metrics::reset_for_tests();
        let metrics = Metrics::new();

        // Record database operations
        metrics.record_database_operation("Deposited", "position_update");
        metrics.record_database_operation("Deposited", "position_update");
        metrics.record_database_operation("Deposited", "vault_aggregation");
        metrics.record_database_operation("SharePriceChanged", "vault_update");
        metrics.record_database_operation("AtomCreated", "term_initialization");

        // Get Prometheus output
        let output = Metrics::get_prometheus_metrics().expect("Should generate metrics");

        // Verify database operation metrics are present
        assert!(output.contains("postgres_writer_database_operations_total"));

        // Verify different event types and operations
        assert!(output.contains("event_type=\"Deposited\""));
        assert!(output.contains("operation=\"position_update\""));
        assert!(output.contains("operation=\"vault_aggregation\""));
        assert!(output.contains("event_type=\"SharePriceChanged\""));
        assert!(output.contains("operation=\"vault_update\""));
        assert!(output.contains("event_type=\"AtomCreated\""));
        assert!(output.contains("operation=\"term_initialization\""));

        // Verify counts
        assert!(output.contains("postgres_writer_database_operations_total{event_type=\"Deposited\",operation=\"position_update\"} 2"));
        assert!(output.contains("postgres_writer_database_operations_total{event_type=\"Deposited\",operation=\"vault_aggregation\"} 1"));
    }

    #[test]
    fn test_cascade_failure() {
        let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        Metrics::reset_for_tests();
        let metrics = Metrics::new();

        // Record cascade failures
        metrics.record_cascade_failure("Deposited");
        metrics.record_cascade_failure("Deposited");
        metrics.record_cascade_failure("SharePriceChanged");

        // Get Prometheus output
        let output = Metrics::get_prometheus_metrics().expect("Should generate metrics");

        // Verify cascade failure metrics are present
        assert!(output.contains("postgres_writer_cascade_failures_total"));
        assert!(output.contains("event_type=\"Deposited\""));
        assert!(output.contains("event_type=\"SharePriceChanged\""));

        // Verify counts
        assert!(
            output.contains("postgres_writer_cascade_failures_total{event_type=\"Deposited\"} 2")
        );
        assert!(output.contains(
            "postgres_writer_cascade_failures_total{event_type=\"SharePriceChanged\"} 1"
        ));
    }

    #[test]
    fn test_multiple_event_types() {
        let _guard = TEST_MUTEX.lock().unwrap_or_else(|e| e.into_inner());
        Metrics::reset_for_tests();
        let metrics = Metrics::new();

        // Simulate processing multiple different event types
        let event_types = vec![
            "AtomCreated",
            "Deposited",
            "SharePriceChanged",
            "TripleCreated",
            "Redeemed",
        ];

        for event_type in &event_types {
            metrics.record_event_by_type_success(event_type);
            metrics.record_event_processing_duration(
                event_type,
                std::time::Duration::from_millis(100),
            );
            metrics.record_cascade_duration(event_type, std::time::Duration::from_millis(25));
        }

        // Add one failure
        metrics.record_event_by_type_failure("Deposited");

        // Add cascade failure
        metrics.record_cascade_failure("SharePriceChanged");

        // Get Prometheus output
        let output = Metrics::get_prometheus_metrics().expect("Should generate metrics");

        // Verify all event types are present in metrics
        for event_type in &event_types {
            assert!(output.contains(&format!("event_type=\"{event_type}\"")));
        }

        // Verify different metric types
        assert!(output.contains("postgres_writer_events_processed_by_type_total"));
        assert!(output.contains("postgres_writer_events_failed_by_type_total"));
        assert!(output.contains("postgres_writer_event_processing_duration_by_type_seconds"));
        assert!(output.contains("postgres_writer_cascade_processing_duration_seconds"));
        assert!(output.contains("postgres_writer_cascade_failures_total"));
    }
}
