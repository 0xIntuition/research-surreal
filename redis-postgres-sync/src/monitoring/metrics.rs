use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use prometheus::{Counter, Gauge, Histogram, Registry, Encoder, TextEncoder, GaugeVec, CounterVec, IntGaugeVec, Opts};
use lazy_static::lazy_static;
use std::time::Instant;

lazy_static! {
    static ref REGISTRY: Registry = Registry::new();
    static ref EVENTS_PROCESSED_COUNTER: Counter = Counter::new(
        "redis_postgres_sync_events_processed_total",
        "Total number of events processed"
    ).unwrap();
    static ref EVENTS_FAILED_COUNTER: Counter = Counter::new(
        "redis_postgres_sync_events_failed_total",
        "Total number of events that failed to process"
    ).unwrap();
    static ref BATCHES_PROCESSED_COUNTER: Counter = Counter::new(
        "redis_postgres_sync_batches_processed_total",
        "Total number of batches processed"
    ).unwrap();
    static ref PROCESSING_DURATION_HISTOGRAM: Histogram = Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "redis_postgres_sync_processing_duration_seconds",
            "Time spent processing events in seconds"
        ).buckets(vec![0.001, 0.01, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0])
    ).unwrap();
    static ref EVENTS_PER_SECOND_GAUGE: Gauge = Gauge::new(
        "redis_postgres_sync_events_per_second",
        "Current events processing rate per second"
    ).unwrap();
    static ref PEAK_EVENTS_PER_SECOND_GAUGE: Gauge = Gauge::new(
        "redis_postgres_sync_peak_events_per_second",
        "Peak events processing rate per second"
    ).unwrap();
    static ref REDIS_HEALTHY_GAUGE: Gauge = Gauge::new(
        "redis_postgres_sync_redis_healthy",
        "Redis connection health status (1=healthy, 0=unhealthy)"
    ).unwrap();
    static ref POSTGRES_HEALTHY_GAUGE: Gauge = Gauge::new(
        "redis_postgres_sync_postgres_healthy",
        "PostgreSQL connection health status (1=healthy, 0=unhealthy)"
    ).unwrap();
    static ref UPTIME_GAUGE: Gauge = Gauge::new(
        "redis_postgres_sync_uptime_seconds",
        "Application uptime in seconds"
    ).unwrap();

    // Redis Streams metrics
    static ref STREAM_LAG_GAUGE: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "redis_postgres_sync_stream_lag",
            "Estimated lag between last read message and stream end per stream"
        ),
        &["stream_name"]
    ).unwrap();
    static ref STREAM_PENDING_MESSAGES_GAUGE: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "redis_postgres_sync_stream_pending_messages",
            "Number of pending messages (consumed but not ACKed) per stream"
        ),
        &["stream_name"]
    ).unwrap();
    static ref STREAM_MESSAGES_CLAIMED_COUNTER: CounterVec = CounterVec::new(
        Opts::new(
            "redis_postgres_sync_stream_messages_claimed_total",
            "Total number of idle messages claimed from other consumers"
        ),
        &["stream_name"]
    ).unwrap();
    static ref STREAM_BATCH_SIZE_GAUGE: GaugeVec = GaugeVec::new(
        Opts::new(
            "redis_postgres_sync_stream_batch_size",
            "Current batch size being processed per stream"
        ),
        &["stream_name"]
    ).unwrap();
    static ref STREAM_LAST_MESSAGE_TIMESTAMP_GAUGE: GaugeVec = GaugeVec::new(
        Opts::new(
            "redis_postgres_sync_stream_last_message_timestamp",
            "Unix timestamp of last processed message per stream"
        ),
        &["stream_name"]
    ).unwrap();
    static ref STREAM_MESSAGES_CONSUMED_COUNTER: CounterVec = CounterVec::new(
        Opts::new(
            "redis_postgres_sync_stream_messages_consumed_total",
            "Total messages consumed from each stream"
        ),
        &["stream_name"]
    ).unwrap();

    // Event type-specific metrics
    static ref EVENTS_PROCESSED_BY_TYPE_COUNTER: CounterVec = CounterVec::new(
        Opts::new(
            "redis_postgres_sync_events_processed_by_type_total",
            "Total number of events processed by event type"
        ),
        &["event_type"]
    ).unwrap();
    static ref EVENTS_FAILED_BY_TYPE_COUNTER: CounterVec = CounterVec::new(
        Opts::new(
            "redis_postgres_sync_events_failed_by_type_total",
            "Total number of events that failed to process by event type"
        ),
        &["event_type"]
    ).unwrap();
    static ref EVENT_PROCESSING_DURATION_BY_TYPE_HISTOGRAM: prometheus::HistogramVec = prometheus::HistogramVec::new(
        prometheus::HistogramOpts::new(
            "redis_postgres_sync_event_processing_duration_by_type_seconds",
            "Time spent processing individual events by type in seconds"
        ).buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]),
        &["event_type"]
    ).unwrap();
    static ref CASCADE_PROCESSING_DURATION_HISTOGRAM: prometheus::HistogramVec = prometheus::HistogramVec::new(
        prometheus::HistogramOpts::new(
            "redis_postgres_sync_cascade_processing_duration_seconds",
            "Time spent in cascade processing by event type in seconds"
        ).buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]),
        &["event_type"]
    ).unwrap();
    static ref DATABASE_OPERATIONS_COUNTER: CounterVec = CounterVec::new(
        Opts::new(
            "redis_postgres_sync_database_operations_total",
            "Total database operations by event type and operation name"
        ),
        &["event_type", "operation"]
    ).unwrap();
    static ref CASCADE_FAILURES_COUNTER: CounterVec = CounterVec::new(
        Opts::new(
            "redis_postgres_sync_cascade_failures_total",
            "Total cascade failures after successful event processing by event type"
        ),
        &["event_type"]
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
    redis_healthy: Arc<RwLock<bool>>,
    postgres_healthy: Arc<RwLock<bool>>,
}

impl Metrics {
    pub fn new() -> Self {
        // Register metrics with Prometheus registry
        REGISTRY.register(Box::new(EVENTS_PROCESSED_COUNTER.clone())).ok();
        REGISTRY.register(Box::new(EVENTS_FAILED_COUNTER.clone())).ok();
        REGISTRY.register(Box::new(BATCHES_PROCESSED_COUNTER.clone())).ok();
        REGISTRY.register(Box::new(PROCESSING_DURATION_HISTOGRAM.clone())).ok();
        REGISTRY.register(Box::new(EVENTS_PER_SECOND_GAUGE.clone())).ok();
        REGISTRY.register(Box::new(PEAK_EVENTS_PER_SECOND_GAUGE.clone())).ok();
        REGISTRY.register(Box::new(REDIS_HEALTHY_GAUGE.clone())).ok();
        REGISTRY.register(Box::new(POSTGRES_HEALTHY_GAUGE.clone())).ok();
        REGISTRY.register(Box::new(UPTIME_GAUGE.clone())).ok();

        // Register Redis Streams metrics
        REGISTRY.register(Box::new(STREAM_LAG_GAUGE.clone())).ok();
        REGISTRY.register(Box::new(STREAM_PENDING_MESSAGES_GAUGE.clone())).ok();
        REGISTRY.register(Box::new(STREAM_MESSAGES_CLAIMED_COUNTER.clone())).ok();
        REGISTRY.register(Box::new(STREAM_BATCH_SIZE_GAUGE.clone())).ok();
        REGISTRY.register(Box::new(STREAM_LAST_MESSAGE_TIMESTAMP_GAUGE.clone())).ok();
        REGISTRY.register(Box::new(STREAM_MESSAGES_CONSUMED_COUNTER.clone())).ok();

        // Register event type-specific metrics
        REGISTRY.register(Box::new(EVENTS_PROCESSED_BY_TYPE_COUNTER.clone())).ok();
        REGISTRY.register(Box::new(EVENTS_FAILED_BY_TYPE_COUNTER.clone())).ok();
        REGISTRY.register(Box::new(EVENT_PROCESSING_DURATION_BY_TYPE_HISTOGRAM.clone())).ok();
        REGISTRY.register(Box::new(CASCADE_PROCESSING_DURATION_HISTOGRAM.clone())).ok();
        REGISTRY.register(Box::new(DATABASE_OPERATIONS_COUNTER.clone())).ok();
        REGISTRY.register(Box::new(CASCADE_FAILURES_COUNTER.clone())).ok();

        Self {
            events_processed: Arc::new(AtomicU64::new(0)),
            events_failed: Arc::new(AtomicU64::new(0)),
            batches_processed: Arc::new(AtomicU64::new(0)),
            start_time: Utc::now(),
            last_event_time: Arc::new(RwLock::new(None)),
            events_per_second: Arc::new(RwLock::new(0.0)),
            peak_events_per_second: Arc::new(RwLock::new(0.0)),
            redis_healthy: Arc::new(RwLock::new(false)),
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
    
    pub async fn set_redis_health(&self, healthy: bool) {
        *self.redis_healthy.write().await = healthy;
        REDIS_HEALTHY_GAUGE.set(if healthy { 1.0 } else { 0.0 });
    }
    
    pub async fn set_postgres_health(&self, healthy: bool) {
        *self.postgres_healthy.write().await = healthy;
        POSTGRES_HEALTHY_GAUGE.set(if healthy { 1.0 } else { 0.0 });
    }
    
    pub async fn update_last_event_time(&self) {
        *self.last_event_time.write().await = Some(Utc::now());
    }

    // Redis Streams metrics methods
    pub fn record_stream_lag(&self, stream_name: &str, lag: i64) {
        STREAM_LAG_GAUGE.with_label_values(&[stream_name]).set(lag);
    }

    pub fn record_stream_pending_messages(&self, stream_name: &str, count: i64) {
        STREAM_PENDING_MESSAGES_GAUGE.with_label_values(&[stream_name]).set(count);
    }

    pub fn record_stream_messages_claimed(&self, stream_name: &str, count: u64) {
        STREAM_MESSAGES_CLAIMED_COUNTER.with_label_values(&[stream_name]).inc_by(count as f64);
    }

    pub fn record_stream_batch_size(&self, stream_name: &str, size: usize) {
        STREAM_BATCH_SIZE_GAUGE.with_label_values(&[stream_name]).set(size as f64);
    }

    pub fn record_stream_last_message_timestamp(&self, stream_name: &str) {
        let timestamp = Utc::now().timestamp() as f64;
        STREAM_LAST_MESSAGE_TIMESTAMP_GAUGE.with_label_values(&[stream_name]).set(timestamp);
    }

    pub fn record_stream_messages_consumed(&self, stream_name: &str, count: usize) {
        STREAM_MESSAGES_CONSUMED_COUNTER.with_label_values(&[stream_name]).inc_by(count as f64);
    }

    // Event type-specific metrics methods
    pub fn record_event_by_type_success(&self, event_type: &str) {
        EVENTS_PROCESSED_BY_TYPE_COUNTER.with_label_values(&[event_type]).inc();
    }

    pub fn record_event_by_type_failure(&self, event_type: &str) {
        EVENTS_FAILED_BY_TYPE_COUNTER.with_label_values(&[event_type]).inc();
    }

    pub fn record_event_processing_duration(&self, event_type: &str, duration: std::time::Duration) {
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
        DATABASE_OPERATIONS_COUNTER.with_label_values(&[event_type, operation]).inc();
    }

    pub fn record_cascade_failure(&self, event_type: &str) {
        CASCADE_FAILURES_COUNTER.with_label_values(&[event_type]).inc();
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
            redis_healthy: *self.redis_healthy.read().await,
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
    pub redis_healthy: bool,
    pub postgres_healthy: bool,
    pub uptime_seconds: u64,
    pub start_time: DateTime<Utc>,
    pub last_event_time: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prometheus_metrics_format() {
        let metrics = Metrics::new();
        
        // Record some test data
        metrics.record_event_success(100);
        metrics.record_event_failure(5);
        metrics.record_batch();
        
        // Test Prometheus output
        let output = Metrics::get_prometheus_metrics().expect("Should generate metrics");
        
        // Check that it contains expected metrics
        assert!(output.contains("redis_postgres_sync_events_processed_total"));
        assert!(output.contains("redis_postgres_sync_events_failed_total"));
        assert!(output.contains("redis_postgres_sync_batches_processed_total"));
        assert!(output.contains("redis_postgres_sync_processing_duration_seconds"));
        
        // Print for manual verification
        println!("Prometheus metrics output:");
        println!("{}", output);
    }

    #[test]
    fn test_event_by_type_success() {
        let metrics = Metrics::new();

        // Record successful events by type
        metrics.record_event_by_type_success("AtomCreated");
        metrics.record_event_by_type_success("AtomCreated");
        metrics.record_event_by_type_success("Deposited");

        // Get Prometheus output
        let output = Metrics::get_prometheus_metrics().expect("Should generate metrics");

        // Verify event-type-specific metrics are present
        assert!(output.contains("redis_postgres_sync_events_processed_by_type_total"));
        assert!(output.contains("event_type=\"AtomCreated\""));
        assert!(output.contains("event_type=\"Deposited\""));

        // Verify counts are recorded (checking that the metric line exists)
        // Note: Exact formatting may vary, so we check for presence of event types
        let atom_created_lines: Vec<&str> = output.lines()
            .filter(|line| line.contains("redis_postgres_sync_events_processed_by_type_total")
                        && line.contains("event_type=\"AtomCreated\""))
            .collect();
        assert!(!atom_created_lines.is_empty(), "AtomCreated metrics should be present");
    }

    #[test]
    fn test_event_by_type_failure() {
        let metrics = Metrics::new();

        // Record failed events by type
        metrics.record_event_by_type_failure("SharePriceChanged");
        metrics.record_event_by_type_failure("SharePriceChanged");
        metrics.record_event_by_type_failure("TripleCreated");

        // Get Prometheus output
        let output = Metrics::get_prometheus_metrics().expect("Should generate metrics");

        // Verify failure metrics are present
        assert!(output.contains("redis_postgres_sync_events_failed_by_type_total"));
        assert!(output.contains("event_type=\"SharePriceChanged\""));
        assert!(output.contains("event_type=\"TripleCreated\""));

        // Verify counts
        assert!(output.contains("redis_postgres_sync_events_failed_by_type_total{event_type=\"SharePriceChanged\"} 2"));
        assert!(output.contains("redis_postgres_sync_events_failed_by_type_total{event_type=\"TripleCreated\"} 1"));
    }

    #[test]
    fn test_event_processing_duration() {
        let metrics = Metrics::new();

        // Record processing durations for different event types
        metrics.record_event_processing_duration("AtomCreated", std::time::Duration::from_millis(100));
        metrics.record_event_processing_duration("AtomCreated", std::time::Duration::from_millis(150));
        metrics.record_event_processing_duration("Deposited", std::time::Duration::from_millis(200));

        // Get Prometheus output
        let output = Metrics::get_prometheus_metrics().expect("Should generate metrics");

        // Verify histogram metrics are present
        assert!(output.contains("redis_postgres_sync_event_processing_duration_by_type_seconds"));
        assert!(output.contains("event_type=\"AtomCreated\""));
        assert!(output.contains("event_type=\"Deposited\""));

        // Verify histogram has count (checking that the metric exists)
        let atom_created_count: Vec<&str> = output.lines()
            .filter(|line| line.contains("redis_postgres_sync_event_processing_duration_by_type_seconds_count")
                        && line.contains("event_type=\"AtomCreated\""))
            .collect();
        assert!(!atom_created_count.is_empty(), "AtomCreated duration metrics should be present");
    }

    #[test]
    fn test_cascade_duration() {
        let metrics = Metrics::new();

        // Record cascade durations
        metrics.record_cascade_duration("Deposited", std::time::Duration::from_millis(50));
        metrics.record_cascade_duration("Deposited", std::time::Duration::from_millis(75));
        metrics.record_cascade_duration("SharePriceChanged", std::time::Duration::from_millis(30));

        // Get Prometheus output
        let output = Metrics::get_prometheus_metrics().expect("Should generate metrics");

        // Verify cascade metrics are present
        assert!(output.contains("redis_postgres_sync_cascade_processing_duration_seconds"));
        assert!(output.contains("event_type=\"Deposited\""));
        assert!(output.contains("event_type=\"SharePriceChanged\""));

        // Verify histogram counts
        assert!(output.contains("redis_postgres_sync_cascade_processing_duration_seconds_count{event_type=\"Deposited\"} 2"));
        assert!(output.contains("redis_postgres_sync_cascade_processing_duration_seconds_count{event_type=\"SharePriceChanged\"} 1"));
    }

    #[test]
    fn test_database_operations() {
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
        assert!(output.contains("redis_postgres_sync_database_operations_total"));

        // Verify different event types and operations
        assert!(output.contains("event_type=\"Deposited\""));
        assert!(output.contains("operation=\"position_update\""));
        assert!(output.contains("operation=\"vault_aggregation\""));
        assert!(output.contains("event_type=\"SharePriceChanged\""));
        assert!(output.contains("operation=\"vault_update\""));
        assert!(output.contains("event_type=\"AtomCreated\""));
        assert!(output.contains("operation=\"term_initialization\""));

        // Verify counts
        assert!(output.contains("redis_postgres_sync_database_operations_total{event_type=\"Deposited\",operation=\"position_update\"} 2"));
        assert!(output.contains("redis_postgres_sync_database_operations_total{event_type=\"Deposited\",operation=\"vault_aggregation\"} 1"));
    }

    #[test]
    fn test_cascade_failure() {
        let metrics = Metrics::new();

        // Record cascade failures
        metrics.record_cascade_failure("Deposited");
        metrics.record_cascade_failure("Deposited");
        metrics.record_cascade_failure("SharePriceChanged");

        // Get Prometheus output
        let output = Metrics::get_prometheus_metrics().expect("Should generate metrics");

        // Verify cascade failure metrics are present
        assert!(output.contains("redis_postgres_sync_cascade_failures_total"));
        assert!(output.contains("event_type=\"Deposited\""));
        assert!(output.contains("event_type=\"SharePriceChanged\""));

        // Verify counts
        assert!(output.contains("redis_postgres_sync_cascade_failures_total{event_type=\"Deposited\"} 2"));
        assert!(output.contains("redis_postgres_sync_cascade_failures_total{event_type=\"SharePriceChanged\"} 1"));
    }

    #[test]
    fn test_multiple_event_types() {
        let metrics = Metrics::new();

        // Simulate processing multiple different event types
        let event_types = vec!["AtomCreated", "Deposited", "SharePriceChanged", "TripleCreated", "Redeemed"];

        for event_type in &event_types {
            metrics.record_event_by_type_success(event_type);
            metrics.record_event_processing_duration(event_type, std::time::Duration::from_millis(100));
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
            assert!(output.contains(&format!("event_type=\"{}\"", event_type)));
        }

        // Verify different metric types
        assert!(output.contains("redis_postgres_sync_events_processed_by_type_total"));
        assert!(output.contains("redis_postgres_sync_events_failed_by_type_total"));
        assert!(output.contains("redis_postgres_sync_event_processing_duration_by_type_seconds"));
        assert!(output.contains("redis_postgres_sync_cascade_processing_duration_seconds"));
        assert!(output.contains("redis_postgres_sync_cascade_failures_total"));
    }
}