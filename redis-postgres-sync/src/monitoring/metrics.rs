use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use prometheus::{Counter, Gauge, Histogram, Registry, Encoder, TextEncoder};
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
}