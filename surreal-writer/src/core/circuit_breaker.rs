use crate::error::{Result, SyncError};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    consecutive_failures: Arc<AtomicU32>,
    max_failures: u32,
    timeout: Duration,
    last_failure: Arc<RwLock<Option<Instant>>>,
    total_failures: Arc<AtomicU64>,
    total_successes: Arc<AtomicU64>,
}

impl CircuitBreaker {
    pub fn new(max_failures: u32, timeout_ms: u64) -> Self {
        Self {
            consecutive_failures: Arc::new(AtomicU32::new(0)),
            max_failures,
            timeout: Duration::from_millis(timeout_ms),
            last_failure: Arc::new(RwLock::new(None)),
            total_failures: Arc::new(AtomicU64::new(0)),
            total_successes: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn check(&self) -> Result<()> {
        let failures = self.consecutive_failures.load(Ordering::Relaxed);

        if failures >= self.max_failures {
            let last_failure = self.last_failure.read().await;
            if let Some(instant) = *last_failure {
                if instant.elapsed() < self.timeout {
                    return Err(SyncError::CircuitBreakerOpen);
                }
            }
            // Reset after timeout
            self.consecutive_failures.store(0, Ordering::Relaxed);
        }

        Ok(())
    }

    pub async fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.total_successes.fetch_add(1, Ordering::Relaxed);
        *self.last_failure.write().await = None;
    }

    pub async fn record_failure(&self) {
        self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        self.total_failures.fetch_add(1, Ordering::Relaxed);
        *self.last_failure.write().await = Some(Instant::now());
    }

    pub fn is_open(&self) -> bool {
        self.consecutive_failures.load(Ordering::Relaxed) >= self.max_failures
    }

    pub fn get_state(&self) -> String {
        if self.is_open() {
            "open".to_string()
        } else {
            "closed".to_string()
        }
    }

    pub fn get_stats(&self) -> (u64, u64) {
        (
            self.total_successes.load(Ordering::Relaxed),
            self.total_failures.load(Ordering::Relaxed),
        )
    }
}
