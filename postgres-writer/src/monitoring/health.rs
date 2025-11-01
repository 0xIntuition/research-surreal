use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Health status for database connections
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub healthy: bool,
    pub healthy_connections: usize,
    pub total_connections: usize,
    pub last_check: DateTime<Utc>,
    pub error_message: Option<String>,
}

impl HealthStatus {
    pub fn healthy(healthy_connections: usize, total_connections: usize) -> Self {
        Self {
            healthy: true,
            healthy_connections,
            total_connections,
            last_check: Utc::now(),
            error_message: None,
        }
    }

    pub fn unhealthy(error_message: String) -> Self {
        Self {
            healthy: false,
            healthy_connections: 0,
            total_connections: 0,
            last_check: Utc::now(),
            error_message: Some(error_message),
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy
    }
}

/// Statistics for connection pool health monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionPoolStats {
    pub total_connections: usize,
    pub healthy_connections: usize,
    pub unhealthy_connections: usize,
    pub pool_utilization: f64,
}
