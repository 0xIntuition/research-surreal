use serde_json::json;
use std::convert::Infallible;
use std::sync::Arc;
use tracing::info;
use warp::Filter;

use crate::core::EventProcessingPipeline;
use crate::monitoring::Metrics;

pub struct HttpServer {
    pipeline: Arc<EventProcessingPipeline>,
    port: u16,
}

impl HttpServer {
    pub fn new(pipeline: Arc<EventProcessingPipeline>, port: u16) -> Self {
        Self { pipeline, port }
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let pipeline = self.pipeline.clone();

        let health = warp::path("health").and(warp::get()).and_then(move || {
            let pipeline = pipeline.clone();
            async move {
                let health = pipeline.health().await;
                if health.healthy {
                    Ok::<_, warp::Rejection>(warp::reply::with_status(
                        warp::reply::json(&health),
                        warp::http::StatusCode::OK,
                    ))
                } else {
                    Ok::<_, warp::Rejection>(warp::reply::with_status(
                        warp::reply::json(&health),
                        warp::http::StatusCode::SERVICE_UNAVAILABLE,
                    ))
                }
            }
        });

        let metrics = warp::path("metrics").and(warp::get()).map(|| {
            match Metrics::get_prometheus_metrics() {
                Ok(metrics_text) => warp::reply::with_header(
                    metrics_text,
                    "content-type",
                    "text/plain; version=0.0.4; charset=utf-8",
                ),
                Err(e) => {
                    tracing::error!("Failed to generate Prometheus metrics: {}", e);
                    warp::reply::with_header(
                        "# Unable to generate metrics".to_string(),
                        "content-type",
                        "text/plain; version=0.0.4; charset=utf-8",
                    )
                }
            }
        });

        // JSON metrics endpoint for backwards compatibility
        let metrics_pipeline = self.pipeline.clone();
        let json_metrics = warp::path("metrics")
            .and(warp::path("json"))
            .and(warp::get())
            .and_then(move || {
                let pipeline = metrics_pipeline.clone();
                async move {
                    let health = pipeline.health().await;
                    let snapshot = pipeline.metrics.get_snapshot().await;
                    let response = json!({
                        "events_processed": health.metrics.total_events_processed,
                        "events_failed": health.metrics.total_events_failed,
                        "redis_healthy": health.redis_consumer_healthy,
                        "postgres_healthy": health.postgres_sync_healthy,
                        "uptime_seconds": snapshot.uptime_seconds,
                        "events_per_second": snapshot.events_per_second,
                        "peak_events_per_second": snapshot.peak_events_per_second,
                        "last_check": health.last_check
                    });

                    Ok::<_, Infallible>(warp::reply::json(&response))
                }
            });

        let routes = health.or(metrics).or(json_metrics);

        info!("Starting HTTP server on port {}", self.port);
        warp::serve(routes).run(([0, 0, 0, 0], self.port)).await;

        Ok(())
    }
}
