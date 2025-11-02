use postgres_writer::{analytics, Config, EventProcessingPipeline, HttpServer};
use std::sync::Arc;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "postgres_writer=info,warn".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting Redis to PostgreSQL sync pipeline");

    // Load configuration from environment
    let config = Config::from_env().map_err(|e| {
        error!("Failed to load configuration: {e}");
        e
    })?;

    config.validate().map_err(|e| {
        error!("Configuration validation failed: {e}");
        e
    })?;

    info!("Configuration loaded successfully");
    info!("Redis URL: {}", config.redis_url);
    info!("Database URL: {}", config.database_url);
    info!("Streams: {:?}", config.stream_names);
    info!(
        "Batch size: {}, Workers: {}",
        config.batch_size, config.workers
    );

    // Create pipeline
    let pipeline = Arc::new(
        EventProcessingPipeline::new(config.clone())
            .await
            .map_err(|e| {
                error!("Failed to create pipeline: {e}");
                e
            })?,
    );

    // Start analytics worker
    let analytics_config = config.clone();
    let analytics_pool = pipeline.get_pool().clone();
    let analytics_token = pipeline.get_cancellation_token();
    let analytics_handle = tokio::spawn(async move {
        info!("Spawning analytics worker");
        if let Err(e) =
            analytics::start_analytics_worker(analytics_config, analytics_pool, analytics_token)
                .await
        {
            error!("Analytics worker error: {e}");
        } else {
            info!("Analytics worker stopped gracefully");
        }
    });

    // Start HTTP server for health checks and metrics
    let http_server = HttpServer::new(pipeline.clone(), config.http_port);
    let http_handle = tokio::spawn(async move {
        if let Err(e) = http_server.start().await {
            error!("HTTP server error: {e}");
        }
    });

    // Set up graceful shutdown
    let shutdown_pipeline = pipeline.clone();
    let shutdown_timeout = config.shutdown_timeout_secs;
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl-c");
        warn!("Received Ctrl-C, initiating graceful shutdown...");

        // Signal shutdown to pipeline
        if let Err(e) = shutdown_pipeline.stop().await {
            error!("Error during shutdown: {e}");
        }

        // Wait for tasks to complete gracefully with timeout
        let shutdown_duration = std::time::Duration::from_secs(shutdown_timeout);
        info!("Waiting up to {}s for tasks to complete...", shutdown_timeout);

        // Wait for analytics worker
        match tokio::time::timeout(shutdown_duration, analytics_handle).await {
            Ok(Ok(())) => info!("Analytics worker stopped gracefully"),
            Ok(Err(e)) => warn!("Analytics worker panicked: {e}"),
            Err(_) => {
                warn!("Analytics worker did not stop within timeout, forcing abort");
            }
        }

        // Wait for HTTP server (reusing same timeout)
        match tokio::time::timeout(shutdown_duration, http_handle).await {
            Ok(Ok(())) => info!("HTTP server stopped gracefully"),
            Ok(Err(e)) => warn!("HTTP server panicked: {e}"),
            Err(_) => {
                warn!("HTTP server did not stop within timeout, forcing abort");
            }
        }

        info!("Shutdown complete");
    });

    // Start pipeline
    info!("Pipeline initialized successfully, starting event processing...");
    match pipeline.start().await {
        Ok(()) => {
            info!("Pipeline shut down gracefully");
            Ok(())
        }
        Err(e) => {
            error!("Pipeline error: {e}");
            Err(e.into())
        }
    }
}
