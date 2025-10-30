use redis_postgres_sync::{Config, EventProcessingPipeline, HttpServer};
use std::sync::Arc;
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "redis_postgres_sync=info,warn".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting Redis to PostgreSQL sync pipeline");

    // Load configuration from environment
    let config = Config::from_env().map_err(|e| {
        error!("Failed to load configuration: {}", e);
        e
    })?;

    config.validate().map_err(|e| {
        error!("Configuration validation failed: {}", e);
        e
    })?;

    info!("Configuration loaded successfully");
    info!("Redis URL: {}", config.redis_url);
    info!("Database URL: {}", config.database_url);
    info!("Streams: {:?}", config.stream_names);
    info!("Batch size: {}, Workers: {}", config.batch_size, config.workers);

    // Create pipeline
    let pipeline = Arc::new(
        EventProcessingPipeline::new(config.clone())
            .await
            .map_err(|e| {
                error!("Failed to create pipeline: {}", e);
                e
            })?
    );

    // Start HTTP server for health checks and metrics
    let http_server = HttpServer::new(pipeline.clone(), config.http_port);
    let http_handle = tokio::spawn(async move {
        if let Err(e) = http_server.start().await {
            error!("HTTP server error: {}", e);
        }
    });

    // Set up graceful shutdown
    let shutdown_pipeline = pipeline.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl-c");
        warn!("Received Ctrl-C, initiating graceful shutdown...");
        if let Err(e) = shutdown_pipeline.stop().await {
            error!("Error during shutdown: {}", e);
        }
        http_handle.abort();
    });

    // Start pipeline
    info!("Pipeline initialized successfully, starting event processing...");
    match pipeline.start().await {
        Ok(()) => {
            info!("Pipeline shut down gracefully");
            Ok(())
        }
        Err(e) => {
            error!("Pipeline error: {}", e);
            Err(e.into())
        }
    }
}
