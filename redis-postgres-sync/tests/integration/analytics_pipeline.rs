use crate::helpers::{DbAssertions, EventBuilder, TestHarness};
use redis_postgres_sync::{config::Config, core::pipeline::EventProcessingPipeline};

#[tokio::test]
#[ignore] // Run with --ignored flag since it requires containers
async fn test_atom_creation_initializes_term() {
    let harness = TestHarness::new().await.unwrap();

    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let creator = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";

    // Create atom
    let events = vec![EventBuilder::new()
        .with_block(1000)
        .atom_created(term_id, creator)];

    harness
        .publish_events("rindexer_producer", events)
        .await
        .unwrap();

    // Start pipeline
    let config = Config {
        redis_url: harness.redis_url().to_string(),
        database_url: harness.database_url().to_string(),
        stream_names: vec!["rindexer_producer".to_string()],
        consumer_group: "test-group".to_string(),
        consumer_name: "test-consumer".to_string(),
        batch_size: 10,
        batch_timeout_ms: 1000,
        workers: 1,
        processing_timeout_ms: 5000,
        max_retries: 3,
        circuit_breaker_threshold: 10,
        circuit_breaker_timeout_ms: 60000,
        http_port: 0,
        consumer_group_suffix: None,
        analytics_stream_name: "term_updates".to_string(),
    };

    let pipeline = EventProcessingPipeline::new(config).await.unwrap();
    let pipeline_handle = tokio::spawn({
        let pipeline = pipeline.clone();
        async move { pipeline.start().await }
    });

    // Wait for processing
    harness.wait_for_processing(1, 10).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Assertions
    let pool = harness.get_pool().await.unwrap();

    // Check that atom was created
    DbAssertions::assert_atom_exists(&pool, term_id, creator)
        .await
        .unwrap();

    // Check that term was initialized by cascade processor
    let term = DbAssertions::assert_term_aggregation(&pool, term_id, "Atom")
        .await
        .unwrap();

    // Initial values should be zero
    assert_eq!(
        term.total_assets, "0",
        "New atom should have 0 total assets"
    );
    assert_eq!(
        term.total_market_cap, "0",
        "New atom should have 0 market cap"
    );

    // Cleanup
    pipeline.stop().await.unwrap();
    pipeline_handle.abort();
}

#[tokio::test]
#[ignore] // Run with --ignored flag since it requires containers
async fn test_triple_creation_initializes_terms() {
    let harness = TestHarness::new().await.unwrap();

    let subject_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let predicate_id = "0x0000000000000000000000000000000000000000000000000000000000000002";
    let object_id = "0x0000000000000000000000000000000000000000000000000000000000000003";
    let triple_id = "0x0000000000000000000000000000000000000000000000000000000000000010";
    let creator = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";

    // Create atoms first
    let mut events = vec![
        EventBuilder::new()
            .with_block(1000)
            .atom_created(subject_id, creator),
        EventBuilder::new()
            .with_block(1001)
            .atom_created(predicate_id, creator),
        EventBuilder::new()
            .with_block(1002)
            .atom_created(object_id, creator),
    ];

    // Create triple
    events.push(
        EventBuilder::new().with_block(1003).triple_created(
            triple_id,
            subject_id,
            predicate_id,
            object_id,
        ),
    );

    harness
        .publish_events("rindexer_producer", events)
        .await
        .unwrap();

    // Start pipeline
    let config = Config {
        redis_url: harness.redis_url().to_string(),
        database_url: harness.database_url().to_string(),
        stream_names: vec!["rindexer_producer".to_string()],
        consumer_group: "test-group".to_string(),
        consumer_name: "test-consumer".to_string(),
        batch_size: 10,
        batch_timeout_ms: 1000,
        workers: 1,
        processing_timeout_ms: 5000,
        max_retries: 3,
        circuit_breaker_threshold: 10,
        circuit_breaker_timeout_ms: 60000,
        http_port: 0,
        consumer_group_suffix: None,
        analytics_stream_name: "term_updates".to_string(),
    };

    let pipeline = EventProcessingPipeline::new(config).await.unwrap();
    let pipeline_handle = tokio::spawn({
        let pipeline = pipeline.clone();
        async move { pipeline.start().await }
    });

    // Wait for processing
    harness.wait_for_processing(4, 15).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Assertions
    let pool = harness.get_pool().await.unwrap();

    // Check that triple was created
    let triple = DbAssertions::assert_triple_exists(
        &pool,
        triple_id,
        subject_id,
        predicate_id,
        object_id,
    )
    .await
    .unwrap();

    // Check that the triple term was initialized
    DbAssertions::assert_term_aggregation(&pool, triple_id, "Triple")
        .await
        .unwrap();

    // Check that counter term was also initialized
    let counter_term_id = &triple.counter_term_id;
    DbAssertions::assert_term_aggregation(&pool, counter_term_id, "Triple")
        .await
        .unwrap();

    // Cleanup
    pipeline.stop().await.unwrap();
    pipeline_handle.abort();
}

#[tokio::test]
#[ignore] // Run with --ignored flag since it requires containers
async fn test_deposits_update_term_aggregations() {
    let harness = TestHarness::new().await.unwrap();

    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let account_id = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";

    // Create atom and deposit
    let events = vec![
        EventBuilder::new()
            .with_block(1000)
            .atom_created(term_id, account_id),
        EventBuilder::new()
            .with_block(1001)
            .deposited(account_id, term_id, 10000, 10000),
    ];

    harness
        .publish_events("rindexer_producer", events)
        .await
        .unwrap();

    // Start pipeline
    let config = Config {
        redis_url: harness.redis_url().to_string(),
        database_url: harness.database_url().to_string(),
        stream_names: vec!["rindexer_producer".to_string()],
        consumer_group: "test-group".to_string(),
        consumer_name: "test-consumer".to_string(),
        batch_size: 10,
        batch_timeout_ms: 1000,
        workers: 1,
        processing_timeout_ms: 5000,
        max_retries: 3,
        circuit_breaker_threshold: 10,
        circuit_breaker_timeout_ms: 60000,
        http_port: 0,
        consumer_group_suffix: None,
        analytics_stream_name: "term_updates".to_string(),
    };

    let pipeline = EventProcessingPipeline::new(config).await.unwrap();
    let pipeline_handle = tokio::spawn({
        let pipeline = pipeline.clone();
        async move { pipeline.start().await }
    });

    // Wait for processing
    harness.wait_for_processing(2, 15).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Assertions
    let pool = harness.get_pool().await.unwrap();

    // Check term aggregation was updated
    let term = DbAssertions::assert_term_aggregation(&pool, term_id, "Atom")
        .await
        .unwrap();

    // After deposit, total_assets should be updated by cascade processor
    let total_assets = term.total_assets.parse::<i64>().unwrap();
    assert!(
        total_assets > 0,
        "Term total_assets should be updated after deposit"
    );

    // Cleanup
    pipeline.stop().await.unwrap();
    pipeline_handle.abort();
}

#[tokio::test]
#[ignore] // Run with --ignored flag since it requires containers
async fn test_share_price_changes_update_term_market_cap() {
    let harness = TestHarness::new().await.unwrap();

    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let account_id = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";

    // Create atom, deposit, and change price
    let events = vec![
        EventBuilder::new()
            .with_block(1000)
            .atom_created(term_id, account_id),
        EventBuilder::new()
            .with_block(1001)
            .deposited(account_id, term_id, 10000, 10000),
        EventBuilder::new()
            .with_block(1002)
            .share_price_changed(term_id, 2000000000000000000), // 2.0 ETH
    ];

    harness
        .publish_events("rindexer_producer", events)
        .await
        .unwrap();

    // Start pipeline
    let config = Config {
        redis_url: harness.redis_url().to_string(),
        database_url: harness.database_url().to_string(),
        stream_names: vec!["rindexer_producer".to_string()],
        consumer_group: "test-group".to_string(),
        consumer_name: "test-consumer".to_string(),
        batch_size: 10,
        batch_timeout_ms: 1000,
        workers: 1,
        processing_timeout_ms: 5000,
        max_retries: 3,
        circuit_breaker_threshold: 10,
        circuit_breaker_timeout_ms: 60000,
        http_port: 0,
        consumer_group_suffix: None,
        analytics_stream_name: "term_updates".to_string(),
    };

    let pipeline = EventProcessingPipeline::new(config).await.unwrap();
    let pipeline_handle = tokio::spawn({
        let pipeline = pipeline.clone();
        async move { pipeline.start().await }
    });

    // Wait for processing
    harness.wait_for_processing(3, 15).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Assertions
    let pool = harness.get_pool().await.unwrap();

    // Check term aggregation
    let term = DbAssertions::assert_term_aggregation(&pool, term_id, "Atom")
        .await
        .unwrap();

    // Market cap should be updated
    let market_cap = term.total_market_cap.parse::<i64>().unwrap();
    assert!(
        market_cap > 0,
        "Term market_cap should be updated after price change"
    );

    // Cleanup
    pipeline.stop().await.unwrap();
    pipeline_handle.abort();
}
