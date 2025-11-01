use crate::helpers::{DbAssertions, EventBuilder, TestHarness};
use redis_postgres_sync::{config::Config, core::pipeline::EventProcessingPipeline};

#[tokio::test]
#[ignore] // Run with --ignored flag since it requires containers
async fn test_deposits_processed_correctly_despite_out_of_order_arrival() {
    // Setup test harness with isolated containers
    let harness = TestHarness::new().await.unwrap();

    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let account_id = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";
    let curve_id = "0x0000000000000000000000000000000000000000000000000000000000000000";

    // Step 1: Create atom first (required for deposits)
    let atom_event = EventBuilder::new()
        .with_block(1000)
        .atom_created(term_id, account_id);

    harness
        .publish_events("rindexer_producer", vec![atom_event])
        .await
        .unwrap();

    // Step 2: Publish deposits OUT OF ORDER
    // The trigger should use the LATEST event (block 1005), but accumulate all deposits
    let events = vec![
        // Block 1005 arrives first (this should be the final state)
        EventBuilder::new()
            .with_block(1005)
            .with_log_index(0)
            .deposited(account_id, term_id, 5000, 5000),
        // Block 1001 arrives second (older! should be ignored for final state)
        EventBuilder::new()
            .with_block(1001)
            .with_log_index(0)
            .deposited(account_id, term_id, 1000, 1000),
        // Block 1003 arrives third (also older than 1005)
        EventBuilder::new()
            .with_block(1003)
            .with_log_index(0)
            .deposited(account_id, term_id, 3000, 3000),
    ];

    harness
        .publish_events("rindexer_producer", events)
        .await
        .unwrap();

    // Step 3: Start pipeline
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
        http_port: 0, // Random port
        consumer_group_suffix: None,
        analytics_stream_name: "term_updates".to_string(),
    };

    let pipeline = EventProcessingPipeline::new(config).await.unwrap();
    let pipeline_handle = tokio::spawn({
        let pipeline = pipeline.clone();
        async move { pipeline.start().await }
    });

    // Step 4: Wait for processing
    harness.wait_for_processing(4, 15).await.unwrap();

    // Give it a bit more time to ensure cascade processing completes
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Step 5: Assertions
    let pool = harness.get_pool().await.unwrap();

    // Should use the LATEST event (block 1005) for position shares
    let position = DbAssertions::assert_position_exists(&pool, account_id, term_id, curve_id)
        .await
        .unwrap();

    // The shares should be from block 1005 (the latest block)
    assert_eq!(
        position.shares, "5000",
        "Position shares should be from block 1005"
    );

    // Get the last deposit block info
    let (last_block, last_log_index) =
        DbAssertions::get_position_last_deposit_info(&pool, account_id, term_id, curve_id)
            .await
            .unwrap();

    assert_eq!(
        last_block, 1005,
        "Last deposit block should be 1005 (the latest)"
    );
    assert_eq!(last_log_index, 0, "Last deposit log index should be 0");

    // Total deposits should accumulate ALL events regardless of order
    assert_eq!(
        position.total_deposit_assets_after_total_fees, "9000",
        "Total deposits should sum all events: 5000 + 1000 + 3000"
    );

    // Check that all events were stored
    DbAssertions::assert_total_events(&pool, 4)
        .await
        .unwrap();

    // Check that vault was updated
    let vault = DbAssertions::assert_vault_state(&pool, term_id, curve_id, 1)
        .await
        .unwrap();

    assert_eq!(
        vault.position_count, 1,
        "Vault should have 1 position"
    );

    // Cleanup
    pipeline.stop().await.unwrap();
    pipeline_handle.abort();
}

#[tokio::test]
#[ignore] // Run with --ignored flag since it requires containers
async fn test_share_price_changes_use_latest_block() {
    let harness = TestHarness::new().await.unwrap();

    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let account_id = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";
    let curve_id = "0x0000000000000000000000000000000000000000000000000000000000000000";

    // Create atom and initial deposit
    let mut events = vec![
        EventBuilder::new()
            .with_block(1000)
            .atom_created(term_id, account_id),
        EventBuilder::new()
            .with_block(1001)
            .deposited(account_id, term_id, 10000, 10000),
    ];

    // Add price changes OUT OF ORDER
    events.extend(vec![
        // Block 1008 arrives first (2.0 ETH - this should be final price)
        EventBuilder::new()
            .with_block(1008)
            .with_log_index(0)
            .share_price_changed(term_id, 2000000000000000000),
        // Block 1002 arrives second (1.2 ETH - should be ignored)
        EventBuilder::new()
            .with_block(1002)
            .with_log_index(0)
            .share_price_changed(term_id, 1200000000000000000),
        // Block 1005 arrives third (1.5 ETH - should be ignored)
        EventBuilder::new()
            .with_block(1005)
            .with_log_index(0)
            .share_price_changed(term_id, 1500000000000000000),
    ]);

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
    harness.wait_for_processing(5, 15).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Assertions
    let pool = harness.get_pool().await.unwrap();

    // Vault should have the price from block 1008 (the latest)
    let vault = DbAssertions::assert_vault_state(&pool, term_id, curve_id, 1)
        .await
        .unwrap();

    assert_eq!(
        vault.current_share_price, "2000000000000000000",
        "Current share price should be 2.0 ETH (from block 1008)"
    );

    // Get the last price event info
    let (last_block, last_log_index) =
        DbAssertions::get_vault_last_price_info(&pool, term_id, curve_id)
            .await
            .unwrap();

    assert_eq!(last_block, 1008, "Last price block should be 1008");
    assert_eq!(last_log_index, 0, "Last price log index should be 0");

    // Cleanup
    pipeline.stop().await.unwrap();
    pipeline_handle.abort();
}
