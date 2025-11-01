use crate::helpers::{DbAssertions, EventBuilder, TestHarness};
use redis_postgres_sync::{config::Config, core::pipeline::EventProcessingPipeline};

#[tokio::test]
#[ignore] // Run with --ignored flag since it requires containers
async fn test_concurrent_deposits_to_same_vault_maintain_consistency() {
    let harness = TestHarness::new().await.unwrap();

    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let creator = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";
    let curve_id = "0x0000000000000000000000000000000000000000000000000000000000000000";

    // Create atom first
    let mut events = vec![EventBuilder::new()
        .with_block(1000)
        .atom_created(term_id, creator)];

    // Create 10 different accounts depositing concurrently to the same vault
    for i in 0..10 {
        let account_id = format!("0x{:040x}", i);
        events.push(
            EventBuilder::new()
                .with_block(1000 + i + 1)
                .with_log_index(0)
                .deposited(&account_id, term_id, 1000, 1000),
        );
    }

    // Publish all at once to simulate concurrent arrival
    harness
        .publish_events("rindexer_producer", events)
        .await
        .unwrap();

    // Start pipeline with multiple workers for concurrency
    let config = Config {
        redis_url: harness.redis_url().to_string(),
        database_url: harness.database_url().to_string(),
        stream_names: vec!["rindexer_producer".to_string()],
        consumer_group: "test-group".to_string(),
        consumer_name: "test-consumer".to_string(),
        batch_size: 10,
        batch_timeout_ms: 1000,
        workers: 4, // Multiple workers to test concurrency
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

    // Wait for processing (11 events: 1 atom + 10 deposits)
    harness.wait_for_processing(11, 20).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Assertions
    let pool = harness.get_pool().await.unwrap();

    // Vault should have exactly 10 positions (advisory locks should prevent race conditions)
    let vault = DbAssertions::assert_vault_state(&pool, term_id, curve_id, 10)
        .await
        .unwrap();

    // Total shares should be sum of all deposits
    assert_eq!(
        vault.total_shares, "10000",
        "Total shares should be 10 * 1000"
    );

    // Each position should exist with correct shares
    for i in 0..10 {
        let account_id = format!("0x{:040x}", i);
        let position = DbAssertions::assert_position_exists(&pool, &account_id, term_id, curve_id)
            .await
            .unwrap();

        assert_eq!(
            position.shares, "1000",
            "Account {} should have 1000 shares",
            i
        );
        assert_eq!(
            position.total_deposit_assets_after_total_fees, "1000",
            "Account {} should have 1000 in deposits",
            i
        );
    }

    // Check term aggregation was updated correctly
    let term = DbAssertions::assert_term_aggregation(&pool, term_id, "Atom")
        .await
        .unwrap();

    // Total assets should reflect all deposits
    assert!(
        term.total_assets.parse::<i64>().unwrap() > 0,
        "Term should have aggregated assets"
    );

    // Cleanup
    pipeline.stop().await.unwrap();
    pipeline_handle.abort();
}

#[tokio::test]
#[ignore] // Run with --ignored flag since it requires containers
async fn test_concurrent_deposits_and_redeems_maintain_consistency() {
    let harness = TestHarness::new().await.unwrap();

    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let account_id = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";
    let creator = account_id;
    let curve_id = "0x0000000000000000000000000000000000000000000000000000000000000000";

    // Create atom and make initial deposits
    let mut events = vec![
        EventBuilder::new()
            .with_block(1000)
            .atom_created(term_id, creator),
        // Multiple deposits
        EventBuilder::new()
            .with_block(1001)
            .deposited(account_id, term_id, 5000, 5000),
        EventBuilder::new()
            .with_block(1002)
            .deposited(account_id, term_id, 3000, 3000),
        EventBuilder::new()
            .with_block(1003)
            .deposited(account_id, term_id, 2000, 2000),
    ];

    // Add redeems that could race with deposits
    events.extend(vec![
        EventBuilder::new()
            .with_block(1004)
            .redeemed(account_id, term_id, 4000, 4000),
        EventBuilder::new()
            .with_block(1005)
            .redeemed(account_id, term_id, 2000, 2000),
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
        workers: 3,
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
    harness.wait_for_processing(6, 20).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Assertions
    let pool = harness.get_pool().await.unwrap();

    let position = DbAssertions::assert_position_exists(&pool, account_id, term_id, curve_id)
        .await
        .unwrap();

    // Final shares should be from the last redeem (block 1005)
    assert_eq!(
        position.shares, "2000",
        "Final shares should be 2000 from last redeem"
    );

    // Total deposits: 5000 + 3000 + 2000 = 10000
    assert_eq!(
        position.total_deposit_assets_after_total_fees, "10000",
        "Total deposits should sum to 10000"
    );

    // Total redeems: 4000 + 2000 = 6000
    assert_eq!(
        position.total_redeem_assets_for_receiver, "6000",
        "Total redeems should sum to 6000"
    );

    // Cleanup
    pipeline.stop().await.unwrap();
    pipeline_handle.abort();
}

#[tokio::test]
#[ignore] // Run with --ignored flag since it requires containers
async fn test_position_count_updates_correctly_with_concurrent_deposits() {
    let harness = TestHarness::new().await.unwrap();

    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let creator = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";
    let curve_id = "0x0000000000000000000000000000000000000000000000000000000000000000";

    // Create atom
    let mut events = vec![EventBuilder::new()
        .with_block(1000)
        .atom_created(term_id, creator)];

    // 5 accounts deposit
    for i in 0..5 {
        let account_id = format!("0x{:040x}", i);
        events.push(
            EventBuilder::new()
                .with_block(1001 + i)
                .deposited(&account_id, term_id, 1000, 1000),
        );
    }

    // 2 accounts redeem everything (shares -> 0)
    for i in 0..2 {
        let account_id = format!("0x{:040x}", i);
        events.push(
            EventBuilder::new()
                .with_block(1010 + i)
                .redeemed(&account_id, term_id, 1000, 0), // shares become 0
        );
    }

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
        workers: 4,
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
    harness.wait_for_processing(8, 20).await.unwrap();
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Assertions
    let pool = harness.get_pool().await.unwrap();

    // Position count should be 3 (5 deposited, 2 redeemed to 0)
    let vault = DbAssertions::assert_vault_state(&pool, term_id, curve_id, 3)
        .await
        .unwrap();

    // Verify the count matches actual positions with shares > 0
    let actual_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM position
         WHERE term_id = $1 AND curve_id = $2 AND shares::numeric > 0",
    )
    .bind(term_id)
    .bind(curve_id)
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(
        actual_count, 3,
        "Should have 3 positions with shares > 0"
    );

    assert_eq!(
        vault.position_count, 3,
        "Vault position_count should match actual count"
    );

    // Cleanup
    pipeline.stop().await.unwrap();
    pipeline_handle.abort();
}
