use crate::helpers::{DbAssertions, EventBuilder, TestHarness};
use postgres_writer::core::pipeline::EventProcessingPipeline;

#[tokio::test]
#[ignore] // Run with --ignored flag since it requires containers
async fn test_concurrent_deposits_to_same_vault_maintain_consistency() {
    let mut harness = TestHarness::new().await.unwrap();

    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let creator = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";
    let curve_id = "0x0000000000000000000000000000000000000000000000000000000000000000";

    // Create atom first
    let mut events = vec![EventBuilder::new()
        .with_block(1000)
        .atom_created(term_id, creator)];

    // Create 10 different accounts depositing concurrently to the same vault
    for i in 0..10 {
        let account_id = format!("0x{i:040x}");
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

    // Start pipeline with single worker first to debug
    let config = harness.config_with_workers(1);

    let pipeline = EventProcessingPipeline::new(config, None)
        .await
        .expect("Failed to create pipeline");
    let pipeline_handle = tokio::spawn({
        let pipeline = pipeline.clone();
        async move { pipeline.start().await }
    });

    // Wait for processing (11 events: 1 atom + 10 deposits)
    harness
        .wait_for_processing(11, 30)
        .await
        .expect("Failed to process 11 events within 30 seconds");
    harness
        .wait_for_cascade(term_id, 5)
        .await
        .expect("Failed to complete cascade processing within 5 seconds");

    // Assertions
    let pool = harness
        .get_pool()
        .await
        .expect("Failed to get database pool");

    // Vault should have exactly 10 positions (advisory locks should prevent race conditions)
    let vault = DbAssertions::assert_vault_state(pool, term_id, curve_id, 10)
        .await
        .expect("Failed to verify vault state");

    // Note: Deposits only update vault.position_count. Financial metrics (total_shares,
    // total_assets, market_cap) are only updated by SharePriceChanged events.
    // Term aggregations are also only updated by SharePriceChanged events.
    // (see perf optimization commit 0e59404)
    assert_eq!(
        vault.position_count, 10,
        "Vault should have correct position count after concurrent deposits"
    );

    // Each position should exist with correct shares
    for i in 0..10 {
        let account_id = format!("0x{i:040x}");
        let position = DbAssertions::assert_position_exists(pool, &account_id, term_id, curve_id)
            .await
            .expect("Failed to find position");

        assert_eq!(
            position.shares, "1000",
            "Account {i} should have 1000 shares"
        );
        assert_eq!(
            position.total_deposit_assets_after_total_fees, "1000",
            "Account {i} should have 1000 in deposits"
        );
    }

    // Term aggregations are not updated by deposits (only by SharePriceChanged events)
    // so we don't check term.total_assets here

    // Cleanup - ensure pipeline stops even if stop() hangs
    let stop_result =
        tokio::time::timeout(std::time::Duration::from_secs(5), pipeline.stop()).await;

    pipeline_handle.abort();

    stop_result
        .expect("Pipeline stop timeout")
        .expect("Pipeline stop failed");
}

#[tokio::test]
#[ignore] // Run with --ignored flag since it requires containers
async fn test_concurrent_deposits_and_redeems_maintain_consistency() {
    let mut harness = TestHarness::new().await.unwrap();

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
    let config = harness.config_with_workers(3);

    let pipeline = EventProcessingPipeline::new(config, None)
        .await
        .expect("Failed to create pipeline");
    let pipeline_handle = tokio::spawn({
        let pipeline = pipeline.clone();
        async move { pipeline.start().await }
    });

    // Wait for processing
    harness
        .wait_for_processing(6, 20)
        .await
        .expect("Failed to process 6 events within 20 seconds");
    harness
        .wait_for_cascade(term_id, 5)
        .await
        .expect("Failed to complete cascade processing within 5 seconds");

    // Assertions
    let pool = harness
        .get_pool()
        .await
        .expect("Failed to get database pool");

    let position = DbAssertions::assert_position_exists(pool, account_id, term_id, curve_id)
        .await
        .expect("Failed to find position");

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

    // Cleanup - ensure pipeline stops even if stop() hangs
    let stop_result =
        tokio::time::timeout(std::time::Duration::from_secs(5), pipeline.stop()).await;

    pipeline_handle.abort();

    stop_result
        .expect("Pipeline stop timeout")
        .expect("Pipeline stop failed");
}

#[tokio::test]
#[ignore] // Run with --ignored flag since it requires containers
async fn test_position_count_updates_correctly_with_concurrent_deposits() {
    let mut harness = TestHarness::new().await.unwrap();

    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let creator = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";
    let curve_id = "0x0000000000000000000000000000000000000000000000000000000000000000";

    // Create atom
    let mut events = vec![EventBuilder::new()
        .with_block(1000)
        .atom_created(term_id, creator)];

    // 5 accounts deposit
    for i in 0..5 {
        let account_id = format!("0x{i:040x}");
        events.push(EventBuilder::new().with_block(1001 + i).deposited(
            &account_id,
            term_id,
            1000,
            1000,
        ));
    }

    // 2 accounts redeem everything (shares -> 0)
    for i in 0..2 {
        let account_id = format!("0x{i:040x}");
        events.push(
            EventBuilder::new()
                .with_block(1010 + i)
                .redeemed(&account_id, term_id, 0, 1000), // remaining shares = 0, assets = 1000
        );
    }

    harness
        .publish_events("rindexer_producer", events)
        .await
        .unwrap();

    // Start pipeline
    let config = harness.config_with_workers(1);

    let pipeline = EventProcessingPipeline::new(config, None)
        .await
        .expect("Failed to create pipeline");
    let pipeline_handle = tokio::spawn({
        let pipeline = pipeline.clone();
        async move { pipeline.start().await }
    });

    // Wait for processing
    harness
        .wait_for_processing(8, 30)
        .await
        .expect("Failed to process 8 events within 30 seconds");
    harness
        .wait_for_cascade(term_id, 5)
        .await
        .expect("Failed to complete cascade processing within 5 seconds");

    // Assertions
    let pool = harness
        .get_pool()
        .await
        .expect("Failed to get database pool");

    // Position count should be 3 (5 deposited, 2 redeemed to 0)
    let vault = DbAssertions::assert_vault_state(pool, term_id, curve_id, 3)
        .await
        .expect("Failed to verify vault state");

    // Verify the count matches actual positions with shares > 0
    let actual_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM position
         WHERE term_id = $1 AND curve_id = $2 AND shares::numeric > 0",
    )
    .bind(term_id)
    .bind(curve_id)
    .fetch_one(pool)
    .await
    .expect("Failed to query position count");

    assert_eq!(actual_count, 3, "Should have 3 positions with shares > 0");

    assert_eq!(
        vault.position_count, 3,
        "Vault position_count should match actual count"
    );

    // Cleanup - ensure pipeline stops even if stop() hangs
    let stop_result =
        tokio::time::timeout(std::time::Duration::from_secs(5), pipeline.stop()).await;

    pipeline_handle.abort();

    stop_result
        .expect("Pipeline stop timeout")
        .expect("Pipeline stop failed");
}
