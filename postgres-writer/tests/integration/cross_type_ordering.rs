use crate::helpers::{DbAssertions, EventBuilder, TestHarness};
use postgres_writer::core::pipeline::EventProcessingPipeline;

/// Test that verifies position shares are correctly updated when deposits and redeems
/// arrive out of order. This is the CRITICAL test case from the report.
///
/// Scenario:
/// 1. Deposit@1000 arrives first (shares=5000)
/// 2. Redeem@1005 arrives second (shares=3000) - should be final state
/// 3. Deposit@1003 arrives third (out of order, shares=4000)
///
/// Expected: position.shares = 3000 (from Redeem@1005, the most recent event)
/// Bug if: position.shares = 4000 (incorrectly using Deposit@1003)
#[tokio::test]
#[ignore] // Run with --ignored flag since it requires containers
async fn test_interleaved_deposits_and_redeems_out_of_order() {
    let mut harness = TestHarness::new().await.unwrap();

    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let account_id = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";
    let curve_id = "0x0000000000000000000000000000000000000000000000000000000000000000";

    // Step 1: Create atom
    let atom_event = EventBuilder::new()
        .with_block(1000)
        .atom_created(term_id, account_id);

    harness
        .publish_events("rindexer_producer", vec![atom_event])
        .await
        .unwrap();

    // Step 2: Publish interleaved deposits and redeems OUT OF ORDER
    // This tests the critical fix: unified tracking across event types
    //
    // Chronological order:
    // @1000: Deposit 5000 shares → balance: 5000
    // @1003: Deposit 4000 shares → balance: 9000 (5000 + 4000)
    // @1005: Redeem 6000 shares → balance: 3000 (9000 - 6000)
    //
    // Arrival order: 1000, 1005, 1003 (out of order!)
    let events = vec![
        // Deposit at block 1000 (arrives first)
        EventBuilder::new()
            .with_block(1000)
            .with_log_index(0)
            .deposited_with_total(account_id, term_id, 5000, 5000, 5000), // +5000 → balance: 5000
        // Redeem at block 1005 (arrives second) - LATEST EVENT
        EventBuilder::new()
            .with_block(1005)
            .with_log_index(0)
            .redeemed_with_total(account_id, term_id, 6000, 6000, 3000), // -6000 → balance: 3000
        // Deposit at block 1003 (arrives third but is OLDER than the redeem)
        EventBuilder::new()
            .with_block(1003)
            .with_log_index(0)
            .deposited_with_total(account_id, term_id, 4000, 4000, 9000), // +4000 → balance: 9000
    ];

    harness
        .publish_events("rindexer_producer", events)
        .await
        .unwrap();

    // Step 3: Start pipeline
    let config = harness.default_config();
    let pipeline = EventProcessingPipeline::new(config)
        .await
        .expect("Failed to create pipeline");
    let pipeline_handle = tokio::spawn({
        let pipeline = pipeline.clone();
        async move { pipeline.start().await }
    });

    // Step 4: Wait for processing
    harness
        .wait_for_processing(4, 15)
        .await
        .expect("Failed to process 4 events within 15 seconds");

    harness
        .wait_for_cascade(term_id, 5)
        .await
        .expect("Failed to complete cascade processing within 5 seconds");

    // Step 5: Assertions
    let pool = harness
        .get_pool()
        .await
        .expect("Failed to get database pool");

    let position = DbAssertions::assert_position_exists(pool, account_id, term_id, curve_id)
        .await
        .expect("Failed to find position");

    // CRITICAL ASSERTION: Position shares should be from block 1005 (the redeem)
    // NOT from block 1003 (the later-arriving deposit)
    assert_eq!(
        position.shares, "3000",
        "Position shares should be 3000 from Redeem@1005 (latest event), not 4000 from Deposit@1003"
    );

    // Verify deposit total accumulated correctly (all deposits regardless of order)
    assert_eq!(
        position.total_deposit_assets_after_total_fees, "9000",
        "Total deposits should be 5000 + 4000 = 9000"
    );

    // Verify redeem total accumulated correctly
    assert_eq!(
        position.total_redeem_assets_for_receiver, "6000",
        "Total redeems should be 6000"
    );

    // Cleanup
    let stop_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        pipeline.stop()
    )
    .await;

    pipeline_handle.abort();

    stop_result
        .expect("Pipeline stop timeout")
        .expect("Pipeline stop failed");
}

/// Test that verifies events in the same block are ordered by log_index
/// across different event types (deposits and redeems).
#[tokio::test]
#[ignore]
async fn test_same_block_different_log_indices_across_types() {
    let mut harness = TestHarness::new().await.unwrap();

    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let account_id = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";
    let curve_id = "0x0000000000000000000000000000000000000000000000000000000000000000";

    // Create atom and initial deposit
    let setup_events = vec![
        EventBuilder::new()
            .with_block(999)
            .atom_created(term_id, account_id),
        // Initial deposit before the same-block events
        EventBuilder::new()
            .with_block(999)
            .with_log_index(1)
            .deposited_with_total(account_id, term_id, 10000, 10000, 10000), // Starting balance: 10000
    ];

    harness
        .publish_events("rindexer_producer", setup_events)
        .await
        .unwrap();

    // All events in same block (1000) but different log indices
    // Starting balance: 10000 (from block 999)
    // Chronological order within block 1000 (by log_index):
    // log_index 1: Redeem  3000 shares → balance: 7000 (10000 - 3000)
    // log_index 2: Deposit 1000 shares → balance: 8000 (7000 + 1000)
    // log_index 5: Deposit 1000 shares → balance: 9000 (8000 + 1000) <- LATEST
    //
    // Arrival order: 2, 5, 1 (out of order by log_index)
    let events = vec![
        // Deposit at log_index 2 (arrives first)
        EventBuilder::new()
            .with_block(1000)
            .with_log_index(2)
            .deposited_with_total(account_id, term_id, 1000, 1000, 8000), // +1000 → balance: 8000
        // Deposit at log_index 5 (should be final state)
        EventBuilder::new()
            .with_block(1000)
            .with_log_index(5)
            .deposited_with_total(account_id, term_id, 1000, 1000, 9000), // +1000 → balance: 9000 (LATEST)
        // Redeem at log_index 1 (arrives last but has lowest log_index)
        EventBuilder::new()
            .with_block(1000)
            .with_log_index(1)
            .redeemed_with_total(account_id, term_id, 3000, 3000, 7000), // -3000 → balance: 7000
    ];

    harness
        .publish_events("rindexer_producer", events)
        .await
        .unwrap();

    let config = harness.default_config();
    let pipeline = EventProcessingPipeline::new(config)
        .await
        .expect("Failed to create pipeline");
    let pipeline_handle = tokio::spawn({
        let pipeline = pipeline.clone();
        async move { pipeline.start().await }
    });

    harness
        .wait_for_processing(5, 15)
        .await
        .expect("Failed to process events"); // 1 atom + 1 initial deposit + 3 same-block events

    harness
        .wait_for_cascade(term_id, 5)
        .await
        .expect("Failed to complete cascade");

    let pool = harness.get_pool().await.expect("Failed to get pool");
    let position = DbAssertions::assert_position_exists(pool, account_id, term_id, curve_id)
        .await
        .expect("Failed to find position");

    // Should use event with highest log_index (5) in the same block
    assert_eq!(
        position.shares, "9000",
        "Position shares should be from log_index 5 (highest in block 1000)"
    );

    // Cleanup
    pipeline_handle.abort();
    let _ = pipeline.stop().await;
}

/// Test multiple interleaved deposits and redeems arriving in random order.
/// This is a comprehensive chaos test to ensure the unified tracking works correctly.
#[tokio::test]
#[ignore]
async fn test_multiple_interleaved_events_random_order() {
    let mut harness = TestHarness::new().await.unwrap();

    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let account_id = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";
    let curve_id = "0x0000000000000000000000000000000000000000000000000000000000000000";

    // Create atom
    let atom_event = EventBuilder::new()
        .with_block(999)
        .atom_created(term_id, account_id);

    harness
        .publish_events("rindexer_producer", vec![atom_event])
        .await
        .unwrap();

    // Chronological order:
    // Block 1001: Deposit 1000 shares → balance: 1000
    // Block 1003: Deposit 3000 shares → balance: 4000 (1000 + 3000)
    // Block 1005: Redeem  2000 shares → balance: 2000 (4000 - 2000)
    // Block 1007: Deposit 3000 shares → balance: 5000 (2000 + 3000)
    // Block 1010: Redeem  4000 shares → balance: 1000 (5000 - 4000) <- LATEST
    //
    // Arrival order: 1007, 1003, 1010, 1001, 1005 (completely random!)
    let events = vec![
        EventBuilder::new()
            .with_block(1007)
            .deposited_with_total(account_id, term_id, 3000, 3000, 5000), // +3000 → balance: 5000
        EventBuilder::new()
            .with_block(1003)
            .deposited_with_total(account_id, term_id, 3000, 3000, 4000), // +3000 → balance: 4000
        EventBuilder::new()
            .with_block(1010)
            .redeemed_with_total(account_id, term_id, 4000, 4000, 1000), // -4000 → balance: 1000 (LATEST)
        EventBuilder::new()
            .with_block(1001)
            .deposited_with_total(account_id, term_id, 1000, 1000, 1000), // +1000 → balance: 1000
        EventBuilder::new()
            .with_block(1005)
            .redeemed_with_total(account_id, term_id, 2000, 2000, 2000), // -2000 → balance: 2000
    ];

    harness
        .publish_events("rindexer_producer", events)
        .await
        .unwrap();

    let config = harness.default_config();
    let pipeline = EventProcessingPipeline::new(config)
        .await
        .expect("Failed to create pipeline");
    let pipeline_handle = tokio::spawn({
        let pipeline = pipeline.clone();
        async move { pipeline.start().await }
    });

    harness
        .wait_for_processing(6, 20)
        .await
        .expect("Failed to process events");

    harness
        .wait_for_cascade(term_id, 5)
        .await
        .expect("Failed to complete cascade");

    let pool = harness.get_pool().await.expect("Failed to get pool");
    let position = DbAssertions::assert_position_exists(pool, account_id, term_id, curve_id)
        .await
        .expect("Failed to find position");

    // Should use the LATEST event across ALL types (Redeem@1010)
    assert_eq!(
        position.shares, "1000",
        "Position shares should be 1000 from Redeem@1010 (latest event)"
    );

    // Total deposits: 1000 + 3000 + 3000 = 7000
    assert_eq!(
        position.total_deposit_assets_after_total_fees, "7000",
        "Total deposits should accumulate to 7000"
    );

    // Total redeems: 2000 + 4000 = 6000
    assert_eq!(
        position.total_redeem_assets_for_receiver, "6000",
        "Total redeems should accumulate to 6000"
    );

    // Cleanup
    pipeline_handle.abort();
    let _ = pipeline.stop().await;
}

/// Test that position count correctly reflects zero-share positions
/// when the latest event results in zero shares.
#[tokio::test]
#[ignore]
async fn test_multiple_redeems_to_zero_out_of_order() {
    let mut harness = TestHarness::new().await.unwrap();

    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let account_id = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";
    let curve_id = "0x0000000000000000000000000000000000000000000000000000000000000000";

    // Create atom
    let atom_event = EventBuilder::new()
        .with_block(999)
        .atom_created(term_id, account_id);

    harness
        .publish_events("rindexer_producer", vec![atom_event])
        .await
        .unwrap();

    // Chronological order:
    // Block 1000: Deposit 10000 shares → balance: 10000
    // Block 1003: Redeem  5000 shares  → balance: 5000 (10000 - 5000)
    // Block 1005: Redeem  5000 shares  → balance: 0 (5000 - 5000) <- LATEST
    //
    // Arrival order: 1000, 1005, 1003 (redeem events out of order)
    let events = vec![
        EventBuilder::new()
            .with_block(1000)
            .deposited_with_total(account_id, term_id, 10000, 10000, 10000), // +10000 → balance: 10000
        EventBuilder::new()
            .with_block(1005)
            .redeemed_with_total(account_id, term_id, 5000, 5000, 0), // -5000 → balance: 0 (LATEST)
        EventBuilder::new()
            .with_block(1003)
            .redeemed_with_total(account_id, term_id, 5000, 5000, 5000), // -5000 → balance: 5000
    ];

    harness
        .publish_events("rindexer_producer", events)
        .await
        .unwrap();

    let config = harness.default_config();
    let pipeline = EventProcessingPipeline::new(config)
        .await
        .expect("Failed to create pipeline");
    let pipeline_handle = tokio::spawn({
        let pipeline = pipeline.clone();
        async move { pipeline.start().await }
    });

    harness
        .wait_for_processing(4, 15)
        .await
        .expect("Failed to process events");

    harness
        .wait_for_cascade(term_id, 5)
        .await
        .expect("Failed to complete cascade");

    let pool = harness.get_pool().await.expect("Failed to get pool");
    let position = DbAssertions::assert_position_exists(pool, account_id, term_id, curve_id)
        .await
        .expect("Failed to find position");

    // Should use the latest event (Redeem@1005 with zero balance)
    assert_eq!(
        position.shares, "0",
        "Position shares should be 0 from Redeem@1005"
    );

    // Verify vault position_count correctly excludes zero-share positions
    let position_count: Option<i64> = sqlx::query_scalar(
        "SELECT position_count FROM vault WHERE term_id = $1 AND curve_id = $2"
    )
    .bind(term_id)
    .bind(curve_id)
    .fetch_one(pool)
    .await
    .expect("Failed to fetch vault");

    assert_eq!(
        position_count.unwrap_or(0),
        0,
        "Vault position_count should be 0 (zero-share positions excluded)"
    );

    // Cleanup
    pipeline_handle.abort();
    let _ = pipeline.stop().await;
}

/// Test that verifies a position can be created by a redeem event arriving
/// before any deposit events (edge case).
///
/// This edge case can occur when:
/// - A user deposits through an external system/contract
/// - The first on-chain event we capture is a redeem
/// - Later, a deposit event arrives for that position
///
/// Scenario:
/// 1. Redeem@1000 arrives first (creates position with shares=7000, representing external balance)
/// 2. Deposit@1005 arrives second (updates to shares=9000) - should be final state
///
/// Expected behavior:
/// - Position is created by the redeem event with shares=7000, total_deposits=0
/// - When deposit arrives, it updates shares to 9000 and total_deposits=2000
/// - Final state: shares from latest event (Deposit@1005)
#[tokio::test]
#[ignore]
async fn test_redeem_before_deposit_edge_case() {
    let mut harness = TestHarness::new().await.unwrap();

    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let account_id = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";
    let curve_id = "0x0000000000000000000000000000000000000000000000000000000000000000";

    // Step 1: Create atom
    let atom_event = EventBuilder::new()
        .with_block(999)
        .atom_created(term_id, account_id);

    harness
        .publish_events("rindexer_producer", vec![atom_event])
        .await
        .unwrap();

    // Step 2: Publish redeem BEFORE deposit (edge case)
    // Simulates: user deposited externally (off-chain or via different contract),
    // then redeems 3000 shares on-chain, leaving 7000 shares,
    // then deposits 2000 more shares on-chain
    //
    // Chronological order:
    // Block 1000: Redeem 3000 shares → balance: 7000 (from external deposit)
    // Block 1005: Deposit 2000 shares → balance: 9000 (7000 + 2000) <- LATEST
    //
    // Arrival order: 1000 (redeem first), 1005 (deposit second)
    let events = vec![
        // Redeem at block 1000 (arrives first, creates position)
        EventBuilder::new()
            .with_block(1000)
            .with_log_index(0)
            .redeemed_with_total(account_id, term_id, 3000, 3000, 7000), // -3000 → balance: 7000
        // Deposit at block 1005 (arrives second, should be final state)
        EventBuilder::new()
            .with_block(1005)
            .with_log_index(0)
            .deposited_with_total(account_id, term_id, 2000, 2000, 9000), // +2000 → balance: 9000
    ];

    harness
        .publish_events("rindexer_producer", events)
        .await
        .unwrap();

    // Step 3: Start pipeline
    let config = harness.default_config();
    let pipeline = EventProcessingPipeline::new(config)
        .await
        .expect("Failed to create pipeline");
    let pipeline_handle = tokio::spawn({
        let pipeline = pipeline.clone();
        async move { pipeline.start().await }
    });

    // Step 4: Wait for processing
    harness
        .wait_for_processing(3, 15) // 1 atom + 2 events
        .await
        .expect("Failed to process events");

    harness
        .wait_for_cascade(term_id, 5)
        .await
        .expect("Failed to complete cascade");

    // Step 5: Assertions
    let pool = harness
        .get_pool()
        .await
        .expect("Failed to get database pool");

    let position = DbAssertions::assert_position_exists(pool, account_id, term_id, curve_id)
        .await
        .expect("Position should exist (created by redeem)");

    // CRITICAL: Position shares should be from Deposit@1005 (latest event)
    // NOT from Redeem@1000 (the event that created the position)
    assert_eq!(
        position.shares, "9000",
        "Position shares should be 9000 from Deposit@1005 (latest event)"
    );

    // Verify deposit total (only the on-chain deposit)
    assert_eq!(
        position.total_deposit_assets_after_total_fees, "2000",
        "Total deposits should be 2000 (single deposit)"
    );

    // Verify redeem total (the initial redeem that created the position)
    assert_eq!(
        position.total_redeem_assets_for_receiver, "3000",
        "Total redeems should be 3000 (single redeem)"
    );

    // Verify that the vault reflects this active position (shares > 0)
    let position_count: Option<i64> = sqlx::query_scalar(
        "SELECT position_count FROM vault WHERE term_id = $1 AND curve_id = $2"
    )
    .bind(term_id)
    .bind(curve_id)
    .fetch_one(pool)
    .await
    .expect("Failed to fetch vault");

    assert_eq!(
        position_count.unwrap_or(0),
        1,
        "Vault position_count should be 1 (position has non-zero shares)"
    );

    // Cleanup
    pipeline_handle.abort();
    let _ = pipeline.stop().await;
}
