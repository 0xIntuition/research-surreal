use crate::helpers::{DbAssertions, EventBuilder, TestHarness};
use postgres_writer::core::pipeline::EventProcessingPipeline;

#[tokio::test]
#[ignore] // Run with --ignored flag since it requires containers
async fn test_atom_creation_initializes_term() {
    let mut harness = TestHarness::new().await.unwrap();

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
    let config = harness.default_config();

    let pipeline = EventProcessingPipeline::new(config)
        .await
        .expect("Failed to create pipeline");
    let pipeline_handle = tokio::spawn({
        let pipeline = pipeline.clone();
        async move { pipeline.start().await }
    });

    // Wait for processing
    harness
        .wait_for_processing(1, 10)
        .await
        .expect("Failed to process 1 event within 10 seconds");
    harness
        .wait_for_cascade(term_id, 5)
        .await
        .expect("Failed to complete cascade processing within 5 seconds");

    // Assertions
    let pool = harness
        .get_pool()
        .await
        .expect("Failed to get database pool");

    // Check that atom was created
    DbAssertions::assert_atom_exists(pool, term_id, creator)
        .await
        .expect("Failed to verify atom exists");

    // Check that term was initialized by cascade processor
    let term = DbAssertions::assert_term_aggregation(pool, term_id, "Atom")
        .await
        .expect("Failed to verify term aggregation");

    // Initial values should be zero
    assert_eq!(
        term.total_assets, "0",
        "New atom should have 0 total assets"
    );
    assert_eq!(
        term.total_market_cap, "0",
        "New atom should have 0 market cap"
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
async fn test_triple_creation_initializes_terms() {
    let mut harness = TestHarness::new().await.unwrap();

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
    events.push(EventBuilder::new().with_block(1003).triple_created(
        triple_id,
        subject_id,
        predicate_id,
        object_id,
    ));

    harness
        .publish_events("rindexer_producer", events)
        .await
        .unwrap();

    // Start pipeline
    let config = harness.default_config();

    let pipeline = EventProcessingPipeline::new(config)
        .await
        .expect("Failed to create pipeline");
    let pipeline_handle = tokio::spawn({
        let pipeline = pipeline.clone();
        async move { pipeline.start().await }
    });

    // Wait for processing
    harness
        .wait_for_processing(4, 15)
        .await
        .expect("Failed to process 4 events within 15 seconds");
    harness
        .wait_for_cascade(triple_id, 5)
        .await
        .expect("Failed to complete cascade processing within 5 seconds");

    // Assertions
    let pool = harness
        .get_pool()
        .await
        .expect("Failed to get database pool");

    // Check that triple was created
    let triple =
        DbAssertions::assert_triple_exists(pool, triple_id, subject_id, predicate_id, object_id)
            .await
            .expect("Failed to verify triple exists");

    // Check that the triple term was initialized
    DbAssertions::assert_term_aggregation(pool, triple_id, "Triple")
        .await
        .expect("Failed to verify triple term aggregation");

    // Check that counter term was also initialized
    let counter_term_id = &triple.counter_term_id;
    DbAssertions::assert_term_aggregation(pool, counter_term_id, "Triple")
        .await
        .expect("Failed to verify counter term aggregation");

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
async fn test_deposits_update_term_aggregations() {
    let mut harness = TestHarness::new().await.unwrap();

    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let account_id = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";

    // Create atom, deposit, and trigger price change to update term aggregations
    // Note: Deposits only update vault.position_count. Term aggregations are only
    // updated by SharePriceChanged events (see perf optimization commit 0e59404)
    let events = vec![
        EventBuilder::new()
            .with_block(1000)
            .atom_created(term_id, account_id),
        EventBuilder::new()
            .with_block(1001)
            .deposited(account_id, term_id, 10000, 10000),
        EventBuilder::new()
            .with_block(1002)
            .share_price_changed(term_id, 10000),
    ];

    harness
        .publish_events("rindexer_producer", events)
        .await
        .unwrap();

    // Start pipeline
    let config = harness.default_config();

    let pipeline = EventProcessingPipeline::new(config)
        .await
        .expect("Failed to create pipeline");
    let pipeline_handle = tokio::spawn({
        let pipeline = pipeline.clone();
        async move { pipeline.start().await }
    });

    // Wait for processing (3 events now)
    harness
        .wait_for_processing(3, 15)
        .await
        .expect("Failed to process 3 events within 15 seconds");
    harness
        .wait_for_cascade(term_id, 5)
        .await
        .expect("Failed to complete cascade processing within 5 seconds");

    // Assertions
    let pool = harness
        .get_pool()
        .await
        .expect("Failed to get database pool");

    // Check term aggregation was updated
    let term = DbAssertions::assert_term_aggregation(pool, term_id, "Atom")
        .await
        .expect("Failed to verify term aggregation");

    // After SharePriceChanged event, total_assets should be updated by cascade processor
    let total_assets = term.total_assets.parse::<i64>().unwrap();
    assert!(
        total_assets > 0,
        "Term total_assets should be updated after share price change"
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
async fn test_share_price_changes_update_term_market_cap() {
    let mut harness = TestHarness::new().await.unwrap();

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
    let config = harness.default_config();

    let pipeline = EventProcessingPipeline::new(config)
        .await
        .expect("Failed to create pipeline");
    let pipeline_handle = tokio::spawn({
        let pipeline = pipeline.clone();
        async move { pipeline.start().await }
    });

    // Wait for processing
    harness
        .wait_for_processing(3, 15)
        .await
        .expect("Failed to process 3 events within 15 seconds");
    harness
        .wait_for_cascade(term_id, 5)
        .await
        .expect("Failed to complete cascade processing within 5 seconds");

    // Assertions
    let pool = harness
        .get_pool()
        .await
        .expect("Failed to get database pool");

    // Check term aggregation
    let term = DbAssertions::assert_term_aggregation(pool, term_id, "Atom")
        .await
        .expect("Failed to verify term aggregation");

    // Market cap should be updated
    let market_cap = term.total_market_cap.parse::<i64>().unwrap();
    assert!(
        market_cap > 0,
        "Term market_cap should be updated after price change"
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
async fn test_triple_vault_multi_curve_no_zero_metrics() {
    let mut harness = TestHarness::new().await.unwrap();

    // Define curve IDs
    let curve_id_0 = "0x0000000000000000000000000000000000000000000000000000000000000000";
    let curve_id_2 = "0x0000000000000000000000000000000000000000000000000000000000000002";

    // Create three atoms: subject, predicate, object
    let subject_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let predicate_id = "0x0000000000000000000000000000000000000000000000000000000000000002";
    let object_id = "0x0000000000000000000000000000000000000000000000000000000000000003";
    let triple_id = "0x0000000000000000000000000000000000000000000000000000000000000010";
    let account_id = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";

    // Scenario:
    // - subject has vault with curve_id=0
    // - predicate has vault with curve_id=2
    // - object has vault with curve_id=0
    // - triple (term=triple_id, counter=counter) should only create entries where BOTH terms have the same curve
    // - This means NO triple_vault entries should exist where one term lacks the curve

    let mut events = vec![
        // Create all atoms
        EventBuilder::new()
            .with_block(1000)
            .atom_created(subject_id, account_id),
        EventBuilder::new()
            .with_block(1001)
            .atom_created(predicate_id, account_id),
        EventBuilder::new()
            .with_block(1002)
            .atom_created(object_id, account_id),
    ];

    // Create deposits with different curve_ids
    // Subject gets curve_id=0
    events.push(
        EventBuilder::new()
            .with_block(1003)
            .deposited_with_curve(account_id, subject_id, curve_id_0, 10000, 10000),
    );
    events.push(
        EventBuilder::new()
            .with_block(1004)
            .share_price_changed_with_curve(subject_id, curve_id_0, 1000000),
    );

    // Predicate gets curve_id=2 (different from subject!)
    events.push(EventBuilder::new().with_block(1005).deposited_with_curve(
        account_id,
        predicate_id,
        curve_id_2,
        20000,
        20000,
    ));
    events.push(
        EventBuilder::new()
            .with_block(1006)
            .share_price_changed_with_curve(predicate_id, curve_id_2, 2000000),
    );

    // Object gets curve_id=0 (same as subject)
    events.push(
        EventBuilder::new()
            .with_block(1007)
            .deposited_with_curve(account_id, object_id, curve_id_0, 30000, 30000),
    );
    events.push(
        EventBuilder::new()
            .with_block(1008)
            .share_price_changed_with_curve(object_id, curve_id_0, 3000000),
    );

    // Create triple
    events.push(EventBuilder::new().with_block(1009).triple_created(
        triple_id,
        subject_id,
        predicate_id,
        object_id,
    ));

    // Create deposits for the triple itself with curve_id=0
    events.push(
        EventBuilder::new()
            .with_block(1010)
            .deposited_with_curve(account_id, triple_id, curve_id_0, 40000, 40000),
    );
    events.push(
        EventBuilder::new()
            .with_block(1011)
            .share_price_changed_with_curve(triple_id, curve_id_0, 4000000),
    );

    harness
        .publish_events("rindexer_producer", events)
        .await
        .unwrap();

    // Start pipeline
    let config = harness.default_config();

    let pipeline = EventProcessingPipeline::new(config)
        .await
        .expect("Failed to create pipeline");
    let pipeline_handle = tokio::spawn({
        let pipeline = pipeline.clone();
        async move { pipeline.start().await }
    });

    // Wait for processing (12 events)
    harness
        .wait_for_processing(12, 20)
        .await
        .expect("Failed to process 12 events within 20 seconds");

    // Wait for cascade of all terms
    for term_id in [subject_id, predicate_id, object_id, triple_id] {
        harness
            .wait_for_cascade(term_id, 5)
            .await
            .expect("Failed to complete cascade processing");
    }

    // Additional wait for analytics worker to process term updates
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Assertions
    let pool = harness
        .get_pool()
        .await
        .expect("Failed to get database pool");

    // Check that triple was created
    let triple =
        DbAssertions::assert_triple_exists(pool, triple_id, subject_id, predicate_id, object_id)
            .await
            .expect("Failed to verify triple exists");

    // Get counter_term_id for the triple
    let counter_term_id = &triple.counter_term_id;

    // CRITICAL ASSERTION: Check that there are NO zero-metric entries in the entire triple_vault table
    let zero_count = DbAssertions::count_triple_vault_zero_metrics(pool)
        .await
        .expect("Failed to count zero-metric entries");

    assert_eq!(
        zero_count, 0,
        "Found {} zero-metric entries in triple_vault table. After the fix, there should be NO entries where all metrics are 0.",
        zero_count
    );

    // Get all triple_vault entries for this triple
    let triple_vault_entries =
        DbAssertions::get_triple_vault_entries(pool, triple_id, counter_term_id)
            .await
            .expect("Failed to get triple_vault entries");

    // ASSERTION: All entries should have at least one non-zero metric
    // The fix ensures we don't create entries where neither term has a vault for that curve
    for entry in &triple_vault_entries {
        let has_nonzero_metric = entry.total_shares != "0"
            || entry.total_assets != "0"
            || entry.position_count != 0
            || entry.market_cap != "0";

        assert!(
            has_nonzero_metric,
            "Triple vault entry for curve {} should have at least one non-zero metric. \
            The bug was fixed to prevent entries where neither term has a vault for the curve.",
            entry.curve_id
        );
    }

    // Cleanup
    let stop_result =
        tokio::time::timeout(std::time::Duration::from_secs(5), pipeline.stop()).await;

    pipeline_handle.abort();

    stop_result
        .expect("Pipeline stop timeout")
        .expect("Pipeline stop failed");
}
