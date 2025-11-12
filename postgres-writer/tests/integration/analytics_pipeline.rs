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

    let pipeline = EventProcessingPipeline::new(config, None)
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

    let pipeline = EventProcessingPipeline::new(config, None)
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

    let pipeline = EventProcessingPipeline::new(config, None)
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

    let pipeline = EventProcessingPipeline::new(config, None)
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
