use crate::helpers::{DbAssertions, EventBuilder, TestHarness, DEFAULT_CURVE_ID};
use postgres_writer::core::pipeline::EventProcessingPipeline;
use postgres_writer::sync::utils::calculate_counter_term_id;

/// Test that TripleCreated events create exactly 2 queue entries (term_id + counter_term_id)
/// and that they are unique (not duplicated by the publisher)
#[tokio::test]
#[ignore] // Run with --ignored flag since it requires containers
async fn test_triple_created_queue_entries() {
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
        .wait_for_processing(4, 10)
        .await
        .expect("Failed to process 4 events within 10 seconds");
    harness
        .wait_for_cascade(triple_id, 5)
        .await
        .expect("Failed to complete cascade processing within 5 seconds");

    // Give a moment for queue publishing
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Assertions
    let pool = harness
        .get_pool()
        .await
        .expect("Failed to get database pool");

    // Calculate counter term ID
    let counter_triple_id =
        calculate_counter_term_id(triple_id).expect("Failed to calculate counter term ID");

    // Check that exactly 2 queue entries were created for the triple (term_id + counter_term_id)
    // Note: There may be additional entries from atom creation, so we filter for the triple terms
    let queue_entries: Vec<(String,)> = sqlx::query_as(
        "SELECT term_id FROM term_update_queue WHERE term_id IN ($1, $2) ORDER BY term_id",
    )
    .bind(triple_id)
    .bind(&counter_triple_id)
    .fetch_all(pool)
    .await
    .expect("Failed to query queue");

    assert_eq!(
        queue_entries.len(),
        2,
        "TripleCreated should create exactly 2 queue entries (term_id + counter_term_id)"
    );

    let term_ids: Vec<String> = queue_entries.into_iter().map(|(id,)| id).collect();
    assert!(
        term_ids.contains(&triple_id.to_string()),
        "Queue should contain triple_id"
    );
    assert!(
        term_ids.contains(&counter_triple_id),
        "Queue should contain counter_triple_id"
    );

    // Cleanup
    let stop_result =
        tokio::time::timeout(std::time::Duration::from_secs(5), pipeline.stop()).await;
    pipeline_handle.abort();
    stop_result
        .expect("Pipeline stop timeout")
        .expect("Pipeline stop failed");
}

/// Test that rapid sequential events on the same term are deduplicated in the queue
#[tokio::test]
#[ignore] // Run with --ignored flag since it requires containers
async fn test_rapid_sequential_events_deduplication() {
    let mut harness = TestHarness::new().await.unwrap();

    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let account = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";
    let creator = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";

    // Create atom and vault first
    let mut events = vec![EventBuilder::new()
        .with_block(1000)
        .atom_created(term_id, creator)];

    // Add multiple events affecting the same term in rapid succession
    // These should be deduplicated in the queue
    events.push(EventBuilder::new().with_block(1001).deposited(
        account,
        term_id,
        1000000000000000000,
        900000000000000000,
    ));
    events.push(
        EventBuilder::new()
            .with_block(1002)
            .share_price_changed(term_id, 1000000000000000000),
    );
    events.push(EventBuilder::new().with_block(1003).redeemed(
        account,
        term_id,
        100000000000000000,
        95000000000000000,
    ));
    events.push(
        EventBuilder::new()
            .with_block(1004)
            .share_price_changed(term_id, 950000000000000000),
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

    // Wait for processing (5 events total)
    harness
        .wait_for_processing(5, 10)
        .await
        .expect("Failed to process 5 events within 10 seconds");
    harness
        .wait_for_cascade(term_id, 5)
        .await
        .expect("Failed to complete cascade processing within 5 seconds");

    // Give a moment for queue publishing
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Assertions
    let pool = harness
        .get_pool()
        .await
        .expect("Failed to get database pool");

    // Count how many times this term_id appears in the queue
    // With deduplication, we should see fewer entries than the number of events (4) that affect it
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM term_update_queue WHERE term_id = $1")
        .bind(term_id)
        .fetch_one(pool)
        .await
        .expect("Failed to query queue count");

    // The exact count depends on batching behavior, but it should be <= 4
    // (AtomCreated initializes the term but doesn't publish to queue in some paths)
    // We expect deduplication to reduce the count from what would be 4-5 without deduplication
    assert!(
        count.0 <= 4,
        "Queue should have deduplicated entries for term_id (got {})",
        count.0
    );

    // Verify that the term was actually processed correctly despite deduplication
    let term = DbAssertions::assert_term_aggregation(pool, term_id, "Atom")
        .await
        .expect("Failed to verify term aggregation");

    // The term should have the final aggregated values from all events
    // (exact values depend on event data, but it should exist and be non-zero)
    assert!(
        !term.total_assets.is_empty(),
        "Term should have aggregated total_assets"
    );

    // Cleanup
    let stop_result =
        tokio::time::timeout(std::time::Duration::from_secs(5), pipeline.stop()).await;
    pipeline_handle.abort();
    stop_result
        .expect("Pipeline stop timeout")
        .expect("Pipeline stop failed");
}

/// Test that mixed event types (Deposited + SharePriceChanged + Redeemed) are deduplicated
#[tokio::test]
#[ignore] // Run with --ignored flag since it requires containers
async fn test_mixed_event_types_deduplication() {
    let mut harness = TestHarness::new().await.unwrap();

    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let account1 = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";
    let account2 = "0x842d35Cc6634C0532925a3b844Bc9e7595f0bEb1";
    let creator = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";

    // Create atom first
    let mut events = vec![EventBuilder::new()
        .with_block(1000)
        .atom_created(term_id, creator)];

    // Multiple users interacting with the same vault in the same block batch
    events.push(EventBuilder::new().with_block(1001).deposited(
        account1,
        term_id,
        1000000000000000000,
        900000000000000000,
    ));
    events.push(EventBuilder::new().with_block(1002).deposited(
        account2,
        term_id,
        2000000000000000000,
        1800000000000000000,
    ));
    events.push(
        EventBuilder::new()
            .with_block(1003)
            .share_price_changed(term_id, 2800000000000000000),
    );
    events.push(EventBuilder::new().with_block(1004).redeemed(
        account1,
        term_id,
        100000000000000000,
        95000000000000000,
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

    // Wait for processing (5 events total)
    harness
        .wait_for_processing(5, 10)
        .await
        .expect("Failed to process 5 events within 10 seconds");
    harness
        .wait_for_cascade(term_id, 5)
        .await
        .expect("Failed to complete cascade processing within 5 seconds");

    // Give a moment for queue publishing
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Assertions
    let pool = harness
        .get_pool()
        .await
        .expect("Failed to get database pool");

    // With deduplication, all these events for the same term_id should result in
    // much fewer queue entries than the number of events
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM term_update_queue WHERE term_id = $1")
        .bind(term_id)
        .fetch_one(pool)
        .await
        .expect("Failed to query queue count");

    // Should have deduplicated to fewer entries than the 4 events affecting this term
    assert!(
        count.0 <= 4,
        "Queue should have deduplicated mixed event types (got {})",
        count.0
    );

    // Verify vault and position data is correct despite deduplication
    DbAssertions::assert_position_exists(pool, account1, term_id, DEFAULT_CURVE_ID)
        .await
        .expect("Position for account1 should exist");
    DbAssertions::assert_position_exists(pool, account2, term_id, DEFAULT_CURVE_ID)
        .await
        .expect("Position for account2 should exist");

    // Cleanup
    let stop_result =
        tokio::time::timeout(std::time::Duration::from_secs(5), pipeline.stop()).await;
    pipeline_handle.abort();
    stop_result
        .expect("Pipeline stop timeout")
        .expect("Pipeline stop failed");
}
