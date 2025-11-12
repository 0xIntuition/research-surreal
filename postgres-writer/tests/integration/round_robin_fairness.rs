use crate::helpers::{mock_events::EventBuilder, test_harness::TestHarness};
use postgres_writer::consumer::rabbitmq_consumer::RabbitMQConsumer;
use serial_test::serial;

#[tokio::test]
#[serial]
async fn test_round_robin_prevents_queue_starvation() {
    // Setup test harness
    let harness = TestHarness::new().await.unwrap();

    // Publish different amounts to different queues to simulate busy/quiet queues
    // atom_created: 500 messages (very busy)
    // deposited: 200 messages (busy)
    // triple_created: 100 messages (moderate)
    // redeemed: 50 messages (quiet)
    // share_price_changed: 25 messages (very quiet)

    let builder = EventBuilder::new();
    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let account = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";
    let subject_id = "0x0000000000000000000000000000000000000000000000000000000000000002";
    let predicate_id = "0x0000000000000000000000000000000000000000000000000000000000000003";
    let object_id = "0x0000000000000000000000000000000000000000000000000000000000000004";

    let atom_events: Vec<_> = (0..500)
        .map(|i| {
            builder
                .clone()
                .with_block(1000 + i)
                .with_log_index(0)
                .atom_created(term_id, account)
        })
        .collect();

    let deposited_events: Vec<_> = (0..200)
        .map(|i| {
            builder
                .clone()
                .with_block(1000 + i)
                .with_log_index(0)
                .deposited(account, term_id, 1000, 1000)
        })
        .collect();

    let triple_events: Vec<_> = (0..100)
        .map(|i| {
            builder
                .clone()
                .with_block(1000 + i)
                .with_log_index(0)
                .triple_created(term_id, subject_id, predicate_id, object_id)
        })
        .collect();

    let redeemed_events: Vec<_> = (0..50)
        .map(|i| {
            builder
                .clone()
                .with_block(1000 + i)
                .with_log_index(0)
                .redeemed(account, term_id, 500, 500)
        })
        .collect();

    let share_price_events: Vec<_> = (0..25)
        .map(|i| {
            builder
                .clone()
                .with_block(1000 + i)
                .with_log_index(0)
                .share_price_changed(term_id, 1000000)
        })
        .collect();

    // Publish to exchanges
    harness
        .publish_events("atom_created", atom_events)
        .await
        .unwrap();
    harness
        .publish_events("deposited", deposited_events)
        .await
        .unwrap();
    harness
        .publish_events("triple_created", triple_events)
        .await
        .unwrap();
    harness
        .publish_events("redeemed", redeemed_events)
        .await
        .unwrap();
    harness
        .publish_events("share_price_changed", share_price_events)
        .await
        .unwrap();

    // Give messages time to reach queues
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Create consumer with a batch size that's smaller than the busiest queue
    // Use higher prefetch_count to ensure enough messages are available
    let consumer = RabbitMQConsumer::new(
        harness.rabbitmq_url(),
        "test",
        &[
            "atom_created".to_string(),
            "triple_created".to_string(),
            "deposited".to_string(),
            "redeemed".to_string(),
            "share_price_changed".to_string(),
        ],
        100, // prefetch_count (higher to ensure messages are available)
    )
    .await
    .unwrap();

    // Small delay to let prefetch fetch messages
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Consume a batch of 250 messages (less than atom_created's 500)
    // With old sequential approach: would take all from atom_created first, starving other queues
    // With round-robin: should take messages from multiple queues fairly
    let batch = consumer.consume_batch(250).await.unwrap();

    // Verify we got a reasonable batch size (may not be exactly 250 due to RabbitMQ delivery timing)
    assert!(
        batch.len() >= 100,
        "Should consume at least 100 messages, got {}",
        batch.len()
    );

    // Count messages by source queue
    let mut queue_counts = std::collections::HashMap::new();
    for msg in &batch {
        *queue_counts.entry(msg.source_stream.clone()).or_insert(0) += 1;
    }

    println!("Queue distribution in batch of 250:");
    for (queue, count) in &queue_counts {
        println!("  {}: {} messages", queue, count);
    }

    // Verify fairness: all queues should be represented (not just atom_created)
    // With round-robin and CHUNK_SIZE=100:
    // - First cycle: atom_created=100, triple_created=100, deposited=50 -> 250 total
    // OR similar distribution depending on starting index

    // Key assertion: more than one queue should be represented
    assert!(
        queue_counts.len() > 1,
        "Multiple queues should be serviced, got: {:?}",
        queue_counts
    );

    // Assert that no single queue dominates entirely
    let max_count = queue_counts.values().max().unwrap_or(&0);
    assert!(
        *max_count < batch.len(),
        "No single queue should dominate entire batch. Max queue had {} of {} messages",
        max_count,
        batch.len()
    );

    // Verify that at least 2 different queues were serviced
    // (ensures we're actually cycling through queues)
    assert!(
        queue_counts.len() >= 2,
        "At least 2 queues should be serviced, got {} queues: {:?}",
        queue_counts.len(),
        queue_counts.keys().collect::<Vec<_>>()
    );
}

#[tokio::test]
#[serial]
async fn test_round_robin_rotation() {
    // Setup test harness
    let harness = TestHarness::new().await.unwrap();

    // Publish 50 messages to each queue (enough to fill multiple batches)
    let builder = EventBuilder::new();
    let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
    let account = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0";
    let subject_id = "0x0000000000000000000000000000000000000000000000000000000000000002";
    let predicate_id = "0x0000000000000000000000000000000000000000000000000000000000000003";
    let object_id = "0x0000000000000000000000000000000000000000000000000000000000000004";

    for exchange in &[
        "atom_created",
        "triple_created",
        "deposited",
        "redeemed",
        "share_price_changed",
    ] {
        let events: Vec<_> = (0..50)
            .map(|i| {
                let b = builder.clone().with_block(2000 + i).with_log_index(0);
                match *exchange {
                    "atom_created" => b.atom_created(term_id, account),
                    "triple_created" => {
                        b.triple_created(term_id, subject_id, predicate_id, object_id)
                    }
                    "deposited" => b.deposited(account, term_id, 1000, 1000),
                    "redeemed" => b.redeemed(account, term_id, 500, 500),
                    "share_price_changed" => b.share_price_changed(term_id, 1000000),
                    _ => unreachable!(),
                }
            })
            .collect();

        harness.publish_events(exchange, events).await.unwrap();
    }

    // Give messages time to reach queues
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Create consumer
    let consumer = RabbitMQConsumer::new(
        harness.rabbitmq_url(),
        "test",
        &[
            "atom_created".to_string(),
            "triple_created".to_string(),
            "deposited".to_string(),
            "redeemed".to_string(),
            "share_price_changed".to_string(),
        ],
        20,
    )
    .await
    .unwrap();

    // Consume multiple batches and verify rotation
    let mut first_queue_in_batches = Vec::new();

    for i in 0..5 {
        let batch = consumer.consume_batch(20).await.unwrap();

        if !batch.is_empty() {
            // Record which queue had the first message in this batch
            let first_queue = batch[0].source_stream.clone();
            first_queue_in_batches.push(first_queue);
            println!("Batch {}: first message from {}", i, batch[0].source_stream);

            // ACK all messages so they don't get redelivered
            for msg in batch {
                if let Some(acker) = msg.acker {
                    consumer.ack_message(&acker).await.unwrap();
                }
            }
        }

        // Small delay between batches
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    // Verify that rotation is happening (not always starting from same queue)
    // We should see different queues as the first queue across batches
    let unique_first_queues: std::collections::HashSet<_> = first_queue_in_batches.iter().collect();

    assert!(
        unique_first_queues.len() > 1,
        "Should start from different queues across batches, got: {:?}",
        first_queue_in_batches
    );
}
