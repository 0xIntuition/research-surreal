use redis::{aio::MultiplexedConnection, Client};
use std::collections::HashMap;
use tracing::{debug, error, info};

use crate::core::types::{BatchConsumptionResult, RindexerEvent, StreamMessage};
use crate::error::{Result, SyncError};

pub struct RedisStreamConsumer {
    connection: MultiplexedConnection,
    stream_names: Vec<String>,
    consumer_group: String,
    consumer_name: String,
}

impl RedisStreamConsumer {
    pub async fn new(
        redis_url: &str,
        stream_names: &[String],
        consumer_group: &str,
        consumer_name: &str,
    ) -> Result<Self> {
        let client = Client::open(redis_url).map_err(SyncError::Redis)?;
        let connection = client
            .get_multiplexed_async_connection()
            .await
            .map_err(SyncError::Redis)?;

        info!(
            "Connected to Redis streams: {:?} with consumer group: {} and consumer: {}",
            stream_names, consumer_group, consumer_name
        );

        let mut consumer = Self {
            connection,
            stream_names: stream_names.to_vec(),
            consumer_group: consumer_group.to_string(),
            consumer_name: consumer_name.to_string(),
        };

        consumer.ensure_consumer_groups().await?;
        Ok(consumer)
    }

    async fn ensure_consumer_groups(&mut self) -> Result<()> {
        for stream_name in &self.stream_names {
            let result: std::result::Result<String, redis::RedisError> = redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(stream_name)
                .arg(&self.consumer_group)
                .arg("0")
                .arg("MKSTREAM")
                .query_async(&mut self.connection)
                .await;

            match result {
                Ok(_) => {
                    info!(
                        "Created consumer group '{}' for stream '{}'",
                        self.consumer_group, stream_name
                    );
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    if error_msg.contains("BUSYGROUP") {
                        debug!(
                            "Consumer group '{}' already exists for stream '{}'",
                            self.consumer_group, stream_name
                        );
                    } else {
                        error!(
                            "Failed to create consumer group for stream '{}': {}",
                            stream_name, e
                        );
                        return Err(SyncError::Redis(e));
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn consume_batch(&self, batch_size: usize) -> Result<BatchConsumptionResult> {
        debug!("consume_batch called with batch_size: {}", batch_size);

        if self.stream_names.is_empty() {
            debug!("No stream names configured, returning empty result");
            return Ok(BatchConsumptionResult {
                messages: Vec::new(),
                claimed_count: 0,
            });
        }

        debug!("Stream names: {:?}", self.stream_names);

        // First try to claim idle pending messages
        debug!("Attempting to claim idle pending messages");
        for stream_name in &self.stream_names {
            let claimed_messages = self
                .claim_idle_messages_for_stream(stream_name, batch_size)
                .await?;
            if !claimed_messages.is_empty() {
                let count = claimed_messages.len();
                debug!(
                    "Claimed {} idle messages from stream {}",
                    count, stream_name
                );
                return Ok(BatchConsumptionResult {
                    messages: claimed_messages,
                    claimed_count: count,
                });
            }
        }

        // Read new messages using XREADGROUP with ">"
        let mut cmd = redis::cmd("XREADGROUP");
        cmd.arg("GROUP")
            .arg(&self.consumer_group)
            .arg(&self.consumer_name)
            .arg("COUNT")
            .arg(batch_size)
            .arg("BLOCK")
            .arg(1000)
            .arg("STREAMS");

        // Add stream names
        for stream_name in &self.stream_names {
            cmd.arg(stream_name);
        }

        // Add ">" for each stream to read new messages
        for _ in &self.stream_names {
            cmd.arg(">");
        }

        let result: redis::Value = cmd
            .query_async(&mut self.connection.clone())
            .await
            .map_err(SyncError::Redis)?;

        let messages = self.parse_redis_response(result)?;
        Ok(BatchConsumptionResult {
            messages,
            claimed_count: 0,
        })
    }

    async fn claim_idle_messages_for_stream(
        &self,
        stream_name: &str,
        count: usize,
    ) -> Result<Vec<StreamMessage>> {
        // Use XPENDING to get pending message IDs for this consumer specifically
        let pending_result: redis::Value = redis::cmd("XPENDING")
            .arg(stream_name)
            .arg(&self.consumer_group)
            .arg("-")
            .arg("+")
            .arg(count)
            .arg(&self.consumer_name) // Only get pending messages for this consumer
            .query_async(&mut self.connection.clone())
            .await
            .map_err(SyncError::Redis)?;

        let message_ids = self.extract_pending_message_ids(pending_result)?;
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }

        debug!(
            "Found {} pending message IDs for stream {}",
            message_ids.len(),
            stream_name
        );

        // Claim the messages with XCLAIM using min-idle-time of 60000ms (1 minute)
        let mut cmd = redis::cmd("XCLAIM");
        cmd.arg(stream_name)
            .arg(&self.consumer_group)
            .arg(&self.consumer_name)
            .arg(60000); // min-idle-time 1 minute - only claim if message has been pending for at least 1 minute

        for message_id in &message_ids {
            cmd.arg(message_id);
        }

        let result: redis::Value = cmd
            .query_async(&mut self.connection.clone())
            .await
            .map_err(SyncError::Redis)?;

        let mut messages = Vec::new();
        if let redis::Value::Array(claimed_messages) = result {
            for message in claimed_messages {
                let parsed_messages = self.parse_message(&message, stream_name)?;
                messages.extend(parsed_messages);
            }
        }

        debug!(
            "Successfully claimed {} messages from stream {}",
            messages.len(),
            stream_name
        );
        Ok(messages)
    }

    fn extract_pending_message_ids(&self, pending_result: redis::Value) -> Result<Vec<String>> {
        let mut message_ids = Vec::new();

        if let redis::Value::Array(pending_messages) = pending_result {
            for pending_entry in pending_messages {
                if let redis::Value::Array(entry_data) = pending_entry {
                    if !entry_data.is_empty() {
                        if let redis::Value::BulkString(message_id_bytes) = &entry_data[0] {
                            let message_id = String::from_utf8_lossy(message_id_bytes).to_string();
                            message_ids.push(message_id);
                        }
                    }
                }
            }
        }

        Ok(message_ids)
    }

    pub async fn ack_message(&self, stream_name: &str, message_id: &str) -> Result<()> {
        let ack_count: u64 = redis::cmd("XACK")
            .arg(stream_name)
            .arg(&self.consumer_group)
            .arg(message_id)
            .query_async(&mut self.connection.clone())
            .await
            .map_err(SyncError::Redis)?;

        if ack_count == 0 {
            debug!(
                "Message {} was already acknowledged in stream {}",
                message_id, stream_name
            );
        }

        Ok(())
    }

    /// Get stream metrics for monitoring - returns (lag, pending_count) for a stream
    ///
    /// Uses XINFO GROUPS to get accurate metrics:
    /// - lag: Number of entries in the stream not yet delivered to the consumer group
    /// - pending: Number of messages delivered but awaiting ACK
    pub async fn get_stream_metrics(&self, stream_name: &str) -> Result<(i64, i64)> {
        use tracing::warn;

        // Get consumer group info using XINFO GROUPS
        let groups_result: redis::Value = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(stream_name)
            .query_async(&mut self.connection.clone())
            .await
            .map_err(|e| {
                warn!("Failed to get stream info for {}: {}", stream_name, e);
                e
            })
            .map_err(SyncError::Redis)?;

        // Parse the XINFO GROUPS response to find our consumer group
        let (lag, pending_count) = match groups_result {
            redis::Value::Array(groups) => {
                // XINFO GROUPS returns array of groups, each group is an array of field-value pairs
                for group_data in groups {
                    if let redis::Value::Array(fields) = group_data {
                        let mut group_name = String::new();
                        let mut lag_value: i64 = 0;
                        let mut pending_value: i64 = 0;

                        // Parse field-value pairs
                        for chunk in fields.chunks(2) {
                            if chunk.len() == 2 {
                                let key = match &chunk[0] {
                                    redis::Value::BulkString(k) => {
                                        String::from_utf8_lossy(k).to_string()
                                    }
                                    redis::Value::SimpleString(k) => k.clone(),
                                    _ => continue,
                                };

                                match key.as_str() {
                                    "name" => {
                                        group_name = match &chunk[1] {
                                            redis::Value::BulkString(v) => {
                                                String::from_utf8_lossy(v).to_string()
                                            }
                                            redis::Value::SimpleString(v) => v.clone(),
                                            _ => String::new(),
                                        };
                                    }
                                    "lag" => {
                                        lag_value = match &chunk[1] {
                                            redis::Value::Int(v) => *v,
                                            _ => 0,
                                        };
                                    }
                                    "pending" => {
                                        pending_value = match &chunk[1] {
                                            redis::Value::Int(v) => *v,
                                            _ => 0,
                                        };
                                    }
                                    _ => {}
                                }
                            }
                        }

                        // Check if this is our consumer group
                        if group_name == self.consumer_group {
                            return Ok((lag_value, pending_value));
                        }
                    }
                }

                // Consumer group not found
                warn!(
                    "Consumer group '{}' not found for stream '{}'",
                    self.consumer_group, stream_name
                );
                (0, 0)
            }
            _ => {
                warn!(
                    "Unexpected response format from XINFO GROUPS for stream '{}'",
                    stream_name
                );
                (0, 0)
            }
        };

        Ok((lag, pending_count))
    }

    pub fn get_stream_names(&self) -> &[String] {
        &self.stream_names
    }

    fn parse_redis_response(&self, value: redis::Value) -> Result<Vec<StreamMessage>> {
        debug!("parse_redis_response called");
        let mut messages = Vec::new();

        if let redis::Value::Array(streams) = value {
            for stream in streams {
                if let redis::Value::Array(stream_data) = stream {
                    if stream_data.len() >= 2 {
                        let stream_name = match &stream_data[0] {
                            redis::Value::BulkString(name) => {
                                String::from_utf8_lossy(name).to_string()
                            }
                            _ => continue,
                        };

                        if let redis::Value::Array(stream_messages) = &stream_data[1] {
                            for message in stream_messages {
                                let parsed_messages = self.parse_message(message, &stream_name)?;
                                messages.extend(parsed_messages);
                            }
                        }
                    }
                }
            }
        }

        debug!("Parsed {} messages from Redis", messages.len());
        Ok(messages)
    }

    fn parse_message(
        &self,
        message: &redis::Value,
        stream_name: &str,
    ) -> Result<Vec<StreamMessage>> {
        if let redis::Value::Array(message_data) = message {
            if message_data.len() >= 2 {
                let message_id = match &message_data[0] {
                    redis::Value::BulkString(id) => String::from_utf8_lossy(id).to_string(),
                    _ => return Ok(Vec::new()),
                };

                if let redis::Value::Array(fields) = &message_data[1] {
                    let mut field_map = HashMap::new();

                    for chunk in fields.chunks(2) {
                        if chunk.len() == 2 {
                            let key = match &chunk[0] {
                                redis::Value::BulkString(k) => {
                                    String::from_utf8_lossy(k).to_string()
                                }
                                _ => continue,
                            };
                            let value = match &chunk[1] {
                                redis::Value::BulkString(v) => {
                                    String::from_utf8_lossy(v).to_string()
                                }
                                _ => continue,
                            };
                            field_map.insert(key, value);
                        }
                    }

                    // Parse the rindexer event from the field map
                    if let Some(event_data) = field_map
                        .get("event_data")
                        .or_else(|| field_map.get("data"))
                        .or_else(|| field_map.get("payload"))
                    {
                        debug!(
                            "Raw event JSON from Redis stream {}: {}",
                            stream_name, event_data
                        );

                        match serde_json::from_str::<RindexerEvent>(event_data) {
                            Ok(event) => {
                                debug!(
                                    "Parsed event: {} with signature: {} from message: {}",
                                    event.event_name, event.event_signature_hash, message_id
                                );

                                let mut messages = Vec::new();

                                // Handle array event_data - create one StreamMessage per blockchain event
                                if let Some(array) = event.event_data.as_array() {
                                    if !array.is_empty() {
                                        debug!("Event data is an array with {} blockchain events, creating individual StreamMessages", array.len());

                                        // Process each blockchain event in the array
                                        for (index, event_data) in array.iter().enumerate() {
                                            let mut individual_event = event.clone();
                                            individual_event.event_data = event_data.clone();

                                            messages.push(StreamMessage {
                                                id: format!("{}-{}", message_id, index),
                                                event: individual_event,
                                                source_stream: stream_name.to_string(),
                                                redis_message_id: message_id.clone(),
                                            });
                                        }
                                    }
                                } else {
                                    // Single event case
                                    messages.push(StreamMessage {
                                        id: message_id.clone(),
                                        event,
                                        source_stream: stream_name.to_string(),
                                        redis_message_id: message_id.clone(),
                                    });
                                }

                                return Ok(messages);
                            }
                            Err(e) => {
                                error!(
                                    "Failed to parse event JSON from message {}: {}. JSON: {}",
                                    message_id, e, event_data
                                );
                            }
                        }
                    } else {
                        debug!(
                            "No event data found in message {}. Available fields: {:?}",
                            message_id,
                            field_map.keys().collect::<Vec<_>>()
                        );
                    }
                }
            }
        }

        Ok(Vec::new())
    }
}
