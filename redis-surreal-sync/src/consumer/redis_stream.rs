use redis::{aio::MultiplexedConnection, Client};
use std::collections::HashMap;
use tracing::{debug, error, info};

use crate::core::types::{RindexerEvent, StreamMessage};
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
        let client = Client::open(redis_url)
            .map_err(SyncError::Redis)?;
        let connection = client.get_multiplexed_async_connection().await
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
                    info!("Created consumer group '{}' for stream '{}'", self.consumer_group, stream_name);
                }
                Err(e) => {
                    let error_msg = e.to_string();
                    if error_msg.contains("BUSYGROUP") {
                        debug!("Consumer group '{}' already exists for stream '{}'", self.consumer_group, stream_name);
                    } else {
                        error!("Failed to create consumer group for stream '{}': {}", stream_name, e);
                        return Err(SyncError::Redis(e));
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn consume_batch(&self, batch_size: usize) -> Result<Vec<StreamMessage>> {
        debug!("consume_batch called with batch_size: {}", batch_size);
        
        if self.stream_names.is_empty() {
            debug!("No stream names configured, returning empty result");
            return Ok(Vec::new());
        }

        debug!("Stream names: {:?}", self.stream_names);

        // First try to claim idle pending messages
        debug!("Attempting to claim idle pending messages");
        for stream_name in &self.stream_names {
            let claimed_messages = self.claim_idle_messages_for_stream(stream_name, batch_size).await?;
            if !claimed_messages.is_empty() {
                debug!("Claimed {} idle messages from stream {}", claimed_messages.len(), stream_name);
                return Ok(claimed_messages);
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

        let result: redis::Value = cmd.query_async(&mut self.connection.clone()).await
            .map_err(SyncError::Redis)?;

        self.parse_redis_response(result)
    }

    async fn read_pending_messages(&self, batch_size: usize) -> Result<Vec<StreamMessage>> {
        debug!("Building XREADGROUP command: GROUP {} {} COUNT {} STREAMS", 
               self.consumer_group, self.consumer_name, batch_size);
        debug!("Stream names: {:?}", self.stream_names);
        
        let mut cmd = redis::cmd("XREADGROUP");
        cmd.arg("GROUP")
            .arg(&self.consumer_group)
            .arg(&self.consumer_name)
            .arg("COUNT")
            .arg(batch_size)
            .arg("STREAMS");

        // Add stream names
        for stream_name in &self.stream_names {
            cmd.arg(stream_name);
        }

        // Add "0-0" for each stream to read pending messages
        for _ in &self.stream_names {
            cmd.arg("0-0");
        }

        debug!("Executing Redis command for pending messages");
        
        // Let's also check what XINFO CONSUMERS shows for debugging
        let info_result: redis::Value = redis::cmd("XINFO")
            .arg("CONSUMERS")
            .arg(&self.stream_names[0])  // Check first stream
            .arg(&self.consumer_group)
            .query_async(&mut self.connection.clone()).await
            .map_err(SyncError::Redis)?;
        debug!("XINFO CONSUMERS result: {:?}", info_result);
        
        let result: redis::Value = cmd.query_async(&mut self.connection.clone()).await
            .map_err(SyncError::Redis)?;

        let messages = self.parse_redis_response(result)?;
        debug!("Pending messages query returned {} messages", messages.len());
        Ok(messages)
    }

    async fn claim_idle_messages_for_stream(&self, stream_name: &str, count: usize) -> Result<Vec<StreamMessage>> {
        // Use XPENDING to get pending message IDs for this consumer specifically
        let pending_result: redis::Value = redis::cmd("XPENDING")
            .arg(stream_name)
            .arg(&self.consumer_group)
            .arg("-")
            .arg("+")
            .arg(count)
            .arg(&self.consumer_name) // Only get pending messages for this consumer
            .query_async(&mut self.connection.clone()).await
            .map_err(SyncError::Redis)?;

        let message_ids = self.extract_pending_message_ids(pending_result)?;
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }

        debug!("Found {} pending message IDs for stream {}", message_ids.len(), stream_name);

        // Claim the messages with XCLAIM using min-idle-time of 60000ms (1 minute)
        let mut cmd = redis::cmd("XCLAIM");
        cmd.arg(stream_name)
            .arg(&self.consumer_group)
            .arg(&self.consumer_name)
            .arg(60000); // min-idle-time 1 minute - only claim if message has been pending for at least 1 minute

        for message_id in &message_ids {
            cmd.arg(message_id);
        }

        let result: redis::Value = cmd.query_async(&mut self.connection.clone()).await
            .map_err(SyncError::Redis)?;

        let mut messages = Vec::new();
        if let redis::Value::Array(claimed_messages) = result {
            for message in claimed_messages {
                let parsed_messages = self.parse_message(&message, stream_name)?;
                messages.extend(parsed_messages);
            }
        }

        debug!("Successfully claimed {} messages from stream {}", messages.len(), stream_name);
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
            debug!("Message {} was already acknowledged in stream {}", message_id, stream_name);
        }
        
        Ok(())
    }

    fn parse_redis_response(&self, value: redis::Value) -> Result<Vec<StreamMessage>> {
        debug!("parse_redis_response called");
        let mut messages = Vec::new();

        if let redis::Value::Array(streams) = value {
            for stream in streams {
                if let redis::Value::Array(stream_data) = stream {
                    if stream_data.len() >= 2 {
                        let stream_name = match &stream_data[0] {
                            redis::Value::BulkString(name) => String::from_utf8_lossy(name).to_string(),
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

    fn parse_message(&self, message: &redis::Value, stream_name: &str) -> Result<Vec<StreamMessage>> {
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
                                redis::Value::BulkString(k) => String::from_utf8_lossy(k).to_string(),
                                _ => continue,
                            };
                            let value = match &chunk[1] {
                                redis::Value::BulkString(v) => String::from_utf8_lossy(v).to_string(),
                                _ => continue,
                            };
                            field_map.insert(key, value);
                        }
                    }

                    // Parse the rindexer event from the field map
                    if let Some(event_data) = field_map.get("event_data")
                        .or_else(|| field_map.get("data"))
                        .or_else(|| field_map.get("payload")) {
                        
                        debug!("Raw event JSON from Redis stream {}: {}", stream_name, event_data);
                        
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
                            message_id, field_map.keys().collect::<Vec<_>>()
                        );
                    }
                }
            }
        }

        Ok(Vec::new())
    }
}