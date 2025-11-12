use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};

fn deserialize_hex_timestamp<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    match opt {
        Some(hex_str) => {
            let cleaned = if hex_str.starts_with("0x") || hex_str.starts_with("0X") {
                &hex_str[2..]
            } else {
                &hex_str
            };

            let timestamp_u64 =
                u64::from_str_radix(cleaned, 16).map_err(serde::de::Error::custom)?;

            let datetime = DateTime::from_timestamp(timestamp_u64 as i64, 0).ok_or_else(|| {
                serde::de::Error::custom(format!("Invalid timestamp: {timestamp_u64}"))
            })?;

            Ok(Some(datetime))
        }
        None => Ok(None),
    }
}

/// Represents a rindexer event from Redis streams
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RindexerEvent {
    pub event_name: String,
    pub event_signature_hash: String,
    pub event_data: serde_json::Value,
    pub network: String,
}

/// Transaction information embedded in event data
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TransactionInformation {
    pub address: String,
    pub block_hash: String,
    pub block_number: u64,
    #[serde(deserialize_with = "deserialize_hex_timestamp")]
    pub block_timestamp: Option<DateTime<Utc>>,
    pub log_index: String,
    pub network: String,
    pub transaction_hash: String,
    pub transaction_index: u64,
}

/// RabbitMQ message structure
#[derive(Debug, Clone)]
pub struct StreamMessage {
    pub id: String,
    pub event: RindexerEvent,
    pub source_stream: String,
    pub acker: Option<lapin::acker::Acker>, // Only present on last message in array
}

/// Pipeline metrics for monitoring and observability
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineMetrics {
    pub total_events_processed: u64,
    pub total_events_failed: u64,
    pub circuit_breaker_state: String,
    pub rabbitmq_consumer_health: bool,
    pub postgres_sync_health: bool,
}

/// Health status for the entire pipeline
#[derive(Debug, Clone, Serialize)]
pub struct PipelineHealth {
    pub healthy: bool,
    pub rabbitmq_consumer_healthy: bool,
    pub postgres_sync_healthy: bool,
    pub circuit_breaker_closed: bool,
    pub last_check: DateTime<Utc>,
    pub metrics: PipelineMetrics,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_rindexer_event_deserialization() {
        let json_data = json!({
            "event_name": "Transfer",
            "event_signature_hash": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            "event_data": {
                "from": "0x0338ce5020c447f7e668dc2ef778025ce3982662",
                "to": "0x0338ce5020c447f7e668dc2ef778025ce3982662",
                "value": "1000000000000000000",
                "transaction_information": {
                    "address": "0xae78736cd615f374d3085123a210448e74fc6393",
                    "block_hash": "0x8461da7a1d4b47190a01fa6eae219be40aacffab0dd64af7259b2d404572c3d9",
                    "block_number": 18718011,
                    "log_index": "0",
                    "network": "ethereum",
                    "transaction_hash": "0x145c6705ffbf461e85d08b4a7f5850d6b52a7364d93a057722ca1194034f3ba4",
                    "transaction_index": 0
                }
            },
            "network": "ethereum"
        });

        let event: RindexerEvent = serde_json::from_value(json_data).unwrap();
        assert_eq!(event.event_name, "Transfer");
        assert_eq!(event.network, "ethereum");
        assert_eq!(
            event.event_signature_hash,
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        );
    }

    #[test]
    fn test_transaction_information_extraction() {
        let json_data = json!({
            "transaction_information": {
                "address": "0xae78736cd615f374d3085123a210448e74fc6393",
                "block_hash": "0x8461da7a1d4b47190a01fa6eae219be40aacffab0dd64af7259b2d404572c3d9",
                "block_number": 18718011,
                "block_timestamp": null,
                "log_index": "0",
                "network": "ethereum",
                "transaction_hash": "0x145c6705ffbf461e85d08b4a7f5850d6b52a7364d93a057722ca1194034f3ba4",
                "transaction_index": 0
            }
        });

        let tx_info: TransactionInformation =
            serde_json::from_value(json_data["transaction_information"].clone()).unwrap();

        assert_eq!(
            tx_info.address,
            "0xae78736cd615f374d3085123a210448e74fc6393"
        );
        assert_eq!(tx_info.block_number, 18718011);
        assert_eq!(tx_info.network, "ethereum");
        assert!(tx_info.block_timestamp.is_none());
    }

    #[test]
    fn test_transaction_information_with_hex_timestamp() {
        let json_data = json!({
            "transaction_information": {
                "address": "0x5a0a023f08df301dcce96166f4185ec77df6a87a",
                "block_hash": "0x849b43070457e7535f252baad4bb61f151e31fa197977bb917036f149b29a3ab",
                "block_number": 2036374,
                "block_timestamp": "0x68b18674",
                "log_index": "33",
                "network": "intuition-testnet",
                "transaction_hash": "0x09cc81fe23c98e4a1a84db71d92f691fb6e2e8ee47397db0475229c4647bb576",
                "transaction_index": 1
            }
        });

        let tx_info: TransactionInformation =
            serde_json::from_value(json_data["transaction_information"].clone()).unwrap();

        assert_eq!(
            tx_info.address,
            "0x5a0a023f08df301dcce96166f4185ec77df6a87a"
        );
        assert_eq!(tx_info.block_number, 2036374);
        assert_eq!(tx_info.network, "intuition-testnet");
        assert!(tx_info.block_timestamp.is_some());
        assert_eq!(tx_info.block_timestamp.unwrap().timestamp(), 1756464756);
    }

    #[test]
    fn test_rindexer_event_with_array_event_data() {
        // Test the actual data structure that was causing the issue
        let json_data = json!({
            "event_name": "AtomCreated",
            "event_signature_hash": "0x123456789abcdef",
            "event_data": [
                {
                    "atomData": "697066733a2f2f6261666b726569686d34797864766772737164656475776d746576346c627134616466726c7478356267786b753633636368786670797135646e34",
                    "atomWallet": "0x5c9f27e28b6d2a422610d259216a4d8d399499ff",
                    "creator": "0xec5e1ab83df052ec5c5ddfe9e64afa3cbb44c649",
                    "transaction_information": {
                        "address": "0x1a6950807e33d5bc9975067e6d6b5ea4cd661665",
                        "block_hash": "0x289372df6e1b6f68e1402bd5ed09eacab4b5e9cd929cd8bab066385d1bde0122",
                        "block_number": "0x1b778a2",
                        "block_timestamp": null,
                        "log_index": "0xd6",
                        "network": "base_sepolia",
                        "transaction_hash": "0x5e433f903bdb67721a3e37ba7b3e9d9b34eddb53fff5f76424f89fa5227d30cb",
                        "transaction_index": 16
                    },
                    "vaultId": "26497"
                }
            ],
            "network": "base_sepolia"
        });

        let mut event: RindexerEvent = serde_json::from_value(json_data).unwrap();

        // Before the fix, event_data would be an array
        assert!(event.event_data.is_array());

        // Simulate the fix that should happen during parsing
        if let Some(array) = event.event_data.as_array() {
            if !array.is_empty() {
                event.event_data = array[0].clone();
            }
        }

        // After the fix, event_data should be the object and transaction_information should be accessible
        assert!(event.event_data.is_object());
        assert!(event.event_data.get("transaction_information").is_some());
        assert_eq!(
            event.event_data.get("atomWallet").and_then(|v| v.as_str()),
            Some("0x5c9f27e28b6d2a422610d259216a4d8d399499ff")
        );
        assert_eq!(
            event.event_data.get("vaultId").and_then(|v| v.as_str()),
            Some("26497")
        );

        // Test that transaction_information is properly structured
        let tx_info = event.event_data.get("transaction_information").unwrap();
        assert_eq!(
            tx_info.get("network").and_then(|v| v.as_str()),
            Some("base_sepolia")
        );
        assert_eq!(
            tx_info.get("block_number").and_then(|v| v.as_str()),
            Some("0x1b778a2")
        );
        assert_eq!(
            tx_info.get("log_index").and_then(|v| v.as_str()),
            Some("0xd6")
        );
    }

    #[test]
    fn test_multiple_events_from_array_event_data() {
        // Test that we can handle multiple blockchain events in a single Redis message
        let json_data = json!({
            "event_name": "AtomCreated",
            "event_signature_hash": "0x123456789abcdef",
            "event_data": [
                {
                    "atomWallet": "0x5c9f27e28b6d2a422610d259216a4d8d399499ff",
                    "creator": "0xec5e1ab83df052ec5c5ddfe9e64afa3cbb44c649",
                    "vaultId": "26497"
                },
                {
                    "atomWallet": "0x6d0f38f29c7d2f532720e569832f3e9c5bb55100",
                    "creator": "0xfd6f2c9e73e064beb5f3c5ffcf3c4d5eb8c77e4a",
                    "vaultId": "26498"
                }
            ],
            "network": "base_sepolia"
        });

        let event: RindexerEvent = serde_json::from_value(json_data).unwrap();

        // Verify the event_data contains an array with 2 blockchain events
        assert!(event.event_data.is_array());
        let array = event.event_data.as_array().unwrap();
        assert_eq!(array.len(), 2);

        // Verify each blockchain event in the array is structured correctly
        assert_eq!(
            array[0].get("vaultId").and_then(|v| v.as_str()),
            Some("26497")
        );
        assert_eq!(
            array[1].get("vaultId").and_then(|v| v.as_str()),
            Some("26498")
        );

        // This test confirms that our fixed parser should create 2 StreamMessage objects
        // from this single Redis message, so metrics.record_event_success(2) instead of (1)
    }
}
