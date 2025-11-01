use chrono::Utc;
use fake::{Fake, Faker};
use redis_postgres_sync::core::types::RindexerEvent;
use serde_json::json;

#[derive(Clone)]
pub struct EventBuilder {
    block_number: u64,
    log_index: u64,
    transaction_hash: String,
    network: String,
    address: String,
}

impl EventBuilder {
    pub fn new() -> Self {
        Self {
            block_number: 1000,
            log_index: 0,
            transaction_hash: format!("0x{}", hex::encode(Faker.fake::<[u8; 32]>())),
            network: "base_sepolia".to_string(),
            address: format!("0x{}", hex::encode(Faker.fake::<[u8; 20]>())),
        }
    }

    pub fn with_block(mut self, block_number: u64) -> Self {
        self.block_number = block_number;
        self
    }

    pub fn with_log_index(mut self, log_index: u64) -> Self {
        self.log_index = log_index;
        self
    }

    pub fn with_network(mut self, network: &str) -> Self {
        self.network = network.to_string();
        self
    }

    /// Creates AtomCreated event
    pub fn atom_created(&self, term_id: &str, creator: &str) -> RindexerEvent {
        let wallet_id = format!("0x{}", hex::encode(Faker.fake::<[u8; 20]>()));

        let event_data = json!({
            "termId": term_id,
            "creator": creator,
            "atomWallet": wallet_id,
            "atomData": "",  // Empty string is valid hex data
            "transaction_information": self.transaction_info(),
        });

        RindexerEvent {
            event_name: "AtomCreated".to_string(),
            event_signature_hash: "0x123456789abcdef".to_string(),
            event_data,
            network: self.network.clone(),
        }
    }

    /// Creates TripleCreated event
    pub fn triple_created(
        &self,
        term_id: &str,
        subject_id: &str,
        predicate_id: &str,
        object_id: &str,
    ) -> RindexerEvent {
        let creator = format!("0x{}", hex::encode(Faker.fake::<[u8; 20]>()));

        let event_data = json!({
            "termId": term_id,
            "subjectId": subject_id,
            "predicateId": predicate_id,
            "objectId": object_id,
            "creator": creator,
            "transaction_information": self.transaction_info(),
        });

        RindexerEvent {
            event_name: "TripleCreated".to_string(),
            event_signature_hash: "0x456789abcdef123".to_string(),
            event_data,
            network: self.network.clone(),
        }
    }

    /// Creates Deposited event
    pub fn deposited(
        &self,
        account_id: &str,
        term_id: &str,
        assets: u64,
        shares: u64,
    ) -> RindexerEvent {
        let curve_id = "0x0000000000000000000000000000000000000000000000000000000000000000";

        let event_data = json!({
            "sender": account_id,
            "receiver": account_id,
            "termId": term_id,
            "curveId": curve_id,
            "assets": assets.to_string(),
            "shares": shares.to_string(),
            "totalShares": (shares * 2).to_string(),
            "assetsAfterFees": assets.to_string(),
            "vaultType": 0,
            "transaction_information": self.transaction_info(),
        });

        RindexerEvent {
            event_name: "Deposited".to_string(),
            event_signature_hash: "0x789abcdef123456".to_string(),
            event_data,
            network: self.network.clone(),
        }
    }

    /// Creates Redeemed event
    pub fn redeemed(
        &self,
        account_id: &str,
        term_id: &str,
        shares: u64,
        assets: u64,
    ) -> RindexerEvent {
        let curve_id = "0x0000000000000000000000000000000000000000000000000000000000000000";

        let event_data = json!({
            "sender": account_id,
            "receiver": account_id,
            "termId": term_id,
            "curveId": curve_id,
            "assets": assets.to_string(),
            "shares": shares.to_string(),
            "totalShares": "0",
            "fees": "0",
            "vaultType": 0,
            "transaction_information": self.transaction_info(),
        });

        RindexerEvent {
            event_name: "Redeemed".to_string(),
            event_signature_hash: "0xabcdef123456789".to_string(),
            event_data,
            network: self.network.clone(),
        }
    }

    /// Creates SharePriceChanged event
    pub fn share_price_changed(&self, term_id: &str, new_price: u64) -> RindexerEvent {
        let curve_id = "0x0000000000000000000000000000000000000000000000000000000000000000";

        let event_data = json!({
            "termId": term_id,
            "curveId": curve_id,
            "sharePrice": new_price.to_string(),
            "totalAssets": "1000000000000000000",
            "totalShares": "1000000000000000000",
            "vaultType": 0,
            "transaction_information": self.transaction_info(),
        });

        RindexerEvent {
            event_name: "SharePriceChanged".to_string(),
            event_signature_hash: "0xdef123456789abc".to_string(),
            event_data,
            network: self.network.clone(),
        }
    }

    fn transaction_info(&self) -> serde_json::Value {
        let block_hash = format!("0x{}", hex::encode(Faker.fake::<[u8; 32]>()));
        let timestamp = Utc::now();
        // Convert timestamp to hex-encoded Unix timestamp (as expected by deserialize_hex_timestamp)
        let timestamp_hex = format!("0x{:x}", timestamp.timestamp());

        json!({
            "address": self.address,
            "block_hash": block_hash,
            "block_number": self.block_number,
            "block_timestamp": timestamp_hex,
            "log_index": self.log_index.to_string(),
            "network": self.network,
            "transaction_hash": self.transaction_hash,
            "transaction_index": 0,
        })
    }
}

impl Default for EventBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper to create non-sequential event sequences
pub struct NonSequentialScenario {
    events: Vec<RindexerEvent>,
}

impl NonSequentialScenario {
    pub fn new() -> Self {
        Self { events: vec![] }
    }

    /// Adds events in intentionally scrambled order
    pub fn add_scrambled_deposits(mut self, account: &str, term_id: &str) -> Self {
        let builder = EventBuilder::new();

        // Add deposits out of order: block 1005, then 1001, then 1003
        self.events.push(
            builder
                .clone()
                .with_block(1005)
                .with_log_index(0)
                .deposited(account, term_id, 5000, 5000),
        );
        self.events.push(
            builder
                .clone()
                .with_block(1001)
                .with_log_index(0)
                .deposited(account, term_id, 1000, 1000),
        );
        self.events.push(
            builder
                .clone()
                .with_block(1003)
                .with_log_index(0)
                .deposited(account, term_id, 3000, 3000),
        );

        self
    }

    /// Adds price changes out of order
    pub fn add_scrambled_price_changes(mut self, term_id: &str) -> Self {
        let builder = EventBuilder::new();

        // Add price changes: block 1008, then 1002, then 1005
        self.events.push(
            builder
                .clone()
                .with_block(1008)
                .with_log_index(0)
                .share_price_changed(term_id, 2000000000000000000), // 2.0 ETH
        );
        self.events.push(
            builder
                .clone()
                .with_block(1002)
                .with_log_index(0)
                .share_price_changed(term_id, 1200000000000000000), // 1.2 ETH
        );
        self.events.push(
            builder
                .clone()
                .with_block(1005)
                .with_log_index(0)
                .share_price_changed(term_id, 1500000000000000000), // 1.5 ETH
        );

        self
    }

    pub fn build(self) -> Vec<RindexerEvent> {
        self.events
    }
}

impl Default for NonSequentialScenario {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_builder_atom_created() {
        let builder = EventBuilder::new();
        let event = builder.atom_created(
            "0x0000000000000000000000000000000000000000000000000000000000000001",
            "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0",
        );

        assert_eq!(event.event_name, "AtomCreated");
        assert_eq!(event.network, "base_sepolia");
        assert!(event.event_data.get("termId").is_some());
        assert!(event.event_data.get("transaction_information").is_some());
    }

    #[test]
    fn test_event_builder_deposited() {
        let builder = EventBuilder::new().with_block(2000).with_log_index(5);
        let event = builder.deposited(
            "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0",
            "0x0000000000000000000000000000000000000000000000000000000000000001",
            10000,
            10000,
        );

        assert_eq!(event.event_name, "Deposited");
        let tx_info = event.event_data.get("transaction_information").unwrap();
        assert_eq!(tx_info.get("block_number").unwrap().as_u64().unwrap(), 2000);
        assert_eq!(
            tx_info.get("log_index").unwrap().as_str().unwrap(),
            "5"
        );
    }

    #[test]
    fn test_non_sequential_scenario() {
        let scenario = NonSequentialScenario::new().add_scrambled_deposits(
            "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0",
            "0x0000000000000000000000000000000000000000000000000000000000000001",
        );

        let events = scenario.build();
        assert_eq!(events.len(), 3);

        // Verify they are out of order
        let blocks: Vec<u64> = events
            .iter()
            .map(|e| {
                e.event_data
                    .get("transaction_information")
                    .unwrap()
                    .get("block_number")
                    .unwrap()
                    .as_u64()
                    .unwrap()
            })
            .collect();

        assert_eq!(blocks, vec![1005, 1001, 1003]);
    }
}
