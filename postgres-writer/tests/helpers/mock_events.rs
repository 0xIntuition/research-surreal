use chrono::Utc;
use fake::{Fake, Faker};
use postgres_writer::core::types::RindexerEvent;
use serde_json::json;

// Common test constants
/// Default curve ID used in tests (linear bonding curve)
pub const DEFAULT_CURVE_ID: &str =
    "0x0000000000000000000000000000000000000000000000000000000000000000";

/// Default starting block number for tests
pub const DEFAULT_BLOCK_START: u64 = 1000;

/// Default test network
pub const DEFAULT_NETWORK: &str = "base_sepolia";

/// Validates that a string is a properly formatted Ethereum address
fn validate_address(addr: &str, field_name: &str) -> Result<(), String> {
    if !addr.starts_with("0x") {
        return Err(format!("{field_name} must start with '0x', got: {addr}"));
    }
    if addr.len() != 42 {
        return Err(format!(
            "{} must be 42 characters (0x + 40 hex digits), got length: {}",
            field_name,
            addr.len()
        ));
    }
    if !addr[2..].chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(format!(
            "{field_name} must contain only hex digits after '0x', got: {addr}"
        ));
    }
    Ok(())
}

/// Validates that a string is a properly formatted term ID (bytes32)
fn validate_term_id(id: &str, field_name: &str) -> Result<(), String> {
    if !id.starts_with("0x") {
        return Err(format!("{field_name} must start with '0x', got: {id}"));
    }
    if id.len() != 66 {
        return Err(format!(
            "{} must be 66 characters (0x + 64 hex digits), got length: {}",
            field_name,
            id.len()
        ));
    }
    if !id[2..].chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(format!(
            "{field_name} must contain only hex digits after '0x', got: {id}"
        ));
    }
    Ok(())
}

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
            block_number: DEFAULT_BLOCK_START,
            log_index: 0,
            transaction_hash: format!("0x{}", hex::encode(Faker.fake::<[u8; 32]>())),
            network: DEFAULT_NETWORK.to_string(),
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

    /// Creates AtomCreated event
    pub fn atom_created(&self, term_id: &str, creator: &str) -> RindexerEvent {
        // Validate inputs to catch errors early
        validate_term_id(term_id, "term_id").expect("Invalid term_id in atom_created");
        validate_address(creator, "creator").expect("Invalid creator address in atom_created");

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
        // Validate inputs to catch errors early
        validate_term_id(term_id, "term_id").expect("Invalid term_id in triple_created");
        validate_term_id(subject_id, "subject_id").expect("Invalid subject_id in triple_created");
        validate_term_id(predicate_id, "predicate_id")
            .expect("Invalid predicate_id in triple_created");
        validate_term_id(object_id, "object_id").expect("Invalid object_id in triple_created");

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
        // Validate inputs to catch errors early
        validate_address(account_id, "account_id").expect("Invalid account_id in deposited");
        validate_term_id(term_id, "term_id").expect("Invalid term_id in deposited");

        let event_data = json!({
            "sender": account_id,
            "receiver": account_id,
            "termId": term_id,
            "curveId": DEFAULT_CURVE_ID,
            "assets": assets.to_string(),
            "shares": shares.to_string(),
            // In real events, totalShares represents user's balance after deposit
            // For test simplicity, we use shares value as the final balance
            "totalShares": shares.to_string(),
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

    /// Creates Deposited event with explicit totalShares
    ///
    /// # Parameters
    /// - `shares_delta`: The shares minted in this transaction (delta)
    /// - `total_shares`: The user's total share balance after the deposit (cumulative)
    pub fn deposited_with_total(
        &self,
        account_id: &str,
        term_id: &str,
        assets: u64,
        shares_delta: u64,
        total_shares: u64,
    ) -> RindexerEvent {
        validate_address(account_id, "account_id")
            .expect("Invalid account_id in deposited_with_total");
        validate_term_id(term_id, "term_id").expect("Invalid term_id in deposited_with_total");

        let event_data = json!({
            "sender": account_id,
            "receiver": account_id,
            "termId": term_id,
            "curveId": DEFAULT_CURVE_ID,
            "assets": assets.to_string(),
            "shares": shares_delta.to_string(),
            "totalShares": total_shares.to_string(),
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
        // Validate inputs to catch errors early
        validate_address(account_id, "account_id").expect("Invalid account_id in redeemed");
        validate_term_id(term_id, "term_id").expect("Invalid term_id in redeemed");

        let event_data = json!({
            "sender": account_id,
            "receiver": account_id,
            "termId": term_id,
            "curveId": DEFAULT_CURVE_ID,
            "assets": assets.to_string(),
            "shares": shares.to_string(),
            // In real events, totalShares represents user's balance after redemption
            // For test simplicity, we use shares value as the final balance
            "totalShares": shares.to_string(),
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

    /// Creates Redeemed event with explicit totalShares
    ///
    /// # Parameters
    /// - `shares_delta`: The shares redeemed in this transaction (delta)
    /// - `total_shares`: The user's total share balance after the redemption (cumulative)
    pub fn redeemed_with_total(
        &self,
        account_id: &str,
        term_id: &str,
        shares_delta: u64,
        assets: u64,
        total_shares: u64,
    ) -> RindexerEvent {
        validate_address(account_id, "account_id")
            .expect("Invalid account_id in redeemed_with_total");
        validate_term_id(term_id, "term_id").expect("Invalid term_id in redeemed_with_total");

        let event_data = json!({
            "sender": account_id,
            "receiver": account_id,
            "termId": term_id,
            "curveId": DEFAULT_CURVE_ID,
            "assets": assets.to_string(),
            "shares": shares_delta.to_string(),
            "totalShares": total_shares.to_string(),
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
        // Validate inputs to catch errors early
        validate_term_id(term_id, "term_id").expect("Invalid term_id in share_price_changed");

        let event_data = json!({
            "termId": term_id,
            "curveId": DEFAULT_CURVE_ID,
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
        // Convert log_index to hex string (as expected by parse_hex_to_u64)
        let log_index_hex = format!("0x{:x}", self.log_index);

        json!({
            "address": self.address,
            "block_hash": block_hash,
            "block_number": self.block_number,
            "block_timestamp": timestamp_hex,
            "log_index": log_index_hex,
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

        // Add deposits out of order: block DEFAULT_BLOCK_START+5, then +1, then +3
        self.events.push(
            builder
                .clone()
                .with_block(DEFAULT_BLOCK_START + 5)
                .with_log_index(0)
                .deposited(account, term_id, 5000, 5000),
        );
        self.events.push(
            builder
                .clone()
                .with_block(DEFAULT_BLOCK_START + 1)
                .with_log_index(0)
                .deposited(account, term_id, 1000, 1000),
        );
        self.events.push(
            builder
                .clone()
                .with_block(DEFAULT_BLOCK_START + 3)
                .with_log_index(0)
                .deposited(account, term_id, 3000, 3000),
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
        assert_eq!(tx_info.get("log_index").unwrap().as_str().unwrap(), "0x5");
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
