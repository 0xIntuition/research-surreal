use chrono::{DateTime, Utc};
use crate::error::{Result, SyncError};
use alloy_primitives::keccak256;

pub fn parse_hex_to_u64(hex_str: &str) -> Result<u64> {
    let cleaned = if hex_str.starts_with("0x") || hex_str.starts_with("0X") {
        &hex_str[2..]
    } else {
        hex_str
    };

    u64::from_str_radix(cleaned, 16).map_err(|e| {
        SyncError::ParseError(format!("Failed to parse hex string '{}': {}", hex_str, e))
    })
}

pub fn parse_hex_timestamp_to_datetime(hex_str: &str) -> Result<DateTime<Utc>> {
    let timestamp_u64 = parse_hex_to_u64(hex_str)?;

    DateTime::from_timestamp(timestamp_u64 as i64, 0)
        .ok_or_else(|| {
            SyncError::ParseError(format!(
                "Failed to convert timestamp '{}' to datetime",
                timestamp_u64
            ))
        })
}

/// Calculate the counter triple ID for a given triple term ID
///
/// This matches the Solidity implementation:
/// ```solidity
/// bytes32 constant COUNTER_SALT = keccak256("COUNTER_SALT");
/// counterTripleId = keccak256(abi.encodePacked(COUNTER_SALT, tripleId));
/// ```
///
/// # Arguments
/// * `term_id` - The hex-encoded term ID (with or without 0x prefix)
///
/// # Returns
/// The counter term ID as a hex-encoded string with 0x prefix
pub fn calculate_counter_term_id(term_id: &str) -> Result<String> {
    // Remove 0x prefix if present
    let term_id_cleaned = if term_id.starts_with("0x") || term_id.starts_with("0X") {
        &term_id[2..]
    } else {
        term_id
    };

    // Decode hex string to bytes
    let term_id_bytes = hex::decode(term_id_cleaned)
        .map_err(|e| SyncError::ParseError(format!("Failed to decode term_id '{}': {}", term_id, e)))?;

    // Calculate COUNTER_SALT = keccak256("COUNTER_SALT")
    let counter_salt = keccak256(b"COUNTER_SALT");

    // Calculate keccak256(abi.encodePacked(COUNTER_SALT, term_id))
    // In Solidity, abi.encodePacked just concatenates the bytes
    let mut concatenated = counter_salt.to_vec();
    concatenated.extend_from_slice(&term_id_bytes);

    let result = keccak256(&concatenated);

    // Return as hex-encoded string with 0x prefix
    Ok(format!("0x{}", hex::encode(result)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_hex_to_u64() {
        // Test block_number values
        assert_eq!(parse_hex_to_u64("0x1b778a2").unwrap(), 28801186);
        // Test log_index values
        assert_eq!(parse_hex_to_u64("0xd6").unwrap(), 214);
        // Test transaction_index values
        assert_eq!(parse_hex_to_u64("0x10").unwrap(), 16);
        assert_eq!(parse_hex_to_u64("0x0").unwrap(), 0);
        assert_eq!(parse_hex_to_u64("0x1").unwrap(), 1);
        assert_eq!(parse_hex_to_u64("0xff").unwrap(), 255);
        // Test without 0x prefix
        assert_eq!(parse_hex_to_u64("10").unwrap(), 16);
        assert_eq!(parse_hex_to_u64("d6").unwrap(), 214);
    }

    #[test]
    fn test_parse_hex_timestamp_to_datetime() {
        // Test with the example timestamp from the user: 0x68b18674 = 1756464756
        let result = parse_hex_timestamp_to_datetime("0x68b18674").unwrap();
        assert_eq!(result.timestamp(), 1756464756);

        // Test a known timestamp: 1641165056 = 2022-01-02 23:10:56 UTC
        let result = parse_hex_timestamp_to_datetime("0x61d23100").unwrap();
        assert_eq!(result.timestamp(), 1641165056);

        // Test without 0x prefix
        let result = parse_hex_timestamp_to_datetime("61d23100").unwrap();
        assert_eq!(result.timestamp(), 1641165056);
    }

    #[test]
    fn test_calculate_counter_term_id() {
        // Test with a sample term_id (64 hex characters = 32 bytes)
        let term_id = "0x0000000000000000000000000000000000000000000000000000000000000001";
        let counter_term_id = calculate_counter_term_id(term_id).unwrap();

        // Should return a valid hex string with 0x prefix and 64 hex characters
        assert!(counter_term_id.starts_with("0x"));
        assert_eq!(counter_term_id.len(), 66); // 0x + 64 hex chars

        // Test without 0x prefix
        let term_id_no_prefix = "0000000000000000000000000000000000000000000000000000000000000001";
        let counter_term_id_no_prefix = calculate_counter_term_id(term_id_no_prefix).unwrap();
        assert_eq!(counter_term_id, counter_term_id_no_prefix);

        // Test that different term_ids produce different counter_term_ids
        let term_id_2 = "0x0000000000000000000000000000000000000000000000000000000000000002";
        let counter_term_id_2 = calculate_counter_term_id(term_id_2).unwrap();
        assert_ne!(counter_term_id, counter_term_id_2);

        // Test that the same term_id always produces the same counter_term_id
        let counter_term_id_again = calculate_counter_term_id(term_id).unwrap();
        assert_eq!(counter_term_id, counter_term_id_again);
    }
}

