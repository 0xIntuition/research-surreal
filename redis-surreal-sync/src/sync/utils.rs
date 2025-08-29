use chrono::{DateTime, Utc};
use crate::error::{Result, SyncError};

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
}

