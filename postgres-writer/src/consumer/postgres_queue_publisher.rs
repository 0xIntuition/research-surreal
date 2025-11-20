use sqlx::PgPool;
use std::collections::HashSet;
use tracing::debug;

/// Publisher for term update queue
pub struct TermQueuePublisher {
    pool: PgPool,
}

impl TermQueuePublisher {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Publish a single term update to the queue
    pub async fn publish_term_update(&self, term_id: &str) -> Result<(), sqlx::Error> {
        debug!("Publishing term update to queue: {}", term_id);

        sqlx::query!(
            r#"
            INSERT INTO term_update_queue (term_id)
            VALUES ($1)
            "#,
            term_id
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Publish multiple term updates in a batch
    /// Deduplicates term_ids to avoid redundant analytics processing
    pub async fn publish_batch(&self, term_ids: Vec<String>) -> Result<(), sqlx::Error> {
        if term_ids.is_empty() {
            return Ok(());
        }

        // Deduplicate term_ids before insertion to prevent redundant analytics processing
        let unique_terms: HashSet<String> = term_ids.into_iter().collect();
        let unique_term_ids: Vec<String> = unique_terms.into_iter().collect();

        debug!(
            "Publishing {} unique term updates to queue (after deduplication)",
            unique_term_ids.len()
        );

        // Use unnest for efficient batch insert
        sqlx::query!(
            r#"
            INSERT INTO term_update_queue (term_id)
            SELECT * FROM UNNEST($1::text[])
            "#,
            &unique_term_ids
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    #[test]
    fn test_deduplication_logic() {
        // Test the core deduplication logic without database
        let term_ids = vec![
            "0x123".to_string(),
            "0x456".to_string(),
            "0x123".to_string(), // duplicate
            "0x789".to_string(),
            "0x456".to_string(), // duplicate
        ];

        let unique_terms: HashSet<String> = term_ids.into_iter().collect();
        assert_eq!(unique_terms.len(), 3, "Should have 3 unique term_ids");
        assert!(unique_terms.contains("0x123"));
        assert!(unique_terms.contains("0x456"));
        assert!(unique_terms.contains("0x789"));
    }

    #[test]
    fn test_deduplication_empty() {
        let term_ids: Vec<String> = vec![];
        let unique_terms: HashSet<String> = term_ids.into_iter().collect();
        assert_eq!(unique_terms.len(), 0, "Should have 0 unique term_ids");
    }

    #[test]
    fn test_deduplication_all_unique() {
        let term_ids = vec![
            "0x111".to_string(),
            "0x222".to_string(),
            "0x333".to_string(),
        ];

        let unique_terms: HashSet<String> = term_ids.into_iter().collect();
        assert_eq!(unique_terms.len(), 3, "Should preserve all unique term_ids");
    }

    #[test]
    fn test_deduplication_all_duplicates() {
        let term_ids = vec![
            "0x999".to_string(),
            "0x999".to_string(),
            "0x999".to_string(),
            "0x999".to_string(),
        ];

        let unique_terms: HashSet<String> = term_ids.into_iter().collect();
        assert_eq!(unique_terms.len(), 1, "Should have only 1 unique term_id");
        assert!(unique_terms.contains("0x999"));
    }
}
