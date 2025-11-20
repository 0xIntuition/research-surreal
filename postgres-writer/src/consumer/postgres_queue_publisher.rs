use sqlx::PgPool;
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
    pub async fn publish_batch(&self, term_ids: Vec<String>) -> Result<(), sqlx::Error> {
        if term_ids.is_empty() {
            return Ok(());
        }

        debug!("Publishing {} term updates to queue", term_ids.len());

        // Use unnest for efficient batch insert
        sqlx::query!(
            r#"
            INSERT INTO term_update_queue (term_id)
            SELECT * FROM UNNEST($1::text[])
            "#,
            &term_ids
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
