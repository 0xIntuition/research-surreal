use sqlx::PgPool;
use std::time::Duration;
use tracing::{debug, info};

/// Message from the term update queue
#[derive(Debug, Clone)]
pub struct QueueMessage {
    pub id: i64,
    pub term_id: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub attempts: i32,
}

/// Consumer for term update queue
pub struct TermQueueConsumer {
    pool: PgPool,
    poll_interval: Duration,
    max_retry_attempts: i32,
}

impl TermQueueConsumer {
    pub fn new(pool: PgPool, poll_interval: Duration, max_retry_attempts: i32) -> Self {
        Self {
            pool,
            poll_interval,
            max_retry_attempts,
        }
    }

    /// Poll for unprocessed messages
    /// Uses FOR UPDATE SKIP LOCKED for concurrent-safe consumption
    pub async fn poll_messages(&self, limit: i64) -> Result<Vec<QueueMessage>, sqlx::Error> {
        let messages = sqlx::query_as!(
            QueueMessage,
            r#"
            SELECT id, term_id, created_at, attempts
            FROM term_update_queue
            WHERE processed_at IS NULL
              AND attempts < $1
            ORDER BY created_at ASC
            LIMIT $2
            FOR UPDATE SKIP LOCKED
            "#,
            self.max_retry_attempts,
            limit
        )
        .fetch_all(&self.pool)
        .await?;

        if !messages.is_empty() {
            debug!("Polled {} messages from queue", messages.len());
        }

        Ok(messages)
    }

    /// Mark a message as successfully processed
    pub async fn mark_processed(&self, id: i64) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE term_update_queue
            SET processed_at = NOW()
            WHERE id = $1
            "#,
            id
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Mark multiple messages as successfully processed in a single query
    pub async fn mark_batch_processed(&self, ids: &[i64]) -> Result<u64, sqlx::Error> {
        if ids.is_empty() {
            return Ok(0);
        }

        let result = sqlx::query!(
            r#"
            UPDATE term_update_queue
            SET processed_at = NOW()
            WHERE id = ANY($1)
            "#,
            ids
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    /// Mark a message as failed, increment attempts
    pub async fn mark_failed(&self, id: i64, error_msg: &str) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            UPDATE term_update_queue
            SET attempts = attempts + 1,
                last_error = $2,
                last_attempt_at = NOW()
            WHERE id = $1
            "#,
            id,
            error_msg
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Mark multiple messages as failed in a single query
    /// Takes Vec of (id, error_msg) tuples
    pub async fn mark_batch_failed(&self, failures: &[(i64, String)]) -> Result<u64, sqlx::Error> {
        if failures.is_empty() {
            return Ok(0);
        }

        // Extract IDs and error messages into separate arrays
        let ids: Vec<i64> = failures.iter().map(|(id, _)| *id).collect();
        let errors: Vec<String> = failures.iter().map(|(_, err)| err.clone()).collect();

        // Use unnest to update multiple rows with different error messages
        let result = sqlx::query!(
            r#"
            UPDATE term_update_queue
            SET attempts = attempts + 1,
                last_error = data.error,
                last_attempt_at = NOW()
            FROM (
                SELECT UNNEST($1::BIGINT[]) as id, UNNEST($2::TEXT[]) as error
            ) AS data
            WHERE term_update_queue.id = data.id
            "#,
            &ids,
            &errors
        )
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    /// Clean up old processed messages
    pub async fn cleanup_old_messages(&self, retention_hours: i32) -> Result<u64, sqlx::Error> {
        let result = sqlx::query!(
            r#"
            DELETE FROM term_update_queue
            WHERE processed_at IS NOT NULL
              AND processed_at < NOW() - INTERVAL '1 hour' * $1
            "#,
            retention_hours as f64
        )
        .execute(&self.pool)
        .await?;

        let deleted = result.rows_affected();
        if deleted > 0 {
            info!("Cleaned up {} old processed messages", deleted);
        }

        Ok(deleted)
    }

    /// Clean up permanently failed messages (attempts >= max_retry_attempts)
    /// that are older than the retention period
    pub async fn cleanup_permanently_failed(
        &self,
        retention_hours: i32,
    ) -> Result<u64, sqlx::Error> {
        let result = sqlx::query!(
            r#"
            DELETE FROM term_update_queue
            WHERE attempts >= $1
              AND last_attempt_at < NOW() - INTERVAL '1 hour' * $2
            "#,
            self.max_retry_attempts,
            retention_hours as f64
        )
        .execute(&self.pool)
        .await?;

        let deleted = result.rows_affected();
        if deleted > 0 {
            info!(
                "Cleaned up {} permanently failed messages (attempts >= {})",
                deleted, self.max_retry_attempts
            );
        }

        Ok(deleted)
    }

    /// Get count of permanently failed messages (attempts >= max_retry_attempts)
    pub async fn get_permanently_failed_count(&self) -> Result<i64, sqlx::Error> {
        let count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!"
            FROM term_update_queue
            WHERE attempts >= $1
            "#,
            self.max_retry_attempts
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }

    /// Get queue statistics for monitoring
    pub async fn get_queue_stats(&self) -> Result<QueueStats, sqlx::Error> {
        let result = sqlx::query!(
            r#"
            SELECT
                COUNT(*) as "pending_count!",
                CAST(EXTRACT(EPOCH FROM (NOW() - MIN(created_at))) AS DOUBLE PRECISION) as "oldest_pending_age_secs"
            FROM term_update_queue
            WHERE processed_at IS NULL
            "#
        )
        .fetch_one(&self.pool)
        .await?;

        // Calculate retry count separately
        let retry_count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!"
            FROM term_update_queue
            WHERE processed_at IS NULL AND attempts > 0
            "#
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(QueueStats {
            pending_count: result.pending_count,
            retry_count,
            oldest_pending_age_secs: result.oldest_pending_age_secs,
        })
    }

    pub fn poll_interval(&self) -> Duration {
        self.poll_interval
    }
}

#[derive(Debug)]
pub struct QueueStats {
    pub pending_count: i64,
    pub retry_count: i64,
    pub oldest_pending_age_secs: Option<f64>,
}
