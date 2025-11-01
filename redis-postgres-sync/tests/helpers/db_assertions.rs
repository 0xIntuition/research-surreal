use anyhow::Result;
use sqlx::PgPool;

#[derive(Debug, sqlx::FromRow, PartialEq)]
pub struct AtomRow {
    pub term_id: String,
    pub creator_id: String,
    pub wallet_id: String,
    pub last_event_block: i64,
    pub last_event_log_index: i64,
}

#[derive(Debug, sqlx::FromRow, PartialEq)]
pub struct TripleRow {
    pub term_id: String,
    pub subject_id: String,
    pub predicate_id: String,
    pub object_id: String,
    pub counter_term_id: String,
    pub last_event_block: i64,
    pub last_event_log_index: i64,
}

#[derive(Debug, sqlx::FromRow, PartialEq)]
pub struct VaultRow {
    pub term_id: String,
    pub curve_id: String,
    pub total_shares: String,
    pub current_share_price: String,
    pub total_assets: String,
    pub market_cap: String,
    pub position_count: i32,
}

#[derive(Debug, sqlx::FromRow, PartialEq)]
pub struct PositionRow {
    pub account_id: String,
    pub term_id: String,
    pub curve_id: String,
    pub shares: String,
    pub total_deposit_assets_after_total_fees: String,
    pub total_redeem_assets_for_receiver: String,
}

#[derive(Debug, sqlx::FromRow, PartialEq)]
pub struct TermRow {
    pub id: String,
    pub r#type: String,
    pub total_assets: String,
    pub total_market_cap: String,
}

pub struct DbAssertions;

impl DbAssertions {
    /// Asserts that an atom exists with expected values
    pub async fn assert_atom_exists(
        pool: &PgPool,
        term_id: &str,
        creator_id: &str,
    ) -> Result<AtomRow> {
        let row = sqlx::query_as::<_, AtomRow>(
            r#"
            SELECT term_id, creator_id, wallet_id,
                   last_event_block, last_event_log_index
            FROM atom
            WHERE term_id = $1
            "#,
        )
        .bind(term_id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Atom not found: {}", term_id))?;

        assert_eq!(
            row.creator_id, creator_id,
            "Creator mismatch for atom {}",
            term_id
        );

        Ok(row)
    }

    /// Asserts that a triple exists
    pub async fn assert_triple_exists(
        pool: &PgPool,
        term_id: &str,
        subject_id: &str,
        predicate_id: &str,
        object_id: &str,
    ) -> Result<TripleRow> {
        let row = sqlx::query_as::<_, TripleRow>(
            r#"
            SELECT term_id, subject_id, predicate_id, object_id, counter_term_id,
                   last_event_block, last_event_log_index
            FROM triple
            WHERE term_id = $1
            "#,
        )
        .bind(term_id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Triple not found: {}", term_id))?;

        assert_eq!(
            row.subject_id, subject_id,
            "Subject mismatch for triple {}",
            term_id
        );
        assert_eq!(
            row.predicate_id, predicate_id,
            "Predicate mismatch for triple {}",
            term_id
        );
        assert_eq!(
            row.object_id, object_id,
            "Object mismatch for triple {}",
            term_id
        );

        Ok(row)
    }

    /// Asserts vault state
    pub async fn assert_vault_state(
        pool: &PgPool,
        term_id: &str,
        curve_id: &str,
        expected_position_count: i32,
    ) -> Result<VaultRow> {
        let row = sqlx::query_as::<_, VaultRow>(
            r#"
            SELECT term_id, curve_id, total_shares, current_share_price,
                   total_assets, market_cap, position_count
            FROM vault
            WHERE term_id = $1 AND curve_id = $2
            "#,
        )
        .bind(term_id)
        .bind(curve_id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Vault not found: {} / {}", term_id, curve_id))?;

        assert_eq!(
            row.position_count, expected_position_count,
            "Position count mismatch for vault {} / {}",
            term_id,
            curve_id
        );

        Ok(row)
    }

    /// Asserts position state
    pub async fn assert_position_exists(
        pool: &PgPool,
        account_id: &str,
        term_id: &str,
        curve_id: &str,
    ) -> Result<PositionRow> {
        let row = sqlx::query_as::<_, PositionRow>(
            r#"
            SELECT account_id, term_id, curve_id, shares,
                   total_deposit_assets_after_total_fees,
                   total_redeem_assets_for_receiver
            FROM position
            WHERE account_id = $1 AND term_id = $2 AND curve_id = $3
            "#,
        )
        .bind(account_id)
        .bind(term_id)
        .bind(curve_id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("Position not found: {} / {} / {}", account_id, term_id, curve_id)
        })?;

        Ok(row)
    }

    /// Asserts term aggregation
    pub async fn assert_term_aggregation(
        pool: &PgPool,
        term_id: &str,
        expected_type: &str,
    ) -> Result<TermRow> {
        let row = sqlx::query_as::<_, TermRow>(
            r#"
            SELECT id, type, total_assets, total_market_cap
            FROM term
            WHERE id = $1
            "#,
        )
        .bind(term_id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| anyhow::anyhow!("Term not found: {}", term_id))?;

        assert_eq!(
            row.r#type, expected_type,
            "Term type mismatch for {}",
            term_id
        );

        Ok(row)
    }

    /// Asserts event count across all event tables
    pub async fn assert_total_events(pool: &PgPool, expected: usize) -> Result<()> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM (
                SELECT 1 FROM atom_created_events
                UNION ALL SELECT 1 FROM triple_created_events
                UNION ALL SELECT 1 FROM deposited_events
                UNION ALL SELECT 1 FROM redeemed_events
                UNION ALL SELECT 1 FROM share_price_changed_events
            ) AS all_events",
        )
        .fetch_one(pool)
        .await?;

        assert_eq!(count as usize, expected, "Event count mismatch");

        Ok(())
    }

    /// Asserts that analytics tables are populated for a triple
    pub async fn assert_analytics_for_triple(
        pool: &PgPool,
        term_id: &str,
        counter_term_id: &str,
    ) -> Result<()> {
        // Check triple_term exists
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM triple_term WHERE term_id = $1 AND counter_term_id = $2)",
        )
        .bind(term_id)
        .bind(counter_term_id)
        .fetch_one(pool)
        .await?;

        assert!(
            exists,
            "Analytics missing for triple {} / {}",
            term_id,
            counter_term_id
        );

        Ok(())
    }

    /// Asserts predicate_object aggregation exists
    pub async fn assert_predicate_object_exists(
        pool: &PgPool,
        predicate_id: &str,
        object_id: &str,
    ) -> Result<()> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM predicate_object WHERE predicate_id = $1 AND object_id = $2)",
        )
        .bind(predicate_id)
        .bind(object_id)
        .fetch_one(pool)
        .await?;

        assert!(
            exists,
            "Predicate-Object aggregation missing: {} / {}",
            predicate_id,
            object_id
        );

        Ok(())
    }

    /// Asserts subject_predicate aggregation exists
    pub async fn assert_subject_predicate_exists(
        pool: &PgPool,
        subject_id: &str,
        predicate_id: &str,
    ) -> Result<()> {
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM subject_predicate WHERE subject_id = $1 AND predicate_id = $2)",
        )
        .bind(subject_id)
        .bind(predicate_id)
        .fetch_one(pool)
        .await?;

        assert!(
            exists,
            "Subject-Predicate aggregation missing: {} / {}",
            subject_id,
            predicate_id
        );

        Ok(())
    }

    /// Gets the latest block and log index from a position
    pub async fn get_position_last_deposit_info(
        pool: &PgPool,
        account_id: &str,
        term_id: &str,
        curve_id: &str,
    ) -> Result<(i64, i64)> {
        let row: (i64, i64) = sqlx::query_as(
            "SELECT last_deposit_block, last_deposit_log_index
             FROM position
             WHERE account_id = $1 AND term_id = $2 AND curve_id = $3",
        )
        .bind(account_id)
        .bind(term_id)
        .bind(curve_id)
        .fetch_one(pool)
        .await?;

        Ok(row)
    }

    /// Gets the latest block and log index from a vault
    pub async fn get_vault_last_price_info(
        pool: &PgPool,
        term_id: &str,
        curve_id: &str,
    ) -> Result<(i64, i64)> {
        let row: (i64, i64) = sqlx::query_as(
            "SELECT last_price_event_block, last_price_event_log_index
             FROM vault
             WHERE term_id = $1 AND curve_id = $2",
        )
        .bind(term_id)
        .bind(curve_id)
        .fetch_one(pool)
        .await?;

        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    // Unit tests would require a database connection
    // These are better tested as part of integration tests
}
