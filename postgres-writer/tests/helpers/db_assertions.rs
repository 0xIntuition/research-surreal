use anyhow::Result;
use sqlx::PgPool;
use postgres_writer::sync::utils::to_eip55_address;

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
    pub position_count: i64,
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

        // Convert expected creator to EIP-55 format for comparison
        let creator_eip55 = to_eip55_address(creator_id)
            .map_err(|e| anyhow::anyhow!("Invalid creator address: {}", e))?;

        assert_eq!(
            row.creator_id, creator_eip55,
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
        expected_position_count: i64,
    ) -> Result<VaultRow> {
        let row = sqlx::query_as::<_, VaultRow>(
            r#"
            SELECT term_id, curve_id,
                   total_shares::TEXT as total_shares,
                   current_share_price::TEXT as current_share_price,
                   total_assets::TEXT as total_assets,
                   market_cap::TEXT as market_cap,
                   position_count
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
        // Convert account address to EIP-55 format for lookup
        let account_eip55 = to_eip55_address(account_id)
            .map_err(|e| anyhow::anyhow!("Invalid account address: {}", e))?;

        let row = sqlx::query_as::<_, PositionRow>(
            r#"
            SELECT account_id, term_id, curve_id,
                   shares::TEXT as shares,
                   total_deposit_assets_after_total_fees::TEXT as total_deposit_assets_after_total_fees,
                   total_redeem_assets_for_receiver::TEXT as total_redeem_assets_for_receiver
            FROM position
            WHERE account_id = $1 AND term_id = $2 AND curve_id = $3
            "#,
        )
        .bind(&account_eip55)
        .bind(term_id)
        .bind(curve_id)
        .fetch_optional(pool)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("Position not found: {} / {} / {}", account_eip55, term_id, curve_id)
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
            SELECT id, type, total_assets::TEXT as total_assets, total_market_cap::TEXT as total_market_cap
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


    /// Gets the latest block and log index from a position
    pub async fn get_position_last_deposit_info(
        pool: &PgPool,
        account_id: &str,
        term_id: &str,
        curve_id: &str,
    ) -> Result<(i64, i64)> {
        // Convert account address to EIP-55 format for lookup
        let account_eip55 = to_eip55_address(account_id)
            .map_err(|e| anyhow::anyhow!("Invalid account address: {}", e))?;

        let row: (i64, i64) = sqlx::query_as(
            "SELECT last_deposit_block, last_deposit_log_index
             FROM position
             WHERE account_id = $1 AND term_id = $2 AND curve_id = $3",
        )
        .bind(&account_eip55)
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
