use sqlx::{Postgres, Transaction};
use tracing::{debug, warn};

use crate::error::{Result, SyncError};

/// VaultUpdater handles vault table updates based on position aggregations
pub struct VaultUpdater;

impl VaultUpdater {
    pub fn new() -> Self {
        Self
    }

    /// Update vault metrics from position table
    /// Calculates:
    /// - position_count: count of positions with shares > 0
    /// - market_cap: recalculated from current_share_price * total_shares
    pub async fn update_vault_from_positions(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        term_id: &str,
        curve_id: &str,
    ) -> Result<()> {
        debug!("Updating vault from positions: term={}, curve={}", term_id, curve_id);

        // Count active positions for this vault
        let position_count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM position
            WHERE term_id = $1
              AND curve_id = $2
              AND shares > 0
            "#,
        )
        .bind(term_id)
        .bind(curve_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| SyncError::Sqlx(e))?;

        debug!("Found {} active positions for vault", position_count);

        // Update vault with new position count and recalculate market_cap
        // Use CASE to detect overflow and set to NULL if it occurs
        let rows_affected = sqlx::query(
            r#"
            UPDATE vault
            SET position_count = $1,
                market_cap = CASE
                    WHEN total_shares IS NULL OR current_share_price IS NULL THEN NULL
                    WHEN total_shares = 0 OR current_share_price = 0 THEN 0
                    -- Check if multiplication would overflow by comparing with max numeric
                    WHEN total_shares > (10^78 - 1) / current_share_price THEN NULL
                    ELSE (total_shares * current_share_price) / 1000000000000000000
                END,
                updated_at = NOW()
            WHERE term_id = $2
              AND curve_id = $3
            "#,
        )
        .bind(position_count)
        .bind(term_id)
        .bind(curve_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| SyncError::Sqlx(e))?
        .rows_affected();

        if rows_affected == 0 {
            warn!("No vault found for term={}, curve={}", term_id, curve_id);
        } else {
            debug!("Updated vault with position_count={}", position_count);
        }

        Ok(())
    }

    /// Recalculate vault market_cap after a price change
    /// This is simpler than update_vault_from_positions because we only need to
    /// recalculate market_cap; position_count doesn't change on price updates
    pub async fn recalculate_market_cap(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        term_id: &str,
        curve_id: &str,
    ) -> Result<()> {
        debug!("Recalculating market cap for vault: term={}, curve={}", term_id, curve_id);

        let rows_affected = sqlx::query(
            r#"
            UPDATE vault
            SET market_cap = CASE
                    WHEN total_shares IS NULL OR current_share_price IS NULL THEN NULL
                    WHEN total_shares = 0 OR current_share_price = 0 THEN 0
                    -- Check if multiplication would overflow by comparing with max numeric
                    WHEN total_shares > (10^78 - 1) / current_share_price THEN NULL
                    ELSE (total_shares * current_share_price) / 1000000000000000000
                END,
                updated_at = NOW()
            WHERE term_id = $1
              AND curve_id = $2
            "#,
        )
        .bind(term_id)
        .bind(curve_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| SyncError::Sqlx(e))?
        .rows_affected();

        if rows_affected == 0 {
            warn!("No vault found for term={}, curve={}", term_id, curve_id);
        } else {
            debug!("Recalculated market cap for vault");
        }

        Ok(())
    }
}

impl Default for VaultUpdater {
    fn default() -> Self {
        Self::new()
    }
}
