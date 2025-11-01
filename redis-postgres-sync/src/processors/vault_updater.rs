use sqlx::{Postgres, Transaction};
use tracing::debug;

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
    /// - total_assets: calculated from total_shares * current_share_price, or from position deposits if no price data
    /// - market_cap: recalculated from current_share_price * total_shares
    pub async fn update_vault_from_positions(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        term_id: &str,
        curve_id: &str,
    ) -> Result<()> {
        debug!("Updating vault from positions: term={}, curve={}", term_id, curve_id);

        // Get position aggregates for this vault in a single query
        let (position_count, position_net_assets): (i64, Option<String>) = sqlx::query_as(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE shares > 0) as position_count,
                COALESCE(
                    SUM(total_deposit_assets_after_total_fees - total_redeem_assets_for_receiver),
                    0
                )::text as position_net_assets
            FROM position
            WHERE term_id = $1 AND curve_id = $2
            "#,
        )
        .bind(term_id)
        .bind(curve_id)
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| SyncError::Sqlx(e))?;

        let position_net_assets = position_net_assets.unwrap_or_else(|| "0".to_string());

        debug!("Found {} active positions for vault, net assets from positions: {}",
               position_count, position_net_assets);

        // Update vault with new metrics
        // For total_assets:
        // - If we have price data (from SharePriceChanged events), use total_shares * current_share_price / 1e18
        // - If we don't have price data yet (only deposits), use sum of position deposits as a fallback
        let rows_affected = sqlx::query(
            r#"
            UPDATE vault
            SET position_count = $1,
                total_assets = CASE
                    -- If we have valid price data, calculate from shares and price
                    WHEN current_share_price > 0 AND total_shares IS NOT NULL THEN
                        (total_shares * current_share_price) / 1000000000000000000
                    -- Otherwise, use pre-calculated sum of deposits from positions
                    ELSE $4::numeric(78,0)
                END,
                market_cap = CASE
                    WHEN total_shares IS NULL OR current_share_price IS NULL THEN NULL
                    WHEN total_shares = 0 OR current_share_price = 0 THEN 0
                    -- Check if multiplication would overflow by comparing with max numeric
                    -- Use NULLIF to prevent division by zero in the overflow check
                    WHEN total_shares > (10^78 - 1) / NULLIF(current_share_price, 0) THEN NULL
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
        .bind(&position_net_assets)
        .execute(&mut **tx)
        .await
        .map_err(|e| SyncError::Sqlx(e))?
        .rows_affected();

        if rows_affected == 0 {
            debug!("No vault found for term={}, curve={}", term_id, curve_id);
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
                    -- Use NULLIF to prevent division by zero in the overflow check
                    WHEN total_shares > (10^78 - 1) / NULLIF(current_share_price, 0) THEN NULL
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
            debug!("No vault found for term={}, curve={}", term_id, curve_id);
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
