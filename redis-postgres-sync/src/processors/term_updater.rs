use sqlx::{Postgres, Transaction};
use tracing::{debug, warn};

use crate::error::{Result, SyncError};

/// TermUpdater handles term table updates based on vault aggregations
pub struct TermUpdater;

impl TermUpdater {
    pub fn new() -> Self {
        Self
    }

    /// Update term metrics by aggregating across all vaults for this term
    /// Calculates:
    /// - total_assets: sum of total_assets across all vaults
    /// - total_market_cap: sum of market_cap across all vaults
    /// Returns a list of updated term IDs (for Redis publishing)
    pub async fn update_term_from_vaults(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        term_id: &str,
    ) -> Result<Vec<String>> {
        debug!("Updating term from vaults: term={}", term_id);

        // Aggregate vault data for this term
        // Using String for NUMERIC values since they're large (78,0)
        let aggregate: Option<(String, String)> = sqlx::query_as(
            r#"
            SELECT
                COALESCE(SUM(total_assets), 0)::text as total_assets,
                COALESCE(SUM(market_cap), 0)::text as total_market_cap
            FROM vault
            WHERE term_id = $1
            "#,
        )
        .bind(term_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| SyncError::Sqlx(e))?;

        let (total_assets, total_market_cap) = aggregate.unwrap_or_else(|| {
            ("0".to_string(), "0".to_string())
        });

        debug!("Aggregated vault data: assets={}, market_cap={}", total_assets, total_market_cap);

        // Update or insert term
        let rows_affected = sqlx::query(
            r#"
            INSERT INTO term (id, type, atom_id, triple_id, total_assets, total_market_cap, created_at, updated_at)
            SELECT
                $1::numeric(78,0),
                CASE
                    WHEN EXISTS (SELECT 1 FROM atom WHERE term_id = $1) THEN 'Atom'
                    WHEN EXISTS (SELECT 1 FROM triple WHERE term_id = $1) THEN 'Triple'
                    ELSE 'Unknown'
                END,
                CASE
                    WHEN EXISTS (SELECT 1 FROM atom WHERE term_id = $1) THEN $1::numeric(78,0)
                    ELSE NULL
                END,
                CASE
                    WHEN EXISTS (SELECT 1 FROM triple WHERE term_id = $1) THEN $1::numeric(78,0)
                    ELSE NULL
                END,
                $2,
                $3,
                NOW(),
                NOW()
            ON CONFLICT (id) DO UPDATE
            SET total_assets = EXCLUDED.total_assets,
                total_market_cap = EXCLUDED.total_market_cap,
                updated_at = NOW()
            "#,
        )
        .bind(term_id)
        .bind(total_assets)
        .bind(total_market_cap)
        .execute(&mut **tx)
        .await
        .map_err(|e| SyncError::Sqlx(e))?
        .rows_affected();

        if rows_affected > 0 {
            debug!("Updated term {}", term_id);
            Ok(vec![term_id.to_string()])
        } else {
            warn!("Failed to update term {}", term_id);
            Ok(vec![])
        }
    }

    /// Initialize a term entry for a newly created atom
    pub async fn initialize_atom_term(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        term_id: &str,
    ) -> Result<()> {
        debug!("Initializing atom term: term={}", term_id);

        sqlx::query(
            r#"
            INSERT INTO term (id, type, atom_id, triple_id, total_assets, total_market_cap, created_at, updated_at)
            VALUES ($1, 'Atom', $1, NULL, 0, 0, NOW(), NOW())
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(term_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| SyncError::Sqlx(e))?;

        debug!("Initialized atom term");
        Ok(())
    }

    /// Initialize a term entry for a newly created triple
    pub async fn initialize_triple_term(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        term_id: &str,
    ) -> Result<()> {
        debug!("Initializing triple term: term={}", term_id);

        sqlx::query(
            r#"
            INSERT INTO term (id, type, atom_id, triple_id, total_assets, total_market_cap, created_at, updated_at)
            VALUES ($1, 'Triple', NULL, $1, 0, 0, NOW(), NOW())
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(term_id)
        .execute(&mut **tx)
        .await
        .map_err(|e| SyncError::Sqlx(e))?;

        debug!("Initialized triple term");
        Ok(())
    }

    /// Get counter_term_id for a triple term (if it exists)
    /// Returns None if the term is not a triple or doesn't exist
    pub async fn get_counter_term_id(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        term_id: &str,
    ) -> Result<Option<String>> {
        let counter_term: Option<(String,)> = sqlx::query_as(
            r#"
            SELECT counter_term_id::text
            FROM triple
            WHERE term_id = $1
            "#,
        )
        .bind(term_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|e| SyncError::Sqlx(e))?;

        Ok(counter_term.map(|(id,)| id))
    }
}

impl Default for TermUpdater {
    fn default() -> Self {
        Self::new()
    }
}
