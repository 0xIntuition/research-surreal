use sqlx::{Postgres, Transaction};
use tracing::debug;

use super::term_updater::TermUpdater;
use super::vault_updater::VaultUpdater;
use crate::error::{Result, SyncError};

/// CascadeProcessor handles cascading updates through the table hierarchy
/// after events are inserted into event tables and processed by triggers.
///
/// Flow:
/// 1. Event inserted -> Trigger updates base table (position/vault/atom/triple)
/// 2. CascadeProcessor runs in same transaction
/// 3. Aggregates vault data from positions
/// 4. Aggregates term data from vaults
/// 5. Publishes to Redis for analytics worker
pub struct CascadeProcessor {
    vault_updater: VaultUpdater,
    term_updater: TermUpdater,
}

impl CascadeProcessor {
    pub fn new() -> Self {
        Self {
            vault_updater: VaultUpdater::new(),
            term_updater: TermUpdater::new(),
        }
    }

    /// Process cascade after a deposit or redeem event
    /// This is called AFTER the trigger has updated the position table
    pub async fn process_position_change(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        account_id: &str,
        term_id: &str,
        curve_id: &str,
    ) -> Result<()> {
        debug!(
            "Processing cascade for position change: account={}, term={}, curve={}",
            account_id, term_id, curve_id
        );

        // Acquire advisory lock for this position to prevent concurrent updates
        let lock_id = Self::hash_position(account_id, term_id, curve_id);
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(lock_id)
            .execute(&mut **tx)
            .await
            .map_err(SyncError::Sqlx)?;

        debug!("Acquired advisory lock {} for position", lock_id);

        // Update vault aggregates (position_count and derived metrics)
        self.vault_updater
            .update_vault_from_positions(tx, term_id, curve_id)
            .await?;

        // Update term aggregates (sum across all vaults for this term)
        let updated_term_ids = self
            .term_updater
            .update_term_from_vaults(tx, term_id)
            .await?;

        debug!(
            "Cascade completed for position change. Updated {} terms",
            updated_term_ids.len()
        );

        // Note: Redis publishing happens after transaction commit
        // See process_position_change_with_redis for the full flow
        Ok(())
    }

    /// Process cascade after a share price change event
    /// This is called AFTER the trigger has updated the vault table
    pub async fn process_price_change(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        term_id: &str,
        curve_id: &str,
    ) -> Result<()> {
        debug!(
            "Processing cascade for price change: term={}, curve={}",
            term_id, curve_id
        );

        // Acquire advisory lock for this vault to prevent concurrent updates
        let lock_id = Self::hash_vault(term_id, curve_id);
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(lock_id)
            .execute(&mut **tx)
            .await
            .map_err(SyncError::Sqlx)?;

        debug!("Acquired advisory lock {} for vault", lock_id);

        // Recalculate vault market_cap based on new share price
        self.vault_updater
            .recalculate_market_cap(tx, term_id, curve_id)
            .await?;

        // Update term aggregates
        let updated_term_ids = self
            .term_updater
            .update_term_from_vaults(tx, term_id)
            .await?;

        debug!(
            "Cascade completed for price change. Updated {} terms",
            updated_term_ids.len()
        );

        Ok(())
    }

    /// Process cascade for atom creation
    /// This is called AFTER the trigger has updated the atom table
    pub async fn process_atom_creation(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        term_id: &str,
    ) -> Result<()> {
        debug!("Processing cascade for atom creation: term={}", term_id);

        // Initialize term entry for this atom
        self.term_updater.initialize_atom_term(tx, term_id).await?;

        debug!("Cascade completed for atom creation");
        Ok(())
    }

    /// Process cascade for triple creation
    /// This is called AFTER the trigger has updated the triple table
    pub async fn process_triple_creation(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        term_id: &str,
        counter_term_id: &str,
    ) -> Result<()> {
        debug!(
            "Processing cascade for triple creation: term={}, counter_term={}",
            term_id, counter_term_id
        );

        // Initialize term entries for this triple (both pro and counter)
        self.term_updater
            .initialize_triple_term(tx, term_id)
            .await?;
        self.term_updater
            .initialize_triple_term(tx, counter_term_id)
            .await?;

        debug!("Cascade completed for triple creation");
        Ok(())
    }

    /// Hash a position key to get a lock ID for advisory locks
    /// This ensures that concurrent updates to the same position are serialized
    /// Uses a stable hash to minimize collision risks compared to DefaultHasher
    fn hash_position(account_id: &str, term_id: &str, curve_id: &str) -> i64 {
        // Use FNV-1a-like hash for better distribution
        let mut hash = 0xcbf29ce484222325u64; // FNV offset basis

        for byte in account_id.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3); // FNV prime
        }
        for byte in term_id.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        for byte in curve_id.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }

        // XOR fold to 63 bits to avoid sign issues with advisory locks
        ((hash >> 32) ^ (hash & 0xFFFFFFFF)) as i64 & 0x7FFFFFFFFFFFFFFF
    }

    /// Hash a vault key to get a lock ID for advisory locks
    /// Uses FNV-1a hash for deterministic, collision-resistant hashing
    fn hash_vault(term_id: &str, curve_id: &str) -> i64 {
        // Use FNV-1a-like hash for better distribution
        let mut hash = 0xcbf29ce484222325u64; // FNV offset basis

        for byte in term_id.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3); // FNV prime
        }
        for byte in curve_id.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }

        // XOR fold to 63 bits to avoid sign issues with advisory locks
        ((hash >> 32) ^ (hash & 0xFFFFFFFF)) as i64 & 0x7FFFFFFFFFFFFFFF
    }
}

impl Default for CascadeProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_position_deterministic() {
        let hash1 = CascadeProcessor::hash_position("acc1", "term1", "curve1");
        let hash2 = CascadeProcessor::hash_position("acc1", "term1", "curve1");
        assert_eq!(hash1, hash2, "Hash should be deterministic");
    }

    #[test]
    fn test_hash_position_different_inputs() {
        let hash1 = CascadeProcessor::hash_position("acc1", "term1", "curve1");
        let hash2 = CascadeProcessor::hash_position("acc2", "term1", "curve1");
        assert_ne!(
            hash1, hash2,
            "Different inputs should produce different hashes"
        );
    }

    #[test]
    fn test_hash_vault_deterministic() {
        let hash1 = CascadeProcessor::hash_vault("term1", "curve1");
        let hash2 = CascadeProcessor::hash_vault("term1", "curve1");
        assert_eq!(hash1, hash2, "Hash should be deterministic");
    }
}
