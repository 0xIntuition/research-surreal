// Analytics table update logic
// Updates triple_vault, triple_term, predicate_object, subject_predicate tables

use crate::{
    consumer::TermUpdateMessage,
    error::{Result, SyncError},
    monitoring::metrics::Metrics,
};
use alloy_primitives::keccak256;
use sqlx::PgPool;
use std::sync::Arc;
use tracing::debug;

const TRIPLE_BATCH_SIZE: i64 = 100;

/// Acquire an advisory lock for a triple pair using PostgreSQL's advisory lock mechanism
/// The lock is automatically released when the transaction commits or rolls back
async fn acquire_triple_lock(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    term_id: &str,
    counter_term_id: &str,
) -> Result<()> {
    // Create a stable hash for the triple pair using keccak256
    // This ensures stability across Rust versions and provides cryptographic collision resistance
    let combined = format!("{term_id}:{counter_term_id}");
    let hash = keccak256(combined.as_bytes());

    // Convert first 8 bytes to i64, ensuring positive value for pg_advisory_xact_lock(bigint)
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&hash[..8]);
    let lock_id = i64::from_be_bytes(bytes) & 0x7FFFFFFFFFFFFFFF;

    // Use pg_advisory_xact_lock which is transaction-scoped and automatically released
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(lock_id)
        .execute(&mut **tx)
        .await
        .map_err(SyncError::Sqlx)?;

    debug!(
        "Acquired advisory lock for triple pair: {} / {} (lock_id: {})",
        term_id, counter_term_id, lock_id
    );
    Ok(())
}

pub async fn update_analytics_tables(
    pool: &PgPool,
    metrics: &Arc<Metrics>,
    term_update: &TermUpdateMessage,
) -> Result<()> {
    // Process triples in batches to avoid loading too many into memory at once
    let mut offset = 0i64;
    let mut total_processed = 0usize;

    loop {
        // Find a batch of triples that reference this term (as the triple itself, counter, subject, predicate, or object)
        // Using UNION instead of OR for better query planning and index usage
        let affected_triples: Vec<(String, String, String, String, String)> = sqlx::query_as(
            r#"
            SELECT DISTINCT
                term_id::text,
                counter_term_id::text,
                subject_id::text,
                predicate_id::text,
                object_id::text
            FROM (
                SELECT term_id, counter_term_id, subject_id, predicate_id, object_id
                FROM triple WHERE term_id = $1
                UNION
                SELECT term_id, counter_term_id, subject_id, predicate_id, object_id
                FROM triple WHERE counter_term_id = $1
                UNION
                SELECT term_id, counter_term_id, subject_id, predicate_id, object_id
                FROM triple WHERE subject_id = $1
                UNION
                SELECT term_id, counter_term_id, subject_id, predicate_id, object_id
                FROM triple WHERE predicate_id = $1
                UNION
                SELECT term_id, counter_term_id, subject_id, predicate_id, object_id
                FROM triple WHERE object_id = $1
            ) all_triples
            ORDER BY term_id, counter_term_id
            LIMIT $2
            OFFSET $3
            "#,
        )
        .bind(&term_update.term_id)
        .bind(TRIPLE_BATCH_SIZE)
        .bind(offset)
        .fetch_all(pool)
        .await
        .map_err(SyncError::Sqlx)?;

        if affected_triples.is_empty() {
            break;
        }

        debug!(
            "Processing batch of {} triples (offset: {}) for term {}",
            affected_triples.len(),
            offset,
            term_update.term_id
        );

        // Start a transaction for this batch
        let mut tx = pool.begin().await.map_err(SyncError::Sqlx)?;

        // Collect unique combinations to avoid duplicate updates
        // This significantly reduces the N+1 query problem by deduplicating updates
        // Note: This could be further optimized by using a single SQL query with VALUES or unnest,
        // but that would require more complex SQL generation and is left for future optimization.
        use std::collections::HashSet;
        let mut triple_pairs: HashSet<(String, String)> = HashSet::new();
        let mut predicate_object_pairs: HashSet<(String, String)> = HashSet::new();
        let mut subject_predicate_pairs: HashSet<(String, String)> = HashSet::new();

        for (term_id, counter_term_id, subject_id, predicate_id, object_id) in &affected_triples {
            triple_pairs.insert((term_id.clone(), counter_term_id.clone()));
            predicate_object_pairs.insert((predicate_id.clone(), object_id.clone()));
            subject_predicate_pairs.insert((subject_id.clone(), predicate_id.clone()));
        }

        debug!(
            "Batch contains {} unique triple pairs, {} predicate-object pairs, {} subject-predicate pairs",
            triple_pairs.len(),
            predicate_object_pairs.len(),
            subject_predicate_pairs.len()
        );

        // Batch update triple_vault and triple_term for all unique term pairs
        // Convert to vectors for batch processing
        let triple_pairs_vec: Vec<(String, String)> = triple_pairs.into_iter().collect();
        let predicate_object_vec: Vec<(String, String)> =
            predicate_object_pairs.into_iter().collect();
        let subject_predicate_vec: Vec<(String, String)> =
            subject_predicate_pairs.into_iter().collect();

        // Acquire all advisory locks first to prevent deadlocks
        for (term_id, counter_term_id) in &triple_pairs_vec {
            // Using pg_advisory_xact_lock which is automatically released at transaction end
            acquire_triple_lock(&mut tx, term_id, counter_term_id).await?;
        }

        // Batch update all triple pairs in fewer queries
        if !triple_pairs_vec.is_empty() {
            update_triple_vault_batch(&mut tx, &triple_pairs_vec).await?;
            update_triple_term_batch(&mut tx, &triple_pairs_vec).await?;
        }

        // Batch update predicate_object for all unique predicate-object pairs
        if !predicate_object_vec.is_empty() {
            update_predicate_object_batch(&mut tx, &predicate_object_vec).await?;
        }

        // Batch update subject_predicate for all unique subject-predicate pairs
        if !subject_predicate_vec.is_empty() {
            update_subject_predicate_batch(&mut tx, &subject_predicate_vec).await?;
        }

        // Commit the transaction for this batch
        tx.commit().await.map_err(SyncError::Sqlx)?;

        total_processed += affected_triples.len();
        offset += affected_triples.len() as i64;

        // If we got fewer triples than the batch size, we've processed all triples
        if (affected_triples.len() as i64) < TRIPLE_BATCH_SIZE {
            break;
        }
    }

    if total_processed > 0 {
        debug!(
            "Processed {} triples affected by term {}",
            total_processed, term_update.term_id
        );
    }

    // Record how many triples were affected by this term update
    metrics.record_analytics_affected_triples(total_processed);

    Ok(())
}

/// Batch update triple_vault for multiple term pairs using a single query
async fn update_triple_vault_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    pairs: &[(String, String)],
) -> Result<()> {
    if pairs.is_empty() {
        return Ok(());
    }

    // Build arrays for bulk update using unnest
    let term_ids: Vec<&str> = pairs.iter().map(|(t, _)| t.as_str()).collect();
    let counter_term_ids: Vec<&str> = pairs.iter().map(|(_, c)| c.as_str()).collect();

    let result = sqlx::query(
        r#"
        INSERT INTO triple_vault (term_id, counter_term_id, curve_id, total_shares, total_assets, position_count, market_cap, updated_at)
        SELECT
            pairs.term_id,
            pairs.counter_term_id,
            curves.curve_id,
            COALESCE(v1.total_shares, 0) + COALESCE(v2.total_shares, 0) as total_shares,
            COALESCE(v1.total_assets, 0) + COALESCE(v2.total_assets, 0) as total_assets,
            COALESCE(v1.position_count, 0) + COALESCE(v2.position_count, 0) as position_count,
            COALESCE(v1.market_cap, 0) + COALESCE(v2.market_cap, 0) as market_cap,
            NOW() as updated_at
        FROM (
            SELECT unnest($1::text[]) as term_id, unnest($2::text[]) as counter_term_id
        ) pairs
        CROSS JOIN (
            SELECT DISTINCT curve_id
            FROM vault
            WHERE term_id = ANY($1::text[]) OR term_id = ANY($2::text[])
        ) curves
        LEFT JOIN vault v1 ON v1.term_id = pairs.term_id AND v1.curve_id = curves.curve_id
        LEFT JOIN vault v2 ON v2.term_id = pairs.counter_term_id AND v2.curve_id = curves.curve_id
        ON CONFLICT (term_id, counter_term_id, curve_id) DO UPDATE
        SET total_shares = EXCLUDED.total_shares,
            total_assets = EXCLUDED.total_assets,
            position_count = EXCLUDED.position_count,
            market_cap = EXCLUDED.market_cap,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(&term_ids)
    .bind(&counter_term_ids)
    .execute(&mut **tx)
    .await
    .map_err(SyncError::Sqlx)?;

    debug!(
        "Batch updated triple_vault for {} pairs: {} rows affected",
        pairs.len(),
        result.rows_affected()
    );

    Ok(())
}

/// Batch update triple_term for multiple term pairs using a single query
async fn update_triple_term_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    pairs: &[(String, String)],
) -> Result<()> {
    if pairs.is_empty() {
        return Ok(());
    }

    let term_ids: Vec<&str> = pairs.iter().map(|(t, _)| t.as_str()).collect();
    let counter_term_ids: Vec<&str> = pairs.iter().map(|(_, c)| c.as_str()).collect();

    let result = sqlx::query(
        r#"
        INSERT INTO triple_term (term_id, counter_term_id, total_assets, total_market_cap, total_position_count, updated_at)
        SELECT
            tv.term_id,
            tv.counter_term_id,
            COALESCE(SUM(tv.total_assets), 0),
            COALESCE(SUM(tv.market_cap), 0),
            COALESCE(SUM(tv.position_count), 0),
            NOW()
        FROM triple_vault tv
        INNER JOIN (
            SELECT unnest($1::text[]) as term_id, unnest($2::text[]) as counter_term_id
        ) pairs ON tv.term_id = pairs.term_id AND tv.counter_term_id = pairs.counter_term_id
        GROUP BY tv.term_id, tv.counter_term_id
        ON CONFLICT (term_id, counter_term_id) DO UPDATE
        SET total_assets = EXCLUDED.total_assets,
            total_market_cap = EXCLUDED.total_market_cap,
            total_position_count = EXCLUDED.total_position_count,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(&term_ids)
    .bind(&counter_term_ids)
    .execute(&mut **tx)
    .await
    .map_err(SyncError::Sqlx)?;

    debug!(
        "Batch updated triple_term for {} pairs: {} rows affected",
        pairs.len(),
        result.rows_affected()
    );

    Ok(())
}

/// Batch update predicate_object for multiple pairs
async fn update_predicate_object_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    pairs: &[(String, String)],
) -> Result<()> {
    if pairs.is_empty() {
        return Ok(());
    }

    let predicate_ids: Vec<&str> = pairs.iter().map(|(p, _)| p.as_str()).collect();
    let object_ids: Vec<&str> = pairs.iter().map(|(_, o)| o.as_str()).collect();

    let result = sqlx::query(
        r#"
        INSERT INTO predicate_object (predicate_id, object_id, triple_count, total_position_count, total_market_cap, updated_at)
        SELECT
            t.predicate_id,
            t.object_id,
            COUNT(DISTINCT t.term_id),
            COALESCE(SUM(tt.total_position_count), 0),
            COALESCE(SUM(tt.total_market_cap), 0),
            NOW()
        FROM triple t
        LEFT JOIN triple_term tt ON t.term_id = tt.term_id AND t.counter_term_id = tt.counter_term_id
        INNER JOIN (
            SELECT unnest($1::text[]) as predicate_id, unnest($2::text[]) as object_id
        ) pairs ON t.predicate_id = pairs.predicate_id AND t.object_id = pairs.object_id
        GROUP BY t.predicate_id, t.object_id
        ON CONFLICT (predicate_id, object_id) DO UPDATE
        SET triple_count = EXCLUDED.triple_count,
            total_position_count = EXCLUDED.total_position_count,
            total_market_cap = EXCLUDED.total_market_cap,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(&predicate_ids)
    .bind(&object_ids)
    .execute(&mut **tx)
    .await
    .map_err(SyncError::Sqlx)?;

    debug!(
        "Batch updated predicate_object for {} pairs: {} rows affected",
        pairs.len(),
        result.rows_affected()
    );

    Ok(())
}

/// Batch update subject_predicate for multiple pairs
async fn update_subject_predicate_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    pairs: &[(String, String)],
) -> Result<()> {
    if pairs.is_empty() {
        return Ok(());
    }

    let subject_ids: Vec<&str> = pairs.iter().map(|(s, _)| s.as_str()).collect();
    let predicate_ids: Vec<&str> = pairs.iter().map(|(_, p)| p.as_str()).collect();

    let result = sqlx::query(
        r#"
        INSERT INTO subject_predicate (subject_id, predicate_id, triple_count, total_position_count, total_market_cap, updated_at)
        SELECT
            t.subject_id,
            t.predicate_id,
            COUNT(DISTINCT t.term_id),
            COALESCE(SUM(tt.total_position_count), 0),
            COALESCE(SUM(tt.total_market_cap), 0),
            NOW()
        FROM triple t
        LEFT JOIN triple_term tt ON t.term_id = tt.term_id AND t.counter_term_id = tt.counter_term_id
        INNER JOIN (
            SELECT unnest($1::text[]) as subject_id, unnest($2::text[]) as predicate_id
        ) pairs ON t.subject_id = pairs.subject_id AND t.predicate_id = pairs.predicate_id
        GROUP BY t.subject_id, t.predicate_id
        ON CONFLICT (subject_id, predicate_id) DO UPDATE
        SET triple_count = EXCLUDED.triple_count,
            total_position_count = EXCLUDED.total_position_count,
            total_market_cap = EXCLUDED.total_market_cap,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(&subject_ids)
    .bind(&predicate_ids)
    .execute(&mut **tx)
    .await
    .map_err(SyncError::Sqlx)?;

    debug!(
        "Batch updated subject_predicate for {} pairs: {} rows affected",
        pairs.len(),
        result.rows_affected()
    );

    Ok(())
}
