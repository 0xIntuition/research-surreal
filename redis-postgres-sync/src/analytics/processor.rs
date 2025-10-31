// Analytics table update logic
// Updates triple_vault, triple_term, predicate_object, subject_predicate tables

use crate::{consumer::TermUpdateMessage, error::{Result, SyncError}};
use sqlx::PgPool;
use tracing::{debug, info};

const TRIPLE_BATCH_SIZE: i64 = 100;

pub async fn update_analytics_tables(
    pool: &PgPool,
    term_update: &TermUpdateMessage,
) -> Result<()> {
    // Process triples in batches to avoid loading too many into memory at once
    let mut offset = 0i64;
    let mut total_processed = 0usize;

    loop {
        // Find a batch of triples that reference this term (as the triple itself, counter, subject, predicate, or object)
        let affected_triples: Vec<(String, String, String, String, String)> = sqlx::query_as(
            r#"
            SELECT
                term_id::text,
                counter_term_id::text,
                subject_id::text,
                predicate_id::text,
                object_id::text
            FROM triple
            WHERE term_id = $1
               OR counter_term_id = $1
               OR subject_id = $1
               OR predicate_id = $1
               OR object_id = $1
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
        .map_err(|e| SyncError::Sqlx(e))?;

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
        for (term_id, counter_term_id) in triple_pairs {
            update_triple_vault(&mut tx, &term_id, &counter_term_id).await?;
            update_triple_term(&mut tx, &term_id, &counter_term_id).await?;
        }

        // Batch update predicate_object for all unique predicate-object pairs
        for (predicate_id, object_id) in predicate_object_pairs {
            update_predicate_object(&mut tx, &predicate_id, &object_id).await?;
        }

        // Batch update subject_predicate for all unique subject-predicate pairs
        for (subject_id, predicate_id) in subject_predicate_pairs {
            update_subject_predicate(&mut tx, &subject_id, &predicate_id).await?;
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

    if total_processed == 0 {
        info!("No triples affected by term update: {}", term_update.term_id);
    } else {
        info!(
            "Processed {} triples affected by term {}",
            total_processed,
            term_update.term_id
        );
    }

    Ok(())
}

async fn update_triple_vault(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    term_id: &str,
    counter_term_id: &str,
) -> Result<()> {
    // Aggregate pro vault + counter vault data for each curve_id
    // First get all unique curve_ids for both terms, then join vaults for each
    let result = sqlx::query(
        r#"
        INSERT INTO triple_vault (term_id, counter_term_id, curve_id, total_shares, total_assets, position_count, market_cap, updated_at)
        SELECT
            $1::text as term_id,
            $2::text as counter_term_id,
            curves.curve_id,
            COALESCE(v1.total_shares, 0) + COALESCE(v2.total_shares, 0) as total_shares,
            COALESCE(v1.total_assets, 0) + COALESCE(v2.total_assets, 0) as total_assets,
            COALESCE(v1.position_count, 0) + COALESCE(v2.position_count, 0) as position_count,
            COALESCE(v1.market_cap, 0) + COALESCE(v2.market_cap, 0) as market_cap,
            NOW() as updated_at
        FROM (
            SELECT DISTINCT curve_id
            FROM vault
            WHERE term_id IN ($1, $2)
        ) curves
        LEFT JOIN vault v1 ON v1.term_id = $1 AND v1.curve_id = curves.curve_id
        LEFT JOIN vault v2 ON v2.term_id = $2 AND v2.curve_id = curves.curve_id
        ON CONFLICT (term_id, counter_term_id, curve_id) DO UPDATE
        SET total_shares = EXCLUDED.total_shares,
            total_assets = EXCLUDED.total_assets,
            position_count = EXCLUDED.position_count,
            market_cap = EXCLUDED.market_cap,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(term_id)
    .bind(counter_term_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| SyncError::Sqlx(e))?;

    debug!(
        "Updated triple_vault for term {} (counter: {}): {} rows affected",
        term_id, counter_term_id, result.rows_affected()
    );

    Ok(())
}

async fn update_triple_term(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    term_id: &str,
    counter_term_id: &str,
) -> Result<()> {
    // Aggregate triple_vault across all curves
    let result = sqlx::query(
        r#"
        INSERT INTO triple_term (term_id, counter_term_id, total_assets, total_market_cap, total_position_count, updated_at)
        SELECT
            term_id,
            counter_term_id,
            COALESCE(SUM(total_assets), 0),
            COALESCE(SUM(market_cap), 0),
            COALESCE(SUM(position_count), 0),
            NOW()
        FROM triple_vault
        WHERE term_id = $1 AND counter_term_id = $2
        GROUP BY term_id, counter_term_id
        ON CONFLICT (term_id, counter_term_id) DO UPDATE
        SET total_assets = EXCLUDED.total_assets,
            total_market_cap = EXCLUDED.total_market_cap,
            total_position_count = EXCLUDED.total_position_count,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(term_id)
    .bind(counter_term_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| SyncError::Sqlx(e))?;

    debug!(
        "Updated triple_term for term {} (counter: {}): {} rows affected",
        term_id, counter_term_id, result.rows_affected()
    );

    Ok(())
}

async fn update_predicate_object(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    predicate_id: &str,
    object_id: &str,
) -> Result<()> {
    // Aggregate by predicate-object pairs
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
        WHERE t.predicate_id = $1 AND t.object_id = $2
        GROUP BY t.predicate_id, t.object_id
        ON CONFLICT (predicate_id, object_id) DO UPDATE
        SET triple_count = EXCLUDED.triple_count,
            total_position_count = EXCLUDED.total_position_count,
            total_market_cap = EXCLUDED.total_market_cap,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(predicate_id)
    .bind(object_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| SyncError::Sqlx(e))?;

    debug!(
        "Updated predicate_object for predicate {} (object: {}): {} rows affected",
        predicate_id, object_id, result.rows_affected()
    );

    Ok(())
}

async fn update_subject_predicate(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    subject_id: &str,
    predicate_id: &str,
) -> Result<()> {
    // Aggregate by subject-predicate pairs
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
        WHERE t.subject_id = $1 AND t.predicate_id = $2
        GROUP BY t.subject_id, t.predicate_id
        ON CONFLICT (subject_id, predicate_id) DO UPDATE
        SET triple_count = EXCLUDED.triple_count,
            total_position_count = EXCLUDED.total_position_count,
            total_market_cap = EXCLUDED.total_market_cap,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(subject_id)
    .bind(predicate_id)
    .execute(&mut **tx)
    .await
    .map_err(|e| SyncError::Sqlx(e))?;

    debug!(
        "Updated subject_predicate for subject {} (predicate: {}): {} rows affected",
        subject_id, predicate_id, result.rows_affected()
    );

    Ok(())
}
