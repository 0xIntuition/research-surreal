// Analytics table update logic
// Updates triple_vault, triple_term, predicate_object, subject_predicate tables

use crate::{consumer::TermUpdateMessage, error::{Result, SyncError}};
use sqlx::PgPool;
use tracing::{debug, info};

pub async fn update_analytics_tables(
    pool: &PgPool,
    term_update: &TermUpdateMessage,
) -> Result<()> {
    // Find all triples that reference this term (as the triple itself, counter, subject, predicate, or object)
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
        "#,
    )
    .bind(&term_update.term_id)
    .fetch_all(pool)
    .await
    .map_err(|e| SyncError::Sqlx(e))?;

    if affected_triples.is_empty() {
        info!("No triples affected by term update: {}", term_update.term_id);
        return Ok(());
    }

    info!(
        "Found {} triples affected by term {}",
        affected_triples.len(),
        term_update.term_id
    );

    // Start a transaction for all updates
    let mut tx = pool.begin().await.map_err(SyncError::Sqlx)?;

    // Update analytics for each affected triple
    for (term_id, counter_term_id, subject_id, predicate_id, object_id) in affected_triples {
        // Update triple_vault (combine pro + counter vault data)
        update_triple_vault(&mut tx, &term_id, &counter_term_id).await?;

        // Update triple_term (aggregate triple_vault by term)
        update_triple_term(&mut tx, &term_id, &counter_term_id).await?;

        // Update predicate_object aggregates
        update_predicate_object(&mut tx, &predicate_id, &object_id).await?;

        // Update subject_predicate aggregates
        update_subject_predicate(&mut tx, &subject_id, &predicate_id).await?;
    }

    // Commit the transaction
    tx.commit().await.map_err(SyncError::Sqlx)?;

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
