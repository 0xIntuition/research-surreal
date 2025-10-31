// Analytics table update logic
// Updates triple_vault, triple_term, predicate_object, subject_predicate tables

use crate::{consumer::TermUpdateMessage, error::{Result, SyncError}};
use sqlx::PgPool;
use tracing::{debug, info, warn};

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
    // Strip 0x prefix from counter_term_id if present (vault.term_id doesn't have 0x prefix)
    let counter_term_clean = counter_term_id.strip_prefix("0x").unwrap_or(counter_term_id);

    // Aggregate pro vault + counter vault data for each curve_id
    sqlx::query(
        r#"
        INSERT INTO triple_vault (term_id, counter_term_id, curve_id, total_shares, total_assets, position_count, market_cap, updated_at)
        SELECT
            $1::text,
            $2::text,
            COALESCE(v1.curve_id, v2.curve_id),
            COALESCE(v1.total_shares, 0) + COALESCE(v2.total_shares, 0),
            COALESCE(v1.total_assets, 0) + COALESCE(v2.total_assets, 0),
            COALESCE(v1.position_count, 0) + COALESCE(v2.position_count, 0),
            COALESCE(v1.market_cap, 0) + COALESCE(v2.market_cap, 0),
            NOW()
        FROM vault v1
        FULL OUTER JOIN vault v2 ON v1.curve_id = v2.curve_id
        WHERE v1.term_id = $1 OR v2.term_id = $3
        ON CONFLICT (term_id, counter_term_id, curve_id) DO UPDATE
        SET total_shares = EXCLUDED.total_shares,
            total_assets = EXCLUDED.total_assets,
            position_count = EXCLUDED.position_count,
            market_cap = EXCLUDED.market_cap,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(term_id)
    .bind(counter_term_id) // Keep original with 0x for storage
    .bind(counter_term_clean) // Use cleaned version for vault lookup
    .execute(&mut **tx)
    .await
    .map_err(|e| SyncError::Sqlx(e))?;

    Ok(())
}

async fn update_triple_term(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    term_id: &str,
    counter_term_id: &str,
) -> Result<()> {
    // Aggregate triple_vault across all curves
    sqlx::query(
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

    Ok(())
}

async fn update_predicate_object(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    predicate_id: &str,
    object_id: &str,
) -> Result<()> {
    // Aggregate by predicate-object pairs
    sqlx::query(
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

    Ok(())
}

async fn update_subject_predicate(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    subject_id: &str,
    predicate_id: &str,
) -> Result<()> {
    // Aggregate by subject-predicate pairs
    sqlx::query(
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

    Ok(())
}
