-- Migration: Create snapshot comparison function
--
-- This migration creates a function to compare data between the snapshot schema
-- (materialized views) and the public schema (trigger-based tables) to validate
-- that both approaches produce the same results.
--
-- Usage:
--   1. Refresh snapshot views: SELECT * FROM snapshot.refresh_all_views();
--   2. Run comparison: SELECT * FROM compare_schemas();
--
-- The function compares only common fields (excluding ordering metadata like
-- last_event_block, last_event_log_index) and provides summary-level differences.

-- Drop existing function if it exists
DROP FUNCTION IF EXISTS compare_schemas() CASCADE;

-- Create the comparison function
CREATE OR REPLACE FUNCTION compare_schemas()
RETURNS TABLE(
    table_name TEXT,
    public_count BIGINT,
    snapshot_count BIGINT,
    count_match BOOLEAN,
    metric_diffs TEXT
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Compare position table
    RETURN QUERY
    WITH public_stats AS (
        SELECT
            COUNT(*) as cnt,
            SUM(shares) as total_shares,
            SUM(total_deposit_assets_after_total_fees) as total_deposits,
            SUM(total_redeem_assets_for_receiver) as total_redeems
        FROM public.position
    ),
    snapshot_stats AS (
        SELECT
            COUNT(*) as cnt,
            SUM(shares) as total_shares,
            SUM(total_deposit_assets_after_total_fees) as total_deposits,
            SUM(total_redeem_assets_for_receiver) as total_redeems
        FROM snapshot.position
    )
    SELECT
        'position'::TEXT,
        p.cnt,
        s.cnt,
        p.cnt = s.cnt,
        CASE
            WHEN p.cnt = s.cnt AND p.total_shares = s.total_shares
                AND p.total_deposits = s.total_deposits AND p.total_redeems = s.total_redeems
            THEN 'All metrics match'
            ELSE format('Shares: %s vs %s, Deposits: %s vs %s, Redeems: %s vs %s',
                p.total_shares, s.total_shares, p.total_deposits, s.total_deposits, p.total_redeems, s.total_redeems)
        END::TEXT
    FROM public_stats p, snapshot_stats s;

    -- Compare atom table
    RETURN QUERY
    WITH public_stats AS (
        SELECT COUNT(*) as cnt FROM public.atom
    ),
    snapshot_stats AS (
        SELECT COUNT(*) as cnt FROM snapshot.atom
    )
    SELECT
        'atom'::TEXT,
        p.cnt,
        s.cnt,
        p.cnt = s.cnt,
        CASE WHEN p.cnt = s.cnt THEN 'All metrics match' ELSE 'Row counts differ' END::TEXT
    FROM public_stats p, snapshot_stats s;

    -- Compare triple table
    RETURN QUERY
    WITH public_stats AS (
        SELECT COUNT(*) as cnt FROM public.triple
    ),
    snapshot_stats AS (
        SELECT COUNT(*) as cnt FROM snapshot.triple
    )
    SELECT
        'triple'::TEXT,
        p.cnt,
        s.cnt,
        p.cnt = s.cnt,
        CASE WHEN p.cnt = s.cnt THEN 'All metrics match' ELSE 'Row counts differ' END::TEXT
    FROM public_stats p, snapshot_stats s;

    -- Compare vault table
    RETURN QUERY
    WITH public_stats AS (
        SELECT
            COUNT(*) as cnt,
            SUM(total_shares) as total_shares,
            SUM(total_assets) as total_assets,
            SUM(market_cap) as total_market_cap,
            SUM(position_count) as total_positions
        FROM public.vault
    ),
    snapshot_stats AS (
        SELECT
            COUNT(*) as cnt,
            SUM(total_shares) as total_shares,
            SUM(total_assets) as total_assets,
            SUM(market_cap) as total_market_cap,
            SUM(position_count) as total_positions
        FROM snapshot.vault
    )
    SELECT
        'vault'::TEXT,
        p.cnt,
        s.cnt,
        p.cnt = s.cnt,
        CASE
            WHEN p.cnt = s.cnt AND p.total_shares = s.total_shares
                AND p.total_assets = s.total_assets AND p.total_market_cap = s.total_market_cap
                AND p.total_positions = s.total_positions
            THEN 'All metrics match'
            ELSE format('Shares: %s vs %s, Assets: %s vs %s, Market cap: %s vs %s, Positions: %s vs %s',
                p.total_shares, s.total_shares, p.total_assets, s.total_assets,
                p.total_market_cap, s.total_market_cap, p.total_positions, s.total_positions)
        END::TEXT
    FROM public_stats p, snapshot_stats s;

    -- Compare term table
    RETURN QUERY
    WITH public_stats AS (
        SELECT
            COUNT(*) as cnt,
            SUM(total_assets) as total_assets,
            SUM(total_market_cap) as total_market_cap
        FROM public.term
    ),
    snapshot_stats AS (
        SELECT
            COUNT(*) as cnt,
            SUM(total_assets) as total_assets,
            SUM(total_market_cap) as total_market_cap
        FROM snapshot.term
    )
    SELECT
        'term'::TEXT,
        p.cnt,
        s.cnt,
        p.cnt = s.cnt,
        CASE
            WHEN p.cnt = s.cnt AND p.total_assets = s.total_assets
                AND p.total_market_cap = s.total_market_cap
            THEN 'All metrics match'
            ELSE format('Assets: %s vs %s, Market cap: %s vs %s',
                p.total_assets, s.total_assets, p.total_market_cap, s.total_market_cap)
        END::TEXT
    FROM public_stats p, snapshot_stats s;

    -- Compare triple_vault table
    RETURN QUERY
    WITH public_stats AS (
        SELECT
            COUNT(*) as cnt,
            SUM(total_shares) as total_shares,
            SUM(total_assets) as total_assets,
            SUM(market_cap) as total_market_cap,
            SUM(position_count) as total_positions
        FROM public.triple_vault
    ),
    snapshot_stats AS (
        SELECT
            COUNT(*) as cnt,
            SUM(total_shares) as total_shares,
            SUM(total_assets) as total_assets,
            SUM(market_cap) as total_market_cap,
            SUM(position_count) as total_positions
        FROM snapshot.triple_vault
    )
    SELECT
        'triple_vault'::TEXT,
        p.cnt,
        s.cnt,
        p.cnt = s.cnt,
        CASE
            WHEN p.cnt = s.cnt AND p.total_shares = s.total_shares
                AND p.total_assets = s.total_assets AND p.total_market_cap = s.total_market_cap
                AND p.total_positions = s.total_positions
            THEN 'All metrics match'
            ELSE format('Shares: %s vs %s, Assets: %s vs %s, Market cap: %s vs %s, Positions: %s vs %s',
                p.total_shares, s.total_shares, p.total_assets, s.total_assets,
                p.total_market_cap, s.total_market_cap, p.total_positions, s.total_positions)
        END::TEXT
    FROM public_stats p, snapshot_stats s;

    -- Compare triple_term table
    RETURN QUERY
    WITH public_stats AS (
        SELECT
            COUNT(*) as cnt,
            SUM(total_assets) as total_assets,
            SUM(total_market_cap) as total_market_cap,
            SUM(total_position_count) as total_positions
        FROM public.triple_term
    ),
    snapshot_stats AS (
        SELECT
            COUNT(*) as cnt,
            SUM(total_assets) as total_assets,
            SUM(total_market_cap) as total_market_cap,
            SUM(total_position_count) as total_positions
        FROM snapshot.triple_term
    )
    SELECT
        'triple_term'::TEXT,
        p.cnt,
        s.cnt,
        p.cnt = s.cnt,
        CASE
            WHEN p.cnt = s.cnt AND p.total_assets = s.total_assets
                AND p.total_market_cap = s.total_market_cap AND p.total_positions = s.total_positions
            THEN 'All metrics match'
            ELSE format('Assets: %s vs %s, Market cap: %s vs %s, Positions: %s vs %s',
                p.total_assets, s.total_assets, p.total_market_cap, s.total_market_cap,
                p.total_positions, s.total_positions)
        END::TEXT
    FROM public_stats p, snapshot_stats s;

    -- Compare predicate_object table
    RETURN QUERY
    WITH public_stats AS (
        SELECT
            COUNT(*) as cnt,
            SUM(triple_count) as total_triples,
            SUM(total_position_count) as total_positions,
            SUM(total_market_cap) as total_market_cap
        FROM public.predicate_object
    ),
    snapshot_stats AS (
        SELECT
            COUNT(*) as cnt,
            SUM(triple_count) as total_triples,
            SUM(total_position_count) as total_positions,
            SUM(total_market_cap) as total_market_cap
        FROM snapshot.predicate_object
    )
    SELECT
        'predicate_object'::TEXT,
        p.cnt,
        s.cnt,
        p.cnt = s.cnt,
        CASE
            WHEN p.cnt = s.cnt AND p.total_triples = s.total_triples
                AND p.total_positions = s.total_positions AND p.total_market_cap = s.total_market_cap
            THEN 'All metrics match'
            ELSE format('Triples: %s vs %s, Positions: %s vs %s, Market cap: %s vs %s',
                p.total_triples, s.total_triples, p.total_positions, s.total_positions,
                p.total_market_cap, s.total_market_cap)
        END::TEXT
    FROM public_stats p, snapshot_stats s;

    -- Compare subject_predicate table
    RETURN QUERY
    WITH public_stats AS (
        SELECT
            COUNT(*) as cnt,
            SUM(triple_count) as total_triples,
            SUM(total_position_count) as total_positions,
            SUM(total_market_cap) as total_market_cap
        FROM public.subject_predicate
    ),
    snapshot_stats AS (
        SELECT
            COUNT(*) as cnt,
            SUM(triple_count) as total_triples,
            SUM(total_position_count) as total_positions,
            SUM(total_market_cap) as total_market_cap
        FROM snapshot.subject_predicate
    )
    SELECT
        'subject_predicate'::TEXT,
        p.cnt,
        s.cnt,
        p.cnt = s.cnt,
        CASE
            WHEN p.cnt = s.cnt AND p.total_triples = s.total_triples
                AND p.total_positions = s.total_positions AND p.total_market_cap = s.total_market_cap
            THEN 'All metrics match'
            ELSE format('Triples: %s vs %s, Positions: %s vs %s, Market cap: %s vs %s',
                p.total_triples, s.total_triples, p.total_positions, s.total_positions,
                p.total_market_cap, s.total_market_cap)
        END::TEXT
    FROM public_stats p, snapshot_stats s;

    RETURN;
END;
$$;

-- Add function documentation
COMMENT ON FUNCTION compare_schemas() IS
'Compares data between snapshot schema (materialized views) and public schema (trigger-based tables).
Returns a summary table showing row counts and aggregated metrics for each table.
Does not auto-refresh - call snapshot.refresh_all_views() first to ensure snapshot is current.
Compares only common fields, excluding ordering metadata fields.

Usage:
  -- 1. Refresh snapshot views first
  SELECT * FROM snapshot.refresh_all_views();

  -- 2. Run comparison
  SELECT * FROM compare_schemas();

Returns:
  - table_name: Name of the compared table
  - public_count: Row count in public schema
  - snapshot_count: Row count in snapshot schema
  - count_match: Boolean indicating if counts match
  - metric_diffs: Summary of differences in aggregated metrics';
