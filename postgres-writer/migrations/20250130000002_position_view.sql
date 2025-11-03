-- Migration: Create position materialized view in snapshot schema
-- This view aggregates deposited and redeemed events to track current positions
-- Part of the snapshot schema for validation against trigger-based public schema

-- Drop existing objects if they exist (for idempotency)
DROP MATERIALIZED VIEW IF EXISTS snapshot.position CASCADE;
DROP FUNCTION IF EXISTS snapshot.refresh_position_view() CASCADE;

-- Create the position materialized view in snapshot schema
CREATE MATERIALIZED VIEW snapshot.position AS
WITH
-- Get latest deposited event per position (most positions only have deposits)
-- This uses the optimized composite index for DISTINCT ON
latest_deposited AS (
    SELECT DISTINCT ON (receiver, term_id, curve_id)
        receiver AS account_id,
        term_id AS term_id_hex,
        CAST(curve_id AS numeric(78,0)) AS curve_id,
        CAST(total_shares AS numeric(78,0)) AS total_shares,
        block_number,
        log_index,
        block_timestamp,
        transaction_hash,
        transaction_index
    FROM public.deposited_events
    ORDER BY receiver, term_id, curve_id, block_number DESC, log_index DESC
),

-- Get latest redeemed event per position (very few positions have redemptions)
latest_redeemed AS (
    SELECT DISTINCT ON (sender, term_id, curve_id)
        sender AS account_id,
        term_id AS term_id_hex,
        CAST(curve_id AS numeric(78,0)) AS curve_id,
        CAST(total_shares AS numeric(78,0)) AS total_shares,
        block_number,
        log_index,
        block_timestamp,
        transaction_hash,
        transaction_index
    FROM public.redeemed_events
    ORDER BY sender, term_id, curve_id, block_number DESC, log_index DESC
),

-- Combine latest from both tables, taking the most recent event
-- This avoids UNION ALL of 3M+ rows by comparing only the latest events
latest_shares AS (
    SELECT
        COALESCE(d.account_id, r.account_id) AS account_id,
        COALESCE(d.term_id_hex, r.term_id_hex) AS term_id,
        COALESCE(d.curve_id, r.curve_id) AS curve_id,
        CASE
            -- If both exist, take the one with higher block_number or log_index
            WHEN d.block_number IS NOT NULL AND r.block_number IS NOT NULL THEN
                CASE
                    WHEN d.block_number > r.block_number THEN d.total_shares
                    WHEN d.block_number < r.block_number THEN r.total_shares
                    WHEN d.log_index > r.log_index THEN d.total_shares
                    ELSE r.total_shares
                END
            -- If only deposited exists
            WHEN d.block_number IS NOT NULL THEN d.total_shares
            -- If only redeemed exists
            ELSE r.total_shares
        END AS shares,
        CASE
            WHEN d.block_number IS NOT NULL AND r.block_number IS NOT NULL THEN
                CASE
                    WHEN d.block_number > r.block_number THEN d.block_timestamp
                    WHEN d.block_number < r.block_number THEN r.block_timestamp
                    WHEN d.log_index > r.log_index THEN d.block_timestamp
                    ELSE r.block_timestamp
                END
            WHEN d.block_number IS NOT NULL THEN d.block_timestamp
            ELSE r.block_timestamp
        END AS updated_at,
        CASE
            WHEN d.block_number IS NOT NULL AND r.block_number IS NOT NULL THEN
                CASE
                    WHEN d.block_number > r.block_number THEN d.block_number
                    WHEN d.block_number < r.block_number THEN r.block_number
                    WHEN d.log_index > r.log_index THEN d.block_number
                    ELSE r.block_number
                END
            WHEN d.block_number IS NOT NULL THEN d.block_number
            ELSE r.block_number
        END AS block_number,
        CASE
            WHEN d.block_number IS NOT NULL AND r.block_number IS NOT NULL THEN
                CASE
                    WHEN d.block_number > r.block_number THEN d.log_index
                    WHEN d.block_number < r.block_number THEN r.log_index
                    WHEN d.log_index > r.log_index THEN d.log_index
                    ELSE r.log_index
                END
            WHEN d.block_number IS NOT NULL THEN d.log_index
            ELSE r.log_index
        END AS log_index,
        CASE
            WHEN d.block_number IS NOT NULL AND r.block_number IS NOT NULL THEN
                CASE
                    WHEN d.block_number > r.block_number THEN d.transaction_hash
                    WHEN d.block_number < r.block_number THEN r.transaction_hash
                    WHEN d.log_index > r.log_index THEN d.transaction_hash
                    ELSE r.transaction_hash
                END
            WHEN d.block_number IS NOT NULL THEN d.transaction_hash
            ELSE r.transaction_hash
        END AS transaction_hash,
        CASE
            WHEN d.block_number IS NOT NULL AND r.block_number IS NOT NULL THEN
                CASE
                    WHEN d.block_number > r.block_number THEN d.transaction_index
                    WHEN d.block_number < r.block_number THEN r.transaction_index
                    WHEN d.log_index > r.log_index THEN d.transaction_index
                    ELSE r.transaction_index
                END
            WHEN d.block_number IS NOT NULL THEN d.transaction_index
            ELSE r.transaction_index
        END AS transaction_index
    FROM latest_deposited d
    FULL OUTER JOIN latest_redeemed r
        ON d.account_id = r.account_id
        AND d.term_id_hex = r.term_id_hex
        AND d.curve_id = r.curve_id
),

-- Aggregate total deposit assets after fees
-- Reuse latest_deposited to get unique positions, then aggregate all deposits
deposit_totals AS (
    SELECT
        ld.account_id,
        ld.term_id_hex AS term_id,
        ld.curve_id,
        COALESCE(SUM(CAST(d.assets_after_fees AS numeric(78,0))), 0) AS total_deposit_assets_after_total_fees,
        MIN(d.block_timestamp) AS created_at  -- Get first deposit timestamp here to avoid another scan
    FROM latest_deposited ld
    JOIN public.deposited_events d
        ON d.receiver = ld.account_id
        AND d.term_id = ld.term_id_hex
        AND CAST(d.curve_id AS numeric(78,0)) = ld.curve_id
    GROUP BY ld.account_id, ld.term_id_hex, ld.curve_id
),

-- Aggregate total redeem assets received
-- Only process positions that actually have redemptions (very small set)
redeem_totals AS (
    SELECT
        lr.account_id,
        lr.term_id_hex AS term_id,
        lr.curve_id,
        COALESCE(SUM(CAST(r.assets AS numeric(78,0))), 0) AS total_redeem_assets_for_receiver
    FROM latest_redeemed lr
    JOIN public.redeemed_events r
        ON r.sender = lr.account_id
        AND r.term_id = lr.term_id_hex
        AND CAST(r.curve_id AS numeric(78,0)) = lr.curve_id
    GROUP BY lr.account_id, lr.term_id_hex, lr.curve_id
)

-- Final join to create the position view
-- Note: deposit_totals now includes created_at, eliminating the need for first_deposit CTE
SELECT
    ls.account_id,
    ls.term_id,
    ls.curve_id,
    ls.shares,
    COALESCE(dt.total_deposit_assets_after_total_fees, 0) AS total_deposit_assets_after_total_fees,
    COALESCE(rt.total_redeem_assets_for_receiver, 0) AS total_redeem_assets_for_receiver,
    dt.created_at,  -- Now comes from deposit_totals
    ls.updated_at,
    CAST(ls.block_number AS BIGINT) AS block_number,
    CAST(ls.log_index AS BIGINT) AS log_index,
    CAST(ls.transaction_hash AS TEXT) AS transaction_hash,
    CAST(ls.transaction_index AS BIGINT) AS transaction_index
FROM latest_shares ls
LEFT JOIN deposit_totals dt
    ON ls.account_id = dt.account_id
    AND ls.term_id = dt.term_id
    AND ls.curve_id = dt.curve_id
LEFT JOIN redeem_totals rt
    ON ls.account_id = rt.account_id
    AND ls.term_id = rt.term_id
    AND ls.curve_id = rt.curve_id;

-- Create indexes for optimized queries
CREATE UNIQUE INDEX position_pkey
    ON snapshot.position (account_id, term_id, curve_id);

CREATE INDEX idx_position_account_id
    ON snapshot.position (account_id);

CREATE INDEX idx_position_term_id
    ON snapshot.position (term_id);

CREATE INDEX idx_position_updated_at
    ON snapshot.position (updated_at);

CREATE INDEX idx_position_created_at
    ON snapshot.position (created_at);

-- Partial indexes for active positions (shares > 0)
-- These are much smaller and faster for queries that only care about active positions
-- Expected size: 50-80% smaller than full indexes
CREATE INDEX idx_position_active_account
    ON snapshot.position (account_id)
    WHERE shares > 0;

CREATE INDEX idx_position_active_term
    ON snapshot.position (term_id)
    WHERE shares > 0;

CREATE INDEX idx_position_active_account_term
    ON snapshot.position (account_id, term_id)
    WHERE shares > 0;

-- Partial index for positions with significant deposits
-- Useful for filtering out dust positions or test transactions
CREATE INDEX idx_position_significant
    ON snapshot.position (account_id, term_id, curve_id)
    WHERE total_deposit_assets_after_total_fees > 1000000000000000000;  -- > 1 token (18 decimals)

-- Create refresh function for the materialized view
CREATE OR REPLACE FUNCTION snapshot.refresh_position_view()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- CONCURRENTLY allows queries during refresh (requires unique index)
    REFRESH MATERIALIZED VIEW CONCURRENTLY snapshot.position;
END;
$$;

COMMENT ON MATERIALIZED VIEW snapshot.position IS
'Aggregated position data from deposited and redeemed events. Shows current shares and cumulative deposit/redeem totals per (account_id, term_id, curve_id). Part of snapshot schema for validation.';

COMMENT ON FUNCTION snapshot.refresh_position_view() IS
'Refreshes the position materialized view in snapshot schema. Can be called manually or scheduled via pg_cron.';

-- Example usage for manual refresh:
-- SELECT snapshot.refresh_position_view();

-- Example usage for scheduled refresh (requires pg_cron extension):
-- SELECT cron.schedule('refresh-snapshot-position', '*/5 * * * *', 'SELECT snapshot.refresh_position_view();');
