-- Migration: Create vault materialized view in snapshot schema
-- This view aggregates SharePriceChanged events and counts positions per vault
-- Part of the snapshot schema for validation against trigger-based public schema
--
-- Description:
-- The vault materialized view provides current state for each vault (term_id, curve_id combination).
-- It sources data from SharePriceChanged events and joins with the position view to count active positions.
--
-- Refresh:
-- SELECT snapshot.refresh_vault_view();

-- Create vault_type enum
DO $$ BEGIN
    CREATE TYPE vault_type AS ENUM ('Atom', 'Triple', 'CounterTriple');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Drop existing objects
DROP MATERIALIZED VIEW IF EXISTS snapshot.vault CASCADE;
DROP FUNCTION IF EXISTS snapshot.refresh_vault_view() CASCADE;

-- Create materialized view in snapshot schema
CREATE MATERIALIZED VIEW snapshot.vault AS
WITH
-- Get latest share price event for each vault
latest_share_price AS (
    SELECT DISTINCT ON (term_id, curve_id)
        term_id,
        CAST(curve_id AS numeric(78,0)) AS curve_id,
        CAST(share_price AS numeric(78,0)) AS current_share_price,
        CAST(total_assets AS numeric(78,0)) AS total_assets,
        CAST(total_shares AS numeric(78,0)) AS total_shares,
        CASE CAST(vault_type AS INTEGER)
            WHEN 0 THEN 'Atom'
            WHEN 1 THEN 'Triple'
            WHEN 2 THEN 'CounterTriple'
        END AS vault_type,
        block_number,
        log_index,
        transaction_hash,
        transaction_index,
        block_timestamp
    FROM public.share_price_changed_events
    ORDER BY term_id, curve_id, block_number DESC, log_index DESC
),

-- Count active positions per vault (only count positions with shares > 0)
position_counts AS (
    SELECT
        term_id,
        curve_id,
        COUNT(*) AS position_count
    FROM snapshot.position
    WHERE shares > 0
    GROUP BY term_id, curve_id
),

-- Get first event timestamp as created_at
first_event AS (
    SELECT
        term_id,
        CAST(curve_id AS numeric(78,0)) AS curve_id,
        MIN(block_timestamp) AS created_at
    FROM public.share_price_changed_events
    GROUP BY term_id, curve_id
)

-- Final vault view combining all CTEs
SELECT
    lsp.term_id,
    lsp.curve_id,
    lsp.total_shares,
    lsp.current_share_price,
    lsp.total_assets,
    -- Market cap calculation: (total_shares * current_share_price) / 1e18
    -- This represents the total value of all shares at current price
    CAST((lsp.total_shares * lsp.current_share_price / 1000000000000000000) AS NUMERIC(78, 0)) AS market_cap,
    COALESCE(pc.position_count, 0) AS position_count,
    lsp.vault_type::vault_type,
    CAST(lsp.block_number AS BIGINT) AS block_number,
    CAST(lsp.log_index AS BIGINT) AS log_index,
    CAST(lsp.transaction_hash AS TEXT) AS transaction_hash,
    CAST(lsp.transaction_index AS BIGINT) AS transaction_index,
    fe.created_at,
    lsp.block_timestamp AS updated_at
FROM latest_share_price lsp
LEFT JOIN position_counts pc
    ON lsp.term_id = pc.term_id
    AND lsp.curve_id = pc.curve_id
LEFT JOIN first_event fe
    ON lsp.term_id = fe.term_id
    AND lsp.curve_id = fe.curve_id;

-- Create unique index for CONCURRENT refresh capability
-- This index is required for REFRESH MATERIALIZED VIEW CONCURRENTLY
CREATE UNIQUE INDEX vault_pkey
    ON snapshot.vault (term_id, curve_id);

-- Additional indexes for query optimization
CREATE INDEX idx_vault_term_id
    ON snapshot.vault (term_id);

CREATE INDEX idx_vault_vault_type
    ON snapshot.vault (vault_type);

CREATE INDEX idx_vault_updated_at
    ON snapshot.vault (updated_at);

CREATE INDEX idx_vault_market_cap
    ON snapshot.vault (market_cap DESC);

CREATE INDEX idx_vault_position_count
    ON snapshot.vault (position_count DESC);

-- Create refresh function
-- This function can be called manually or scheduled via pg_cron
CREATE OR REPLACE FUNCTION snapshot.refresh_vault_view()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY snapshot.vault;
END;
$$;

-- Add comments for documentation
COMMENT ON MATERIALIZED VIEW snapshot.vault IS
'Aggregated vault data from SharePriceChanged events. Shows current state including total assets, total shares, share price, market cap, and position count per (term_id, curve_id). Part of snapshot schema for validation.';

COMMENT ON FUNCTION snapshot.refresh_vault_view() IS
'Refreshes the vault materialized view in snapshot schema using CONCURRENT mode. Can be called manually or scheduled via pg_cron for periodic updates.';

COMMENT ON COLUMN snapshot.vault.term_id IS
'Unique identifier for the term (atom or triple) as hex-encoded bytes32.';

COMMENT ON COLUMN snapshot.vault.curve_id IS
'Bonding curve identifier for the vault (typically 0 for standard curve).';

COMMENT ON COLUMN snapshot.vault.vault_type IS
'Type of vault: Atom, Triple (pro), CounterTriple (con).';

COMMENT ON COLUMN snapshot.vault.market_cap IS
'Calculated market capitalization: (total_shares * current_share_price) / 1e18. Represents total value of all shares.';

COMMENT ON COLUMN snapshot.vault.position_count IS
'Number of active positions (accounts with shares > 0) in this vault.';

COMMENT ON COLUMN snapshot.vault.created_at IS
'Timestamp when the vault was first created (first SharePriceChanged event).';

COMMENT ON COLUMN snapshot.vault.updated_at IS
'Timestamp of the most recent SharePriceChanged event for this vault.';
