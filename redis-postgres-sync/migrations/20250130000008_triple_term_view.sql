-- Migration: Create triple_term materialized view
-- Description: Aggregates triple_vault data grouped by term_id across all curve_ids
--
-- Prerequisites:
-- This migration requires:
-- - 20250130000002_position_view.sql (position table)
-- - 20250130000003_vault_view.sql (vault materialized view)
-- - 20250130000006_triple_view.sql (triple materialized view)
-- - 20250130000007_triple_vault_view.sql (triple_vault materialized view)
--
-- Refresh:
-- SELECT refresh_triple_term_view();

-- 1. DROP EXISTING OBJECTS (for idempotency)
DROP MATERIALIZED VIEW IF EXISTS public.triple_term CASCADE;
DROP FUNCTION IF EXISTS refresh_triple_term_view() CASCADE;

-- 2. CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW public.triple_term AS
WITH
-- Aggregate triple_vault data by term_id (across all curve_ids)
aggregated_triple_vaults AS (
    SELECT
        term_id,
        counter_term_id,
        -- Sum all financial metrics across all curves
        SUM(total_assets) AS total_assets,
        SUM(market_cap) AS total_market_cap,
        SUM(position_count) AS total_position_count,
        -- Get most recent update timestamp
        MAX(updated_at) AS updated_at
    FROM public.triple_vault
    GROUP BY term_id, counter_term_id
)

-- Final triple_term view
SELECT
    term_id,
    counter_term_id,
    CAST(total_assets AS NUMERIC(78, 0)) AS total_assets,
    CAST(total_market_cap AS NUMERIC(78, 0)) AS total_market_cap,
    CAST(total_position_count AS BIGINT) AS total_position_count,
    updated_at
FROM aggregated_triple_vaults;

-- 3. CREATE INDEXES

-- Primary index (unique identifier) - required for CONCURRENT refresh
CREATE UNIQUE INDEX triple_term_pkey
    ON public.triple_term (term_id);

-- Counter triple index (for pro/con relationship queries)
CREATE INDEX idx_triple_term_counter_term_id
    ON public.triple_term (counter_term_id);

-- Temporal index (for time-based queries and recent updates)
CREATE INDEX idx_triple_term_updated_at
    ON public.triple_term (updated_at DESC);

-- Market cap index (for ranking and filtering by total market value)
CREATE INDEX idx_triple_term_total_market_cap
    ON public.triple_term (total_market_cap DESC);

-- Total assets index (for ranking by total assets)
CREATE INDEX idx_triple_term_total_assets
    ON public.triple_term (total_assets DESC);

-- Position count index (for ranking by engagement/popularity)
CREATE INDEX idx_triple_term_total_position_count
    ON public.triple_term (total_position_count DESC);

-- Composite index for most active triples (by market cap and recency)
CREATE INDEX idx_triple_term_market_cap_updated
    ON public.triple_term (total_market_cap DESC, updated_at DESC);

-- 4. CREATE REFRESH FUNCTION
-- This function can be called manually or scheduled via pg_cron
CREATE OR REPLACE FUNCTION refresh_triple_term_view()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.triple_term;
END;
$$;

-- 5. ADD COMMENTS FOR DOCUMENTATION

COMMENT ON MATERIALIZED VIEW public.triple_term IS
'Aggregated triple term data from triple_vault view. Shows combined totals across all curve_ids for each triple (including both pro and counter vault data). Updated via refresh_triple_term_view().';

COMMENT ON FUNCTION refresh_triple_term_view() IS
'Refreshes the triple_term materialized view using CONCURRENT mode. Can be called manually or scheduled via pg_cron for periodic updates.';

-- Triple identifier columns
COMMENT ON COLUMN public.triple_term.term_id IS
'Unique identifier for the triple (hex-encoded bytes32). This is the primary term ID for the triple (the "pro" vault).';

COMMENT ON COLUMN public.triple_term.counter_term_id IS
'Term ID of the counter-triple (the "con" or against position). Should be calculated using keccak256(abi.encodePacked(COUNTER_SALT, term_id)) in the application.';

-- Aggregated metric columns
COMMENT ON COLUMN public.triple_term.total_assets IS
'Sum of total_assets across all vaults (all curve_ids, both pro and counter) for this triple. Represents total assets locked in this triple.';

COMMENT ON COLUMN public.triple_term.total_market_cap IS
'Sum of market_cap across all vaults (all curve_ids, both pro and counter) for this triple. Represents total market capitalization and economic activity.';

COMMENT ON COLUMN public.triple_term.total_position_count IS
'Sum of position_count across all vaults (all curve_ids, both pro and counter) for this triple. Represents total number of positions/participants.';

-- Timestamp column
COMMENT ON COLUMN public.triple_term.updated_at IS
'Timestamp of the most recent update across any vault (any curve_id, either pro or counter) for this triple.';
