-- Migration: Create detailed row-level comparison functions
--
-- This migration adds functions to compare individual rows between the snapshot schema
-- and public schema. These are useful for debugging when compare_schemas() shows mismatches.
--
-- Usage:
--   SELECT * FROM compare_position_details();
--   SELECT * FROM compare_vault_details();
--   SELECT * FROM compare_term_details();
--   SELECT * FROM compare_atom_details();
--   SELECT * FROM compare_triple_details();

-- Drop existing functions if they exist
DROP FUNCTION IF EXISTS compare_position_details() CASCADE;
DROP FUNCTION IF EXISTS compare_vault_details() CASCADE;
DROP FUNCTION IF EXISTS compare_term_details() CASCADE;
DROP FUNCTION IF EXISTS compare_atom_details() CASCADE;
DROP FUNCTION IF EXISTS compare_triple_details() CASCADE;

-- Compare position table row by row
-- Note: snapshot.position.curve_id is NUMERIC, public.position.curve_id is TEXT
CREATE OR REPLACE FUNCTION compare_position_details()
RETURNS TABLE(
    account_id TEXT,
    term_id TEXT,
    curve_id TEXT,
    diff_type TEXT,
    public_shares NUMERIC,
    snapshot_shares NUMERIC,
    public_deposits NUMERIC,
    snapshot_deposits NUMERIC,
    public_redeems NUMERIC,
    snapshot_redeems NUMERIC
)
LANGUAGE SQL
AS $$
    SELECT
        COALESCE(p.account_id, s.account_id) AS account_id,
        COALESCE(p.term_id, s.term_id) AS term_id,
        COALESCE(p.curve_id, s.curve_id::TEXT) AS curve_id,
        CASE
            WHEN p.account_id IS NULL THEN 'MISSING_IN_PUBLIC'
            WHEN s.account_id IS NULL THEN 'MISSING_IN_SNAPSHOT'
            ELSE 'VALUE_MISMATCH'
        END AS diff_type,
        p.shares AS public_shares,
        s.shares AS snapshot_shares,
        p.total_deposit_assets_after_total_fees AS public_deposits,
        s.total_deposit_assets_after_total_fees AS snapshot_deposits,
        p.total_redeem_assets_for_receiver AS public_redeems,
        s.total_redeem_assets_for_receiver AS snapshot_redeems
    FROM public.position p
    FULL OUTER JOIN snapshot.position s
        ON p.account_id = s.account_id
        AND p.term_id = s.term_id
        AND p.curve_id = s.curve_id::TEXT
    WHERE
        p.account_id IS NULL
        OR s.account_id IS NULL
        OR p.shares IS DISTINCT FROM s.shares
        OR p.total_deposit_assets_after_total_fees IS DISTINCT FROM s.total_deposit_assets_after_total_fees
        OR p.total_redeem_assets_for_receiver IS DISTINCT FROM s.total_redeem_assets_for_receiver;
$$;

-- Compare vault table row by row
-- Note: snapshot.vault.curve_id is NUMERIC, public.vault.curve_id is TEXT
CREATE OR REPLACE FUNCTION compare_vault_details()
RETURNS TABLE(
    term_id TEXT,
    curve_id TEXT,
    diff_type TEXT,
    public_total_shares NUMERIC,
    snapshot_total_shares NUMERIC,
    public_total_assets NUMERIC,
    snapshot_total_assets NUMERIC,
    public_market_cap NUMERIC,
    snapshot_market_cap NUMERIC,
    public_position_count BIGINT,
    snapshot_position_count BIGINT
)
LANGUAGE SQL
AS $$
    SELECT
        COALESCE(p.term_id, s.term_id) AS term_id,
        COALESCE(p.curve_id, s.curve_id::TEXT) AS curve_id,
        CASE
            WHEN p.term_id IS NULL THEN 'MISSING_IN_PUBLIC'
            WHEN s.term_id IS NULL THEN 'MISSING_IN_SNAPSHOT'
            ELSE 'VALUE_MISMATCH'
        END AS diff_type,
        p.total_shares AS public_total_shares,
        s.total_shares AS snapshot_total_shares,
        p.total_assets AS public_total_assets,
        s.total_assets AS snapshot_total_assets,
        p.market_cap AS public_market_cap,
        s.market_cap AS snapshot_market_cap,
        p.position_count AS public_position_count,
        s.position_count AS snapshot_position_count
    FROM public.vault p
    FULL OUTER JOIN snapshot.vault s
        ON p.term_id = s.term_id
        AND p.curve_id = s.curve_id::TEXT
    WHERE
        p.term_id IS NULL
        OR s.term_id IS NULL
        OR p.total_shares IS DISTINCT FROM s.total_shares
        OR p.total_assets IS DISTINCT FROM s.total_assets
        OR p.market_cap IS DISTINCT FROM s.market_cap
        OR p.position_count IS DISTINCT FROM s.position_count;
$$;

-- Compare term table row by row
-- Note: Both public.term and snapshot.term use 'id' column
CREATE OR REPLACE FUNCTION compare_term_details()
RETURNS TABLE(
    term_id TEXT,
    diff_type TEXT,
    public_total_assets NUMERIC,
    snapshot_total_assets NUMERIC,
    public_market_cap NUMERIC,
    snapshot_market_cap NUMERIC
)
LANGUAGE SQL
AS $$
    SELECT
        COALESCE(p.id, s.id) AS term_id,
        CASE
            WHEN p.id IS NULL THEN 'MISSING_IN_PUBLIC'
            WHEN s.id IS NULL THEN 'MISSING_IN_SNAPSHOT'
            ELSE 'VALUE_MISMATCH'
        END AS diff_type,
        p.total_assets AS public_total_assets,
        s.total_assets AS snapshot_total_assets,
        p.total_market_cap AS public_market_cap,
        s.total_market_cap AS snapshot_market_cap
    FROM public.term p
    FULL OUTER JOIN snapshot.term s
        ON p.id = s.id
    WHERE
        p.id IS NULL
        OR s.id IS NULL
        OR p.total_assets IS DISTINCT FROM s.total_assets
        OR p.total_market_cap IS DISTINCT FROM s.total_market_cap;
$$;

-- Compare atom table row by row
CREATE OR REPLACE FUNCTION compare_atom_details()
RETURNS TABLE(
    term_id TEXT,
    diff_type TEXT,
    public_wallet_id TEXT,
    snapshot_wallet_id TEXT,
    public_creator_id TEXT,
    snapshot_creator_id TEXT,
    public_data TEXT,
    snapshot_data TEXT
)
LANGUAGE SQL
AS $$
    SELECT
        COALESCE(p.term_id, s.term_id) AS term_id,
        CASE
            WHEN p.term_id IS NULL THEN 'MISSING_IN_PUBLIC'
            WHEN s.term_id IS NULL THEN 'MISSING_IN_SNAPSHOT'
            ELSE 'VALUE_MISMATCH'
        END AS diff_type,
        p.wallet_id AS public_wallet_id,
        s.wallet_id AS snapshot_wallet_id,
        p.creator_id AS public_creator_id,
        s.creator_id AS snapshot_creator_id,
        p.data AS public_data,
        s.data AS snapshot_data
    FROM public.atom p
    FULL OUTER JOIN snapshot.atom s
        ON p.term_id = s.term_id
    WHERE
        p.term_id IS NULL
        OR s.term_id IS NULL
        OR p.wallet_id IS DISTINCT FROM s.wallet_id
        OR p.creator_id IS DISTINCT FROM s.creator_id
        OR p.data IS DISTINCT FROM s.data;
$$;

-- Compare triple table row by row
CREATE OR REPLACE FUNCTION compare_triple_details()
RETURNS TABLE(
    term_id TEXT,
    diff_type TEXT,
    public_subject_id TEXT,
    snapshot_subject_id TEXT,
    public_predicate_id TEXT,
    snapshot_predicate_id TEXT,
    public_object_id TEXT,
    snapshot_object_id TEXT,
    public_creator_id TEXT,
    snapshot_creator_id TEXT
)
LANGUAGE SQL
AS $$
    SELECT
        COALESCE(p.term_id, s.term_id) AS term_id,
        CASE
            WHEN p.term_id IS NULL THEN 'MISSING_IN_PUBLIC'
            WHEN s.term_id IS NULL THEN 'MISSING_IN_SNAPSHOT'
            ELSE 'VALUE_MISMATCH'
        END AS diff_type,
        p.subject_id AS public_subject_id,
        s.subject_id AS snapshot_subject_id,
        p.predicate_id AS public_predicate_id,
        s.predicate_id AS snapshot_predicate_id,
        p.object_id AS public_object_id,
        s.object_id AS snapshot_object_id,
        p.creator_id AS public_creator_id,
        s.creator_id AS snapshot_creator_id
    FROM public.triple p
    FULL OUTER JOIN snapshot.triple s
        ON p.term_id = s.term_id
    WHERE
        p.term_id IS NULL
        OR s.term_id IS NULL
        OR p.subject_id IS DISTINCT FROM s.subject_id
        OR p.predicate_id IS DISTINCT FROM s.predicate_id
        OR p.object_id IS DISTINCT FROM s.object_id
        OR p.creator_id IS DISTINCT FROM s.creator_id;
$$;

-- Add function documentation
COMMENT ON FUNCTION compare_position_details() IS
'Returns row-by-row differences between public.position and snapshot.position tables.
Shows missing rows and value mismatches in key fields.
Returns empty if schemas match perfectly.

Usage:
  SELECT * FROM compare_position_details() LIMIT 10;';

COMMENT ON FUNCTION compare_vault_details() IS
'Returns row-by-row differences between public.vault and snapshot.vault tables.
Shows missing rows and value mismatches in aggregated metrics.
Returns empty if schemas match perfectly.

Usage:
  SELECT * FROM compare_vault_details() LIMIT 10;';

COMMENT ON FUNCTION compare_term_details() IS
'Returns row-by-row differences between public.term and snapshot.term tables.
Shows missing rows and value mismatches in aggregated metrics.
Returns empty if schemas match perfectly.

Usage:
  SELECT * FROM compare_term_details() LIMIT 10;';

COMMENT ON FUNCTION compare_atom_details() IS
'Returns row-by-row differences between public.atom and snapshot.atom tables.
Shows missing rows and value mismatches in atom metadata.
Returns empty if schemas match perfectly.

Usage:
  SELECT * FROM compare_atom_details() LIMIT 10;';

COMMENT ON FUNCTION compare_triple_details() IS
'Returns row-by-row differences between public.triple and snapshot.triple tables.
Shows missing rows and value mismatches in triple components.
Returns empty if schemas match perfectly.

Usage:
  SELECT * FROM compare_triple_details() LIMIT 10;';
