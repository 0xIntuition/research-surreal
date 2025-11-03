-- Migration: Create term materialized view in snapshot schema
-- This view aggregates vault data grouped by term_id
-- Part of the snapshot schema for validation against trigger-based public schema
--
-- Description:
-- The term materialized view provides aggregated data for each term (atom or triple).
-- It sources data from the vault view and aggregates across all curve_ids for each term.
-- For Atom type, atom_id is set to term_id. For Triple/CounterTriple, triple_id is set to term_id.
--
-- Refresh:
-- SELECT snapshot.refresh_term_view();

-- Create term_type enum
DO $$ BEGIN
    CREATE TYPE term_type AS ENUM ('Atom', 'Triple', 'CounterTriple');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Drop existing objects
DROP MATERIALIZED VIEW IF EXISTS snapshot.term CASCADE;
DROP FUNCTION IF EXISTS snapshot.refresh_term_view() CASCADE;

-- Create materialized view in snapshot schema
CREATE MATERIALIZED VIEW snapshot.term AS
WITH
-- Aggregate vault data by term_id
aggregated_vaults AS (
    SELECT
        term_id,
        vault_type,
        SUM(total_assets) AS total_assets,
        SUM(market_cap) AS total_market_cap,
        MIN(created_at) AS created_at,
        MAX(updated_at) AS updated_at
    FROM snapshot.vault
    GROUP BY term_id, vault_type
)

-- Final term view
SELECT
    term_id AS id,
    CASE vault_type
        WHEN 'Atom' THEN 'Atom'::term_type
        WHEN 'Triple' THEN 'Triple'::term_type
        WHEN 'CounterTriple' THEN 'CounterTriple'::term_type
    END AS type,
    CASE WHEN vault_type = 'Atom' THEN term_id ELSE NULL END AS atom_id,
    CASE WHEN vault_type IN ('Triple', 'CounterTriple') THEN term_id ELSE NULL END AS triple_id,
    total_assets,
    total_market_cap,
    created_at,
    updated_at
FROM aggregated_vaults;

-- Create unique index for CONCURRENT refresh capability
-- This index is required for REFRESH MATERIALIZED VIEW CONCURRENTLY
CREATE UNIQUE INDEX term_pkey
    ON snapshot.term (id);

-- Additional indexes for query optimization
CREATE INDEX idx_term_type
    ON snapshot.term (type);

CREATE INDEX idx_term_updated_at
    ON snapshot.term (updated_at);

CREATE INDEX idx_term_total_market_cap
    ON snapshot.term (total_market_cap DESC);

CREATE INDEX idx_term_total_assets
    ON snapshot.term (total_assets DESC);

CREATE INDEX idx_term_atom_id
    ON snapshot.term (atom_id) WHERE atom_id IS NOT NULL;

CREATE INDEX idx_term_triple_id
    ON snapshot.term (triple_id) WHERE triple_id IS NOT NULL;

-- Create refresh function
-- This function can be called manually or scheduled via pg_cron
CREATE OR REPLACE FUNCTION snapshot.refresh_term_view()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY snapshot.term;
END;
$$;

-- Add comments for documentation
COMMENT ON MATERIALIZED VIEW snapshot.term IS
'Aggregated term data from vault view. Shows totals across all curve_ids for each term. Part of snapshot schema for validation.';

COMMENT ON FUNCTION snapshot.refresh_term_view() IS
'Refreshes the term materialized view in snapshot schema using CONCURRENT mode. Can be called manually or scheduled via pg_cron for periodic updates.';

COMMENT ON COLUMN snapshot.term.id IS
'Unique identifier for the term (atom or triple) as hex-encoded bytes32. This is the term_id from the vault.';

COMMENT ON COLUMN snapshot.term.type IS
'Type of term: Atom (individual entity), Triple (subject-predicate-object pro), or CounterTriple (con).';

COMMENT ON COLUMN snapshot.term.atom_id IS
'Set to term_id when type is Atom, NULL otherwise. Used for filtering and joining with atom-specific data.';

COMMENT ON COLUMN snapshot.term.triple_id IS
'Set to term_id when type is Triple or CounterTriple, NULL otherwise. Used for filtering and joining with triple-specific data.';

COMMENT ON COLUMN snapshot.term.total_assets IS
'Sum of total_assets across all vaults (all curve_ids) for this term.';

COMMENT ON COLUMN snapshot.term.total_market_cap IS
'Sum of market_cap across all vaults (all curve_ids) for this term. Represents total market capitalization.';

COMMENT ON COLUMN snapshot.term.created_at IS
'Timestamp when the term was first created (earliest created_at from any vault with this term_id).';

COMMENT ON COLUMN snapshot.term.updated_at IS
'Timestamp of the most recent update across any vault with this term_id.';
