-- Migration: Create triple materialized view in snapshot schema
-- Description: Transforms triple_created events into a triple view
-- Part of the snapshot schema for validation against trigger-based public schema
--
-- Note: counter_term_id is computed by the application using:
--   COUNTER_SALT = keccak256("COUNTER_SALT")
--   counter_term_id = keccak256(COUNTER_SALT + term_id)
-- Using alloy::primitives::keccak256 in Rust before insertion
--
-- Refresh:
-- SELECT snapshot.refresh_triple_view();

-- 1. DROP EXISTING OBJECTS (for idempotency)
DROP MATERIALIZED VIEW IF EXISTS snapshot.triple CASCADE;
DROP FUNCTION IF EXISTS snapshot.refresh_triple_view() CASCADE;

-- 2. CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW snapshot.triple AS
WITH triple_events AS (
    SELECT
        tc.term_id,
        tc.creator AS creator_id,
        tc.subject_id,
        tc.predicate_id,
        tc.object_id,
        tc.counter_term_id,
        CAST(tc.block_number AS NUMERIC(78, 0)) AS block_number,
        tc.block_timestamp AS created_at,
        tc.transaction_hash
    FROM public.triple_created_events tc
)
SELECT * FROM triple_events;

-- 3. CREATE INDEXES

-- Primary index (unique identifier)
CREATE UNIQUE INDEX triple_pkey ON snapshot.triple (term_id);

-- Creator index (for user queries - "show me all triples created by user X")
CREATE INDEX idx_triple_creator_id ON snapshot.triple (creator_id);

-- Atom reference indexes (for foreign key joins with atom table)
CREATE INDEX idx_triple_subject_id ON snapshot.triple (subject_id);
CREATE INDEX idx_triple_predicate_id ON snapshot.triple (predicate_id);
CREATE INDEX idx_triple_object_id ON snapshot.triple (object_id);

-- Counter triple index (for pro/con relationship queries)
-- Note: This index will be useful once counter_term_id is populated
CREATE INDEX idx_triple_counter_term_id ON snapshot.triple (counter_term_id);

-- Temporal indexes (for time-based queries and ordering)
CREATE INDEX idx_triple_created_at ON snapshot.triple (created_at);
CREATE INDEX idx_triple_block_number ON snapshot.triple (block_number);

-- Composite index (for subject-predicate relationship queries)
CREATE INDEX idx_triple_subject_predicate ON snapshot.triple (subject_id, predicate_id);

-- 4. CREATE REFRESH FUNCTION
CREATE OR REPLACE FUNCTION snapshot.refresh_triple_view()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY snapshot.triple;
END;
$$;

-- 5. ADD COMMENTS

COMMENT ON MATERIALIZED VIEW snapshot.triple IS 'Materialized view of triples created from triple_created events';
COMMENT ON FUNCTION snapshot.refresh_triple_view() IS 'Refreshes the triple materialized view concurrently';

COMMENT ON COLUMN snapshot.triple.term_id IS 'Unique identifier for the triple (hex-encoded bytes32). This is the "pro" (for) vault term ID.';
COMMENT ON COLUMN snapshot.triple.creator_id IS 'Ethereum address of the account that created the triple';
COMMENT ON COLUMN snapshot.triple.subject_id IS 'Term ID of the subject atom (the entity the statement is about)';
COMMENT ON COLUMN snapshot.triple.predicate_id IS 'Term ID of the predicate atom (the relationship or property being asserted)';
COMMENT ON COLUMN snapshot.triple.object_id IS 'Term ID of the object atom (the value or target of the relationship)';
COMMENT ON COLUMN snapshot.triple.counter_term_id IS 'Term ID of the counter-triple (the "con" or against position). Calculated using keccak256(abi.encodePacked(COUNTER_SALT, term_id)) where COUNTER_SALT = keccak256("COUNTER_SALT"). Each triple has two vaults: one for the statement (term_id) and one against it (counter_term_id). Computed by the application using alloy::primitives::keccak256.';
COMMENT ON COLUMN snapshot.triple.block_number IS 'Block number when the triple was created';
COMMENT ON COLUMN snapshot.triple.created_at IS 'Timestamp when the triple was created (from block timestamp)';
COMMENT ON COLUMN snapshot.triple.transaction_hash IS 'Transaction hash of the creation event';
