-- Migration: Create predicate_object and subject_predicate materialized views
-- Description: Aggregates triple data grouped by predicate-object and subject-predicate pairs
--
-- Prerequisites:
-- This migration requires:
-- - 20250130000002_position_view.sql (position table)
-- - 20250130000003_vault_view.sql (vault materialized view)
-- - 20250130000006_triple_view.sql (triple materialized view)
-- - 20250130000008_triple_term_view.sql (triple_term materialized view)
--
-- Refresh:
-- SELECT refresh_predicate_object_view();
-- SELECT refresh_subject_predicate_view();

-- 1. DROP EXISTING OBJECTS (for idempotency)
DROP MATERIALIZED VIEW IF EXISTS public.predicate_object CASCADE;
DROP MATERIALIZED VIEW IF EXISTS public.subject_predicate CASCADE;
DROP FUNCTION IF EXISTS refresh_predicate_object_view() CASCADE;
DROP FUNCTION IF EXISTS refresh_subject_predicate_view() CASCADE;

-- 2. CREATE MATERIALIZED VIEW: predicate_object
-- Aggregates triple data grouped by predicate_id and object_id
CREATE MATERIALIZED VIEW public.predicate_object AS
SELECT
    t.predicate_id,
    t.object_id,
    COUNT(DISTINCT t.term_id)::INTEGER AS triple_count,
    COALESCE(SUM(tt.total_position_count), 0)::INTEGER AS total_position_count,
    COALESCE(SUM(tt.total_market_cap), 0) AS total_market_cap
FROM triple t
LEFT JOIN triple_term tt ON tt.term_id = t.term_id
GROUP BY t.predicate_id, t.object_id;

-- 3. CREATE INDEXES FOR predicate_object

-- Primary index (unique identifier) - required for CONCURRENT refresh
CREATE UNIQUE INDEX idx_predicate_object_unique
    ON public.predicate_object (predicate_id, object_id);

-- Predicate index (for filtering by predicate)
CREATE INDEX idx_predicate_object_predicate_id
    ON public.predicate_object (predicate_id);

-- Object index (for filtering by object)
CREATE INDEX idx_predicate_object_object_id
    ON public.predicate_object (object_id);

-- Triple count index (for ranking by number of triples)
CREATE INDEX idx_predicate_object_triple_count
    ON public.predicate_object (triple_count DESC);

-- Market cap index (for ranking by total market value)
CREATE INDEX idx_predicate_object_total_market_cap
    ON public.predicate_object (total_market_cap DESC);

-- Position count index (for ranking by engagement/popularity)
CREATE INDEX idx_predicate_object_total_position_count
    ON public.predicate_object (total_position_count DESC);

-- 4. CREATE MATERIALIZED VIEW: subject_predicate
-- Aggregates triple data grouped by subject_id and predicate_id
CREATE MATERIALIZED VIEW public.subject_predicate AS
SELECT
    t.subject_id,
    t.predicate_id,
    COUNT(DISTINCT t.term_id)::INTEGER AS triple_count,
    COALESCE(SUM(tt.total_position_count), 0)::INTEGER AS total_position_count,
    COALESCE(SUM(tt.total_market_cap), 0) AS total_market_cap
FROM triple t
LEFT JOIN triple_term tt ON tt.term_id = t.term_id
GROUP BY t.subject_id, t.predicate_id;

-- 5. CREATE INDEXES FOR subject_predicate

-- Primary index (unique identifier) - required for CONCURRENT refresh
CREATE UNIQUE INDEX idx_subject_predicate_unique
    ON public.subject_predicate (subject_id, predicate_id);

-- Subject index (for filtering by subject)
CREATE INDEX idx_subject_predicate_subject_id
    ON public.subject_predicate (subject_id);

-- Predicate index (for filtering by predicate)
CREATE INDEX idx_subject_predicate_predicate_id
    ON public.subject_predicate (predicate_id);

-- Triple count index (for ranking by number of triples)
CREATE INDEX idx_subject_predicate_triple_count
    ON public.subject_predicate (triple_count DESC);

-- Market cap index (for ranking by total market value)
CREATE INDEX idx_subject_predicate_total_market_cap
    ON public.subject_predicate (total_market_cap DESC);

-- Position count index (for ranking by engagement/popularity)
CREATE INDEX idx_subject_predicate_total_position_count
    ON public.subject_predicate (total_position_count DESC);

-- 6. CREATE REFRESH FUNCTIONS
-- These functions can be called manually or scheduled via pg_cron

CREATE OR REPLACE FUNCTION refresh_predicate_object_view()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.predicate_object;
END;
$$;

CREATE OR REPLACE FUNCTION refresh_subject_predicate_view()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY public.subject_predicate;
END;
$$;

-- 7. ADD COMMENTS FOR DOCUMENTATION

COMMENT ON MATERIALIZED VIEW public.predicate_object IS
'Aggregated triple data grouped by predicate_id and object_id. Shows the count of unique triples, total position count, and total market cap for each predicate-object pair. Updated via refresh_predicate_object_view().';

COMMENT ON FUNCTION refresh_predicate_object_view() IS
'Refreshes the predicate_object materialized view using CONCURRENT mode. Can be called manually or scheduled via pg_cron for periodic updates.';

-- predicate_object columns
COMMENT ON COLUMN public.predicate_object.predicate_id IS
'Identifier for the predicate (relationship type) in the triple. Used to group triples by their predicate.';

COMMENT ON COLUMN public.predicate_object.object_id IS
'Identifier for the object (target entity) in the triple. Used to group triples by their object.';

COMMENT ON COLUMN public.predicate_object.triple_count IS
'Number of distinct triples that have this predicate-object combination. Represents how many unique relationships of this type exist.';

COMMENT ON COLUMN public.predicate_object.total_position_count IS
'Sum of position counts across all triples with this predicate-object pair. Aggregated from triple_term table. Represents total engagement.';

COMMENT ON COLUMN public.predicate_object.total_market_cap IS
'Sum of market capitalization across all triples with this predicate-object pair. Aggregated from triple_term table. Represents total economic activity.';

COMMENT ON MATERIALIZED VIEW public.subject_predicate IS
'Aggregated triple data grouped by subject_id and predicate_id. Shows the count of unique triples, total position count, and total market cap for each subject-predicate pair. Updated via refresh_subject_predicate_view().';

COMMENT ON FUNCTION refresh_subject_predicate_view() IS
'Refreshes the subject_predicate materialized view using CONCURRENT mode. Can be called manually or scheduled via pg_cron for periodic updates.';

-- subject_predicate columns
COMMENT ON COLUMN public.subject_predicate.subject_id IS
'Identifier for the subject (source entity) in the triple. Used to group triples by their subject.';

COMMENT ON COLUMN public.subject_predicate.predicate_id IS
'Identifier for the predicate (relationship type) in the triple. Used to group triples by their predicate.';

COMMENT ON COLUMN public.subject_predicate.triple_count IS
'Number of distinct triples that have this subject-predicate combination. Represents how many unique relationships of this type this subject has.';

COMMENT ON COLUMN public.subject_predicate.total_position_count IS
'Sum of position counts across all triples with this subject-predicate pair. Aggregated from triple_term table. Represents total engagement.';

COMMENT ON COLUMN public.subject_predicate.total_market_cap IS
'Sum of market capitalization across all triples with this subject-predicate pair. Aggregated from triple_term table. Represents total economic activity.';
