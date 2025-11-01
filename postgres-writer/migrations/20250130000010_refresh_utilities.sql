-- Refresh All Views Function
--
-- This migration creates a utility function to refresh all materialized views
-- in the correct dependency order and track the execution time for each view.
--
-- Usage:
--   SELECT * FROM refresh_all_views();
--
-- Returns a table with columns:
--   - view_name: Name of the materialized view
--   - duration_seconds: Time taken to refresh the view (in seconds)
--   - status: 'success' or 'error'
--   - error_message: Error details if refresh failed, NULL otherwise
--
-- The function refreshes views in dependency order:
--   1. Base views: position, atom, triple
--   2. First-level aggregates: vault, triple_vault
--   3. Second-level aggregates: term, triple_term
--   4. Third-level aggregates: predicate_object, subject_predicate
--
-- Uses CONCURRENT refresh mode to avoid locking views during refresh.
-- Continues refreshing remaining views even if individual refreshes fail.

-- Drop existing function if it exists
DROP FUNCTION IF EXISTS refresh_all_views() CASCADE;

-- Create the refresh function
CREATE OR REPLACE FUNCTION refresh_all_views()
RETURNS TABLE(
    view_name TEXT,
    duration_seconds NUMERIC,
    status TEXT,
    error_message TEXT
)
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration NUMERIC;
BEGIN
    -- Refresh position (base view)
    BEGIN
        start_time := clock_timestamp();
        REFRESH MATERIALIZED VIEW CONCURRENTLY public.position;
        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));

        view_name := 'position';
        duration_seconds := duration;
        status := 'success';
        error_message := NULL;
        RETURN NEXT;
    EXCEPTION WHEN OTHERS THEN
        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));

        view_name := 'position';
        duration_seconds := duration;
        status := 'error';
        error_message := SQLERRM;
        RETURN NEXT;
    END;

    -- Refresh atom (base view)
    BEGIN
        start_time := clock_timestamp();
        REFRESH MATERIALIZED VIEW CONCURRENTLY public.atom;
        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));

        view_name := 'atom';
        duration_seconds := duration;
        status := 'success';
        error_message := NULL;
        RETURN NEXT;
    EXCEPTION WHEN OTHERS THEN
        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));

        view_name := 'atom';
        duration_seconds := duration;
        status := 'error';
        error_message := SQLERRM;
        RETURN NEXT;
    END;

    -- Refresh triple (base view)
    BEGIN
        start_time := clock_timestamp();
        REFRESH MATERIALIZED VIEW CONCURRENTLY public.triple;
        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));

        view_name := 'triple';
        duration_seconds := duration;
        status := 'success';
        error_message := NULL;
        RETURN NEXT;
    EXCEPTION WHEN OTHERS THEN
        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));

        view_name := 'triple';
        duration_seconds := duration;
        status := 'error';
        error_message := SQLERRM;
        RETURN NEXT;
    END;

    -- Refresh vault (depends on position)
    BEGIN
        start_time := clock_timestamp();
        REFRESH MATERIALIZED VIEW CONCURRENTLY public.vault;
        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));

        view_name := 'vault';
        duration_seconds := duration;
        status := 'success';
        error_message := NULL;
        RETURN NEXT;
    EXCEPTION WHEN OTHERS THEN
        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));

        view_name := 'vault';
        duration_seconds := duration;
        status := 'error';
        error_message := SQLERRM;
        RETURN NEXT;
    END;

    -- Refresh triple_vault (depends on triple/atom)
    BEGIN
        start_time := clock_timestamp();
        REFRESH MATERIALIZED VIEW CONCURRENTLY public.triple_vault;
        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));

        view_name := 'triple_vault';
        duration_seconds := duration;
        status := 'success';
        error_message := NULL;
        RETURN NEXT;
    EXCEPTION WHEN OTHERS THEN
        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));

        view_name := 'triple_vault';
        duration_seconds := duration;
        status := 'error';
        error_message := SQLERRM;
        RETURN NEXT;
    END;

    -- Refresh term (depends on vault)
    BEGIN
        start_time := clock_timestamp();
        REFRESH MATERIALIZED VIEW CONCURRENTLY public.term;
        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));

        view_name := 'term';
        duration_seconds := duration;
        status := 'success';
        error_message := NULL;
        RETURN NEXT;
    EXCEPTION WHEN OTHERS THEN
        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));

        view_name := 'term';
        duration_seconds := duration;
        status := 'error';
        error_message := SQLERRM;
        RETURN NEXT;
    END;

    -- Refresh triple_term (depends on triple_vault)
    BEGIN
        start_time := clock_timestamp();
        REFRESH MATERIALIZED VIEW CONCURRENTLY public.triple_term;
        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));

        view_name := 'triple_term';
        duration_seconds := duration;
        status := 'success';
        error_message := NULL;
        RETURN NEXT;
    EXCEPTION WHEN OTHERS THEN
        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));

        view_name := 'triple_term';
        duration_seconds := duration;
        status := 'error';
        error_message := SQLERRM;
        RETURN NEXT;
    END;

    -- Refresh predicate_object (depends on triple and triple_term)
    BEGIN
        start_time := clock_timestamp();
        REFRESH MATERIALIZED VIEW CONCURRENTLY public.predicate_object;
        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));

        view_name := 'predicate_object';
        duration_seconds := duration;
        status := 'success';
        error_message := NULL;
        RETURN NEXT;
    EXCEPTION WHEN OTHERS THEN
        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));

        view_name := 'predicate_object';
        duration_seconds := duration;
        status := 'error';
        error_message := SQLERRM;
        RETURN NEXT;
    END;

    -- Refresh subject_predicate (depends on triple and triple_term)
    BEGIN
        start_time := clock_timestamp();
        REFRESH MATERIALIZED VIEW CONCURRENTLY public.subject_predicate;
        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));

        view_name := 'subject_predicate';
        duration_seconds := duration;
        status := 'success';
        error_message := NULL;
        RETURN NEXT;
    EXCEPTION WHEN OTHERS THEN
        end_time := clock_timestamp();
        duration := EXTRACT(EPOCH FROM (end_time - start_time));

        view_name := 'subject_predicate';
        duration_seconds := duration;
        status := 'error';
        error_message := SQLERRM;
        RETURN NEXT;
    END;

    RETURN;
END;
$$;

-- Add function documentation
COMMENT ON FUNCTION refresh_all_views() IS
'Refreshes all materialized views in dependency order and returns timing information.
Refreshes views using CONCURRENT mode to avoid locking.
Returns a table with view_name, duration_seconds, status, and error_message columns.
Continues processing remaining views even if individual refreshes fail.

Example usage:
  SELECT * FROM refresh_all_views();

Can be scheduled with pg_cron for periodic refreshes:
  SELECT cron.schedule(''refresh-all-views'', ''0 * * * *'', ''SELECT refresh_all_views();'');';
