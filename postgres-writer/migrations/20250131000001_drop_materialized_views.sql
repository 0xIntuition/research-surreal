-- Drop all materialized views in reverse dependency order
-- This must be run before creating the new regular tables

-- Level 4: Aggregate views (depend on triple_term)
DROP MATERIALIZED VIEW IF EXISTS predicate_object CASCADE;
DROP MATERIALIZED VIEW IF EXISTS subject_predicate CASCADE;

-- Level 3: Triple aggregates (depend on triple_vault)
DROP MATERIALIZED VIEW IF EXISTS triple_term CASCADE;

-- Level 2: Combined views (depend on base views)
DROP MATERIALIZED VIEW IF EXISTS triple_vault CASCADE;

-- Level 1: Base aggregation views (depend on event tables)
DROP MATERIALIZED VIEW IF EXISTS term CASCADE;
DROP MATERIALIZED VIEW IF EXISTS vault CASCADE;
DROP MATERIALIZED VIEW IF EXISTS position CASCADE;
DROP MATERIALIZED VIEW IF EXISTS triple CASCADE;
DROP MATERIALIZED VIEW IF EXISTS atom CASCADE;

-- Drop refresh utility functions
DROP FUNCTION IF EXISTS refresh_position_view() CASCADE;
DROP FUNCTION IF EXISTS refresh_vault_view() CASCADE;
DROP FUNCTION IF EXISTS refresh_term_view() CASCADE;
DROP FUNCTION IF EXISTS refresh_atom_view() CASCADE;
DROP FUNCTION IF EXISTS refresh_triple_view() CASCADE;
DROP FUNCTION IF EXISTS refresh_triple_vault_view() CASCADE;
DROP FUNCTION IF EXISTS refresh_triple_term_view() CASCADE;
DROP FUNCTION IF EXISTS refresh_predicate_object_view() CASCADE;
DROP FUNCTION IF EXISTS refresh_subject_predicate_view() CASCADE;
DROP FUNCTION IF EXISTS refresh_all_views() CASCADE;
