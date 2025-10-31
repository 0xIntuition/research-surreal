-- Create regular tables to replace materialized views
-- These tables will be updated by triggers and Rust application cascade logic

-- ====================
-- Level 1: Base Tables
-- ====================

-- atom: Direct mapping from atom_created_events
CREATE TABLE atom (
    term_id TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    creator_id TEXT NOT NULL,
    data TEXT,  -- Nullable: NULL when UTF-8 decode fails
    raw_data BYTEA,  -- Stores raw bytes when UTF-8 decode fails
    type TEXT,
    emoji TEXT,
    label TEXT,
    image TEXT,
    value_id TEXT,
    resolving_status TEXT NOT NULL DEFAULT 'Pending',
    -- Ordering fields for out-of-order event handling
    last_event_block BIGINT NOT NULL,
    last_event_log_index BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_atom_wallet_id ON atom(wallet_id);
CREATE INDEX idx_atom_creator_id ON atom(creator_id);
CREATE INDEX idx_atom_type ON atom(type) WHERE type IS NOT NULL;

-- triple: Direct mapping from triple_created_events
CREATE TABLE triple (
    term_id TEXT PRIMARY KEY,
    creator_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    predicate_id TEXT NOT NULL,
    object_id TEXT NOT NULL,
    counter_term_id TEXT NOT NULL,
    -- Ordering fields
    last_event_block BIGINT NOT NULL,
    last_event_log_index BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_triple_creator ON triple(creator_id);
CREATE INDEX idx_triple_subject ON triple(subject_id);
CREATE INDEX idx_triple_predicate ON triple(predicate_id);
CREATE INDEX idx_triple_object ON triple(object_id);
CREATE INDEX idx_triple_counter_term ON triple(counter_term_id);
CREATE INDEX idx_triple_subj_pred ON triple(subject_id, predicate_id);
CREATE INDEX idx_triple_pred_obj ON triple(predicate_id, object_id);

-- position: Current position state per (account_id, term_id, curve_id)
CREATE TABLE position (
    account_id TEXT NOT NULL,
    term_id TEXT NOT NULL,
    curve_id TEXT NOT NULL,
    shares NUMERIC(78, 0) NOT NULL DEFAULT 0,
    total_deposit_assets_after_total_fees NUMERIC(78, 0) NOT NULL DEFAULT 0,
    total_redeem_assets_for_receiver NUMERIC(78, 0) NOT NULL DEFAULT 0,
    -- Ordering fields - track last processed event for this position
    last_deposit_block BIGINT,
    last_deposit_log_index BIGINT,
    last_redeem_block BIGINT,
    last_redeem_log_index BIGINT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, term_id, curve_id)
);

CREATE INDEX idx_position_term ON position(term_id);
CREATE INDEX idx_position_curve ON position(curve_id);
CREATE INDEX idx_position_account ON position(account_id);
-- Partial index for active positions (most queries filter on this)
CREATE INDEX idx_position_active ON position(account_id, term_id, curve_id) WHERE shares > 0;
-- Partial index for significant positions
CREATE INDEX idx_position_significant ON position(account_id, term_id, curve_id) WHERE shares > 1000000000000000000;

-- ====================
-- Level 2: Aggregated Tables (managed by Rust)
-- ====================

-- vault: Current state per (term_id, curve_id)
CREATE TABLE vault (
    term_id TEXT NOT NULL,
    curve_id TEXT NOT NULL,
    total_shares NUMERIC(78, 0) NOT NULL DEFAULT 0,
    current_share_price NUMERIC(78, 0) NOT NULL DEFAULT 0,
    total_assets NUMERIC(78, 0) NOT NULL DEFAULT 0,
    market_cap NUMERIC(78, 0) NOT NULL DEFAULT 0,
    position_count BIGINT NOT NULL DEFAULT 0,
    vault_type TEXT NOT NULL,
    -- Ordering fields for share_price_changed events
    last_price_event_block BIGINT NOT NULL,
    last_price_event_log_index BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (term_id, curve_id)
);

CREATE INDEX idx_vault_term ON vault(term_id);
CREATE INDEX idx_vault_curve ON vault(curve_id);
CREATE INDEX idx_vault_type ON vault(vault_type);
CREATE INDEX idx_vault_market_cap ON vault(market_cap DESC);

-- term: Aggregated data per term across all curves
CREATE TABLE term (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    atom_id TEXT,
    triple_id TEXT,
    total_assets NUMERIC(78, 0) NOT NULL DEFAULT 0,
    total_market_cap NUMERIC(78, 0) NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_term_type ON term(type);
CREATE INDEX idx_term_atom ON term(atom_id) WHERE atom_id IS NOT NULL;
CREATE INDEX idx_term_triple ON term(triple_id) WHERE triple_id IS NOT NULL;
CREATE INDEX idx_term_market_cap ON term(total_market_cap DESC);

-- ====================
-- Level 3: Analytics Tables (eventual consistency)
-- ====================

-- triple_vault: Combined pro + counter vault metrics for triples
CREATE TABLE triple_vault (
    term_id TEXT NOT NULL,
    counter_term_id TEXT NOT NULL,
    curve_id TEXT NOT NULL,
    total_shares NUMERIC(78, 0) NOT NULL DEFAULT 0,
    total_assets NUMERIC(78, 0) NOT NULL DEFAULT 0,
    position_count BIGINT NOT NULL DEFAULT 0,
    market_cap NUMERIC(78, 0) NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (term_id, counter_term_id, curve_id)
);

CREATE INDEX idx_triple_vault_term ON triple_vault(term_id);
CREATE INDEX idx_triple_vault_counter ON triple_vault(counter_term_id);
CREATE INDEX idx_triple_vault_curve ON triple_vault(curve_id);

-- triple_term: Aggregated triple metrics across all curves
CREATE TABLE triple_term (
    term_id TEXT NOT NULL,
    counter_term_id TEXT NOT NULL,
    total_assets NUMERIC(78, 0) NOT NULL DEFAULT 0,
    total_market_cap NUMERIC(78, 0) NOT NULL DEFAULT 0,
    total_position_count BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (term_id, counter_term_id)
);

CREATE INDEX idx_triple_term_term ON triple_term(term_id);
CREATE INDEX idx_triple_term_counter ON triple_term(counter_term_id);

-- predicate_object: Aggregate by predicate-object pairs
CREATE TABLE predicate_object (
    predicate_id TEXT NOT NULL,
    object_id TEXT NOT NULL,
    triple_count BIGINT NOT NULL DEFAULT 0,
    total_position_count BIGINT NOT NULL DEFAULT 0,
    total_market_cap NUMERIC(78, 0) NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (predicate_id, object_id)
);

CREATE INDEX idx_predicate_object_pred ON predicate_object(predicate_id);
CREATE INDEX idx_predicate_object_obj ON predicate_object(object_id);
CREATE INDEX idx_predicate_object_market_cap ON predicate_object(total_market_cap DESC);

-- subject_predicate: Aggregate by subject-predicate pairs
CREATE TABLE subject_predicate (
    subject_id TEXT NOT NULL,
    predicate_id TEXT NOT NULL,
    triple_count BIGINT NOT NULL DEFAULT 0,
    total_position_count BIGINT NOT NULL DEFAULT 0,
    total_market_cap NUMERIC(78, 0) NOT NULL DEFAULT 0,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (subject_id, predicate_id)
);

CREATE INDEX idx_subject_predicate_subj ON subject_predicate(subject_id);
CREATE INDEX idx_subject_predicate_pred ON subject_predicate(predicate_id);
CREATE INDEX idx_subject_predicate_market_cap ON subject_predicate(total_market_cap DESC);
