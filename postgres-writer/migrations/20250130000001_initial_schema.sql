-- Initial schema for redis-postgres-sync
-- Creates all event tables with transaction information embedded

-- Atom Created Events
CREATE TABLE IF NOT EXISTS atom_created_events (
    transaction_hash TEXT NOT NULL,
    log_index BIGINT NOT NULL,
    atom_data TEXT,  -- Nullable: NULL when UTF-8 decode fails
    raw_data BYTEA,  -- Stores raw bytes when UTF-8 decode fails
    atom_wallet TEXT NOT NULL,
    creator TEXT NOT NULL,
    term_id TEXT NOT NULL,
    address TEXT NOT NULL,
    block_hash TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    network TEXT NOT NULL,
    transaction_index BIGINT NOT NULL,
    block_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_hash, log_index)
);

-- Deposited Events
CREATE TABLE IF NOT EXISTS deposited_events (
    transaction_hash TEXT NOT NULL,
    log_index BIGINT NOT NULL,
    assets TEXT NOT NULL,
    assets_after_fees TEXT NOT NULL,
    curve_id TEXT NOT NULL,
    receiver TEXT NOT NULL,
    sender TEXT NOT NULL,
    shares TEXT NOT NULL,
    term_id TEXT NOT NULL,
    total_shares TEXT NOT NULL,
    vault_type SMALLINT NOT NULL,
    address TEXT NOT NULL,
    block_hash TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    network TEXT NOT NULL,
    transaction_index BIGINT NOT NULL,
    block_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_hash, log_index)
);

-- Triple Created Events
CREATE TABLE IF NOT EXISTS triple_created_events (
    transaction_hash TEXT NOT NULL,
    log_index BIGINT NOT NULL,
    creator TEXT NOT NULL,
    object_id TEXT NOT NULL,
    predicate_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    term_id TEXT NOT NULL,
    counter_term_id TEXT NOT NULL,
    address TEXT NOT NULL,
    block_hash TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    network TEXT NOT NULL,
    transaction_index BIGINT NOT NULL,
    block_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_hash, log_index)
);

-- Redeemed Events
CREATE TABLE IF NOT EXISTS redeemed_events (
    transaction_hash TEXT NOT NULL,
    log_index BIGINT NOT NULL,
    assets TEXT NOT NULL,
    curve_id TEXT NOT NULL,
    fees TEXT NOT NULL,
    receiver TEXT NOT NULL,
    sender TEXT NOT NULL,
    shares TEXT NOT NULL,
    term_id TEXT NOT NULL,
    total_shares TEXT NOT NULL,
    vault_type SMALLINT NOT NULL,
    address TEXT NOT NULL,
    block_hash TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    network TEXT NOT NULL,
    transaction_index BIGINT NOT NULL,
    block_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_hash, log_index)
);

-- Share Price Changed Events
CREATE TABLE IF NOT EXISTS share_price_changed_events (
    transaction_hash TEXT NOT NULL,
    log_index BIGINT NOT NULL,
    term_id TEXT NOT NULL,
    curve_id TEXT NOT NULL,
    share_price TEXT NOT NULL,
    total_assets TEXT NOT NULL,
    total_shares TEXT NOT NULL,
    vault_type SMALLINT NOT NULL,
    address TEXT NOT NULL,
    block_hash TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    network TEXT NOT NULL,
    transaction_index BIGINT NOT NULL,
    block_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_hash, log_index)
);

-- Generic Events (fallback for unknown event types)
CREATE TABLE IF NOT EXISTS generic_events (
    transaction_hash TEXT NOT NULL,
    log_index BIGINT NOT NULL,
    event_name TEXT NOT NULL,
    event_data JSONB NOT NULL,
    address TEXT NOT NULL,
    block_hash TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    network TEXT NOT NULL,
    transaction_index BIGINT NOT NULL,
    block_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (transaction_hash, log_index)
);

-- Create indexes for commonly queried fields
CREATE INDEX IF NOT EXISTS idx_atom_created_term_id ON atom_created_events(term_id);
CREATE INDEX IF NOT EXISTS idx_atom_created_creator ON atom_created_events(creator);
CREATE INDEX IF NOT EXISTS idx_atom_created_block_number ON atom_created_events(block_number);
CREATE INDEX IF NOT EXISTS idx_atom_created_timestamp ON atom_created_events(block_timestamp);

CREATE INDEX IF NOT EXISTS idx_deposited_term_id ON deposited_events(term_id);
CREATE INDEX IF NOT EXISTS idx_deposited_receiver ON deposited_events(receiver);
CREATE INDEX IF NOT EXISTS idx_deposited_sender ON deposited_events(sender);
CREATE INDEX IF NOT EXISTS idx_deposited_block_number ON deposited_events(block_number);
CREATE INDEX IF NOT EXISTS idx_deposited_timestamp ON deposited_events(block_timestamp);

CREATE INDEX IF NOT EXISTS idx_triple_created_term_id ON triple_created_events(term_id);
CREATE INDEX IF NOT EXISTS idx_triple_created_counter_term_id ON triple_created_events(counter_term_id);
CREATE INDEX IF NOT EXISTS idx_triple_created_subject ON triple_created_events(subject_id);
CREATE INDEX IF NOT EXISTS idx_triple_created_predicate ON triple_created_events(predicate_id);
CREATE INDEX IF NOT EXISTS idx_triple_created_object ON triple_created_events(object_id);
CREATE INDEX IF NOT EXISTS idx_triple_created_block_number ON triple_created_events(block_number);
CREATE INDEX IF NOT EXISTS idx_triple_created_timestamp ON triple_created_events(block_timestamp);

CREATE INDEX IF NOT EXISTS idx_redeemed_term_id ON redeemed_events(term_id);
CREATE INDEX IF NOT EXISTS idx_redeemed_receiver ON redeemed_events(receiver);
CREATE INDEX IF NOT EXISTS idx_redeemed_sender ON redeemed_events(sender);
CREATE INDEX IF NOT EXISTS idx_redeemed_block_number ON redeemed_events(block_number);
CREATE INDEX IF NOT EXISTS idx_redeemed_timestamp ON redeemed_events(block_timestamp);

CREATE INDEX IF NOT EXISTS idx_share_price_term_id ON share_price_changed_events(term_id);
CREATE INDEX IF NOT EXISTS idx_share_price_block_number ON share_price_changed_events(block_number);
CREATE INDEX IF NOT EXISTS idx_share_price_timestamp ON share_price_changed_events(block_timestamp);

CREATE INDEX IF NOT EXISTS idx_generic_event_name ON generic_events(event_name);
CREATE INDEX IF NOT EXISTS idx_generic_block_number ON generic_events(block_number);
CREATE INDEX IF NOT EXISTS idx_generic_timestamp ON generic_events(block_timestamp);
