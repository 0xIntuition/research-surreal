-- Create trigger functions and triggers for incremental table updates
-- Triggers handle simple updates; Rust application handles complex cascading aggregations

-- ====================
-- Helper Functions
-- ====================

-- Compare (block, log_index) tuples to determine which event is newer
CREATE OR REPLACE FUNCTION is_event_newer(
    new_block BIGINT,
    new_log_index BIGINT,
    old_block BIGINT,
    old_log_index BIGINT
) RETURNS BOOLEAN AS $$
BEGIN
    IF old_block IS NULL THEN
        RETURN TRUE;
    END IF;
    RETURN (new_block > old_block) OR (new_block = old_block AND new_log_index > old_log_index);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ====================
-- Atom Triggers
-- ====================

CREATE OR REPLACE FUNCTION update_atom_table()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO atom (
        term_id,
        wallet_id,
        creator_id,
        data,
        raw_data,
        type,
        emoji,
        label,
        image,
        value_id,
        resolving_status,
        last_event_block,
        last_event_log_index,
        created_at,
        updated_at
    ) VALUES (
        NEW.term_id,
        NEW.atom_wallet,
        NEW.creator,
        NEW.atom_data,  -- Can be NULL if UTF-8 decode failed
        NEW.raw_data,   -- Will contain raw bytes if atom_data is NULL
        NULL, -- type will be enriched later
        NULL, -- emoji will be enriched later
        NULL, -- label will be enriched later
        NULL, -- image will be enriched later
        NULL, -- value_id will be enriched later
        'Pending',
        NEW.block_number,
        NEW.log_index,
        NEW.block_timestamp,
        NEW.block_timestamp
    )
    ON CONFLICT (term_id) DO UPDATE SET
        wallet_id = EXCLUDED.wallet_id,
        creator_id = EXCLUDED.creator_id,
        data = EXCLUDED.data,
        raw_data = EXCLUDED.raw_data,
        last_event_block = EXCLUDED.last_event_block,
        last_event_log_index = EXCLUDED.last_event_log_index,
        updated_at = EXCLUDED.updated_at
    WHERE is_event_newer(
        EXCLUDED.last_event_block,
        EXCLUDED.last_event_log_index,
        atom.last_event_block,
        atom.last_event_log_index
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_atom
    AFTER INSERT ON atom_created_events
    FOR EACH ROW
    EXECUTE FUNCTION update_atom_table();

-- ====================
-- Triple Triggers
-- ====================

CREATE OR REPLACE FUNCTION update_triple_table()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO triple (
        term_id,
        creator_id,
        subject_id,
        predicate_id,
        object_id,
        counter_term_id,
        last_event_block,
        last_event_log_index,
        created_at,
        updated_at
    ) VALUES (
        NEW.term_id,
        NEW.creator,
        NEW.subject_id,
        NEW.predicate_id,
        NEW.object_id,
        NEW.counter_term_id,
        NEW.block_number,
        NEW.log_index,
        NEW.block_timestamp,
        NEW.block_timestamp
    )
    ON CONFLICT (term_id) DO UPDATE SET
        creator_id = EXCLUDED.creator_id,
        subject_id = EXCLUDED.subject_id,
        predicate_id = EXCLUDED.predicate_id,
        object_id = EXCLUDED.object_id,
        counter_term_id = EXCLUDED.counter_term_id,
        last_event_block = EXCLUDED.last_event_block,
        last_event_log_index = EXCLUDED.last_event_log_index,
        updated_at = EXCLUDED.updated_at
    WHERE is_event_newer(
        EXCLUDED.last_event_block,
        EXCLUDED.last_event_log_index,
        triple.last_event_block,
        triple.last_event_log_index
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_triple
    AFTER INSERT ON triple_created_events
    FOR EACH ROW
    EXECUTE FUNCTION update_triple_table();

-- ====================
-- Position Triggers (Deposits)
-- ====================

CREATE OR REPLACE FUNCTION update_position_on_deposit()
RETURNS TRIGGER AS $$
DECLARE
    current_shares NUMERIC(78, 0);
    current_deposit_total NUMERIC(78, 0);
    current_deposit_block BIGINT;
    current_deposit_log_index BIGINT;
BEGIN
    -- Get current position state
    SELECT
        shares,
        total_deposit_assets_after_total_fees,
        last_deposit_block,
        last_deposit_log_index
    INTO
        current_shares,
        current_deposit_total,
        current_deposit_block,
        current_deposit_log_index
    FROM position
    WHERE
        account_id = NEW.receiver
        AND term_id = NEW.term_id
        AND curve_id = NEW.curve_id;

    -- If position doesn't exist or this event is newer, update
    IF NOT FOUND THEN
        -- Insert new position
        INSERT INTO position (
            account_id,
            term_id,
            curve_id,
            shares,
            total_deposit_assets_after_total_fees,
            total_redeem_assets_for_receiver,
            last_deposit_block,
            last_deposit_log_index,
            created_at,
            updated_at
        ) VALUES (
            NEW.receiver,
            NEW.term_id,
            NEW.curve_id,
            NEW.total_shares::NUMERIC(78, 0),
            NEW.assets_after_fees::NUMERIC(78, 0),
            0,
            NEW.block_number,
            NEW.log_index,
            NEW.block_timestamp,
            NEW.block_timestamp
        );
    ELSIF is_event_newer(NEW.block_number, NEW.log_index, current_deposit_block, current_deposit_log_index) THEN
        -- Update existing position with newer deposit event
        UPDATE position SET
            shares = NEW.total_shares::NUMERIC(78, 0),
            total_deposit_assets_after_total_fees = current_deposit_total + NEW.assets_after_fees::NUMERIC(78, 0),
            last_deposit_block = NEW.block_number,
            last_deposit_log_index = NEW.log_index,
            updated_at = NEW.block_timestamp
        WHERE
            account_id = NEW.receiver
            AND term_id = NEW.term_id
            AND curve_id = NEW.curve_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_position_deposit
    AFTER INSERT ON deposited_events
    FOR EACH ROW
    EXECUTE FUNCTION update_position_on_deposit();

-- ====================
-- Position Triggers (Redemptions)
-- ====================

CREATE OR REPLACE FUNCTION update_position_on_redeem()
RETURNS TRIGGER AS $$
DECLARE
    current_shares NUMERIC(78, 0);
    current_redeem_total NUMERIC(78, 0);
    current_redeem_block BIGINT;
    current_redeem_log_index BIGINT;
BEGIN
    -- Get current position state
    SELECT
        shares,
        total_redeem_assets_for_receiver,
        last_redeem_block,
        last_redeem_log_index
    INTO
        current_shares,
        current_redeem_total,
        current_redeem_block,
        current_redeem_log_index
    FROM position
    WHERE
        account_id = NEW.receiver
        AND term_id = NEW.term_id
        AND curve_id = NEW.curve_id;

    -- If position doesn't exist or this event is newer, update
    IF NOT FOUND THEN
        -- Insert new position (redeem before any deposit - edge case)
        INSERT INTO position (
            account_id,
            term_id,
            curve_id,
            shares,
            total_deposit_assets_after_total_fees,
            total_redeem_assets_for_receiver,
            last_redeem_block,
            last_redeem_log_index,
            created_at,
            updated_at
        ) VALUES (
            NEW.receiver,
            NEW.term_id,
            NEW.curve_id,
            NEW.total_shares::NUMERIC(78, 0),
            0,
            NEW.assets::NUMERIC(78, 0),
            NEW.block_number,
            NEW.log_index,
            NEW.block_timestamp,
            NEW.block_timestamp
        );
    ELSIF is_event_newer(NEW.block_number, NEW.log_index, current_redeem_block, current_redeem_log_index) THEN
        -- Update existing position with newer redeem event
        UPDATE position SET
            shares = NEW.total_shares::NUMERIC(78, 0),
            total_redeem_assets_for_receiver = current_redeem_total + NEW.assets::NUMERIC(78, 0),
            last_redeem_block = NEW.block_number,
            last_redeem_log_index = NEW.log_index,
            updated_at = NEW.block_timestamp
        WHERE
            account_id = NEW.receiver
            AND term_id = NEW.term_id
            AND curve_id = NEW.curve_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_position_redeem
    AFTER INSERT ON redeemed_events
    FOR EACH ROW
    EXECUTE FUNCTION update_position_on_redeem();

-- ====================
-- Vault Triggers (Share Price Changes)
-- ====================

CREATE OR REPLACE FUNCTION update_vault_on_price_change()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO vault (
        term_id,
        curve_id,
        total_shares,
        current_share_price,
        total_assets,
        market_cap,
        position_count,
        vault_type,
        last_price_event_block,
        last_price_event_log_index,
        created_at,
        updated_at
    ) VALUES (
        NEW.term_id,
        NEW.curve_id,
        NEW.total_shares::NUMERIC(78, 0),
        NEW.share_price::NUMERIC(78, 0),
        NEW.total_assets::NUMERIC(78, 0),
        -- Calculate market cap: (total_shares * share_price) / 1e18
        (NEW.total_shares::NUMERIC(78, 0) * NEW.share_price::NUMERIC(78, 0)) / 1000000000000000000,
        0, -- position_count will be updated by Rust cascade
        NEW.vault_type,
        NEW.block_number,
        NEW.log_index,
        NEW.block_timestamp,
        NEW.block_timestamp
    )
    ON CONFLICT (term_id, curve_id) DO UPDATE SET
        total_shares = EXCLUDED.total_shares,
        current_share_price = EXCLUDED.current_share_price,
        total_assets = EXCLUDED.total_assets,
        market_cap = EXCLUDED.market_cap,
        vault_type = EXCLUDED.vault_type,
        last_price_event_block = EXCLUDED.last_price_event_block,
        last_price_event_log_index = EXCLUDED.last_price_event_log_index,
        updated_at = EXCLUDED.updated_at
    WHERE is_event_newer(
        EXCLUDED.last_price_event_block,
        EXCLUDED.last_price_event_log_index,
        vault.last_price_event_block,
        vault.last_price_event_log_index
    );

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_vault
    AFTER INSERT ON share_price_changed_events
    FOR EACH ROW
    EXECUTE FUNCTION update_vault_on_price_change();

-- ====================
-- Comments
-- ====================

COMMENT ON FUNCTION is_event_newer IS 'Helper to compare (block, log_index) tuples for event ordering';
COMMENT ON FUNCTION update_atom_table IS 'Trigger to maintain atom table from atom_created_events (supports NULL data with raw_data fallback)';
COMMENT ON FUNCTION update_triple_table IS 'Trigger to maintain triple table from triple_created_events';
COMMENT ON FUNCTION update_position_on_deposit IS 'Trigger to update position table on deposits (incremental)';
COMMENT ON FUNCTION update_position_on_redeem IS 'Trigger to update position table on redemptions (incremental)';
COMMENT ON FUNCTION update_vault_on_price_change IS 'Trigger to update vault table on share price changes';
