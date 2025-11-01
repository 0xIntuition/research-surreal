# PostgreSQL Writer: Logic Comparison Analysis

**Date:** November 1, 2025
**Scope:** Comparison of materialized views approach vs. trigger-based approach
**Focus Areas:** Logic correctness, non-sequential event handling, edge case coverage

---

## Executive Summary

This report analyzes the transition from a materialized views-based architecture to a trigger-based architecture in the `postgres-writer` component. The analysis reveals **one critical discrepancy** in position share tracking logic, several edge cases that need additional test coverage, and provides recommendations for ensuring correctness.

### Key Findings

1. **CRITICAL:** Position shares field discrepancy between approaches
2. **GOOD:** Non-sequential event handling is properly implemented with ordering checks
3. **GOOD:** Concurrent update protection via advisory locks
4. **GAP:** Some edge cases lack explicit test coverage
5. **GOOD:** Accumulation logic for deposits/redeems is correctly implemented

---

## 1. Architecture Comparison

### Materialized Views Approach (Original)

**Structure:**
- Event tables → Materialized views (9 views total)
- Manual refresh required (CONCURRENTLY supported)
- Complex CTEs with DISTINCT ON for latest events
- Full aggregation on every refresh

**Pros:**
- Declarative SQL logic, easier to audit
- Atomic refresh ensures consistency
- Well-understood PostgreSQL feature

**Cons:**
- Stale data between refreshes
- Expensive full recalculation
- Cannot react immediately to events
- Requires external refresh scheduling

### Trigger-Based Approach (Current)

**Structure:**
- Event tables → Triggers → Base tables → Rust cascade → Aggregated tables
- Immediate updates on event insertion
- Incremental updates only for affected entities
- Advisory locks for concurrency control

**Pros:**
- Real-time updates (no staleness)
- Efficient incremental processing
- Scales better with event volume
- Better integration with application logic

**Cons:**
- More complex debugging (trigger + Rust code)
- Requires careful ordering logic
- Potential for race conditions (mitigated by locks)

---

## 2. CRITICAL ISSUE: Position Shares Field Discrepancy

### Problem Description

The two approaches use **different fields** from the `deposited_events` table for tracking position shares:

#### Materialized View (Original)
```sql
-- File: 20250130000002_position_view.sql:18
CAST(total_shares AS numeric(78,0)) AS total_shares,  -- Uses total_shares field
```

#### Trigger (Current)
```sql
-- File: 20250131000003_create_triggers.sql:189
shares = NEW.shares::NUMERIC(78, 0),  -- Uses shares field
```

### Event Structure

From `deposited_events` table (20250130000001_initial_schema.sql:32-33):
```sql
shares TEXT NOT NULL,        -- Field used by TRIGGER
term_id TEXT NOT NULL,
total_shares TEXT NOT NULL,  -- Field used by MATERIALIZED VIEW
```

From the Rust event handler (`src/sync/deposited.rs:18-22`):
```rust
pub shares: String,        // Delta shares for this transaction
pub total_shares: String,  // Cumulative shares after transaction
```

### Semantic Difference

Based on standard ERC4626 vault semantics:

- **`shares`**: The number of shares minted/transferred in THIS specific transaction (delta)
- **`total_shares`**: The user's total position shares AFTER this transaction (cumulative)

If this interpretation is correct, then:
- ✅ **Materialized view is CORRECT** - uses cumulative `total_shares`
- ❌ **Trigger logic is INCORRECT** - uses delta `shares` instead of cumulative

### Impact

If the interpretation above is correct, positions will show incorrect share balances under the trigger approach. However, this needs verification by:
1. Examining actual blockchain event data
2. Checking smart contract event definitions
3. Running the existing tests (which appear to pass)

### Recommendation: URGENT

**Priority: P0 - Critical**

1. **Immediately verify** the semantic meaning of `shares` vs `total_shares` in the actual blockchain events
2. If `total_shares` represents cumulative position shares, update the trigger to use:
   ```sql
   shares = NEW.total_shares::NUMERIC(78, 0),  -- Use total_shares instead
   ```
3. Add explicit tests that verify share accumulation across multiple deposits
4. Review all existing tests - they may be passing due to oversimplified mock data

### Alternative Interpretation

It's possible that the event structure changed between implementations, or that `shares` in the blockchain event already represents the user's cumulative position. The passing tests suggest this might be the case, but the mock data in `TESTING_PROPOSAL.md` lines 409-411 appears oversimplified:

```rust
"shares": shares.to_string(),
"totalShares": (shares * 2).to_string(),  // Suspiciously simple formula
```

---

## 3. Logic Comparison: Position Handling

### 3.1 Out-of-Order Deposit Handling

#### Materialized View Logic
```sql
-- Lines 14-25: Get latest deposit per position
SELECT DISTINCT ON (receiver, term_id, curve_id)
    ...
    CAST(total_shares AS numeric(78,0)) AS total_shares,
FROM public.deposited_events
ORDER BY receiver, term_id, curve_id, block_number DESC, log_index DESC
```

**Behavior:** Selects the event with highest `(block_number, log_index)` for shares.

#### Trigger Logic
```sql
-- Lines 199-210: Handle newer deposits
IF is_event_newer(NEW.block_number, NEW.log_index, current_deposit_block, current_deposit_log_index) THEN
    UPDATE position SET
        shares = NEW.shares::NUMERIC(78, 0),  -- Update shares from new event
        total_deposit_assets_after_total_fees = current_deposit_total + NEW.assets_after_fees::NUMERIC(78, 0),
        last_deposit_block = NEW.block_number,
        last_deposit_log_index = NEW.log_index,
        ...
```

**Behavior:** Only updates shares if new event has higher `(block_number, log_index)`.

**Comparison:** ✅ **EQUIVALENT LOGIC** (assuming field discrepancy is resolved)

Both approaches:
- Compare `(block_number, log_index)` tuples to determine ordering
- Use shares from the most recent event
- Always accumulate deposit totals regardless of order

### 3.2 Deposit/Redeem Interaction

#### Materialized View Logic
```sql
-- Lines 46-64: Combine latest from both deposit and redeem
latest_shares AS (
    SELECT
        ...
        CASE
            WHEN d.block_number > r.block_number THEN d.total_shares
            WHEN d.block_number < r.block_number THEN r.total_shares
            WHEN d.log_index > r.log_index THEN d.total_shares
            ELSE r.total_shares
        END AS shares,
```

**Behavior:** Compares latest deposit vs latest redeem, takes shares from whichever is more recent.

#### Trigger Logic
```sql
-- Separate triggers for deposits and redeems
-- Each tracks last_deposit_block/last_redeem_block independently
-- Shares are updated only if the new event is newer than the last event of that type
```

**Comparison:** ⚠️ **POTENTIAL DIFFERENCE**

The trigger approach tracks deposits and redeems separately:
- `last_deposit_block`/`last_deposit_log_index`
- `last_redeem_block`/`last_redeem_log_index`

This means:
- If deposit at block 1005 arrives, position.shares = deposit.shares
- If redeem at block 1003 arrives later, position.shares stays at deposit.shares (correct!)
- If redeem at block 1010 arrives, position.shares = redeem.shares (correct!)

However, there's a subtle issue: **The trigger doesn't compare deposits vs redeems across types.**

**Example Scenario:**
1. Deposit at block 1000 (shares = 5000) arrives → position.shares = 5000
2. Redeem at block 1005 (shares = 3000) arrives → position.shares = 3000
3. Deposit at block 1003 (shares = 4000) arrives later (out of order)
   - Deposit is at block 1003, which is OLDER than last_deposit_block (1000)
   - Deposit trigger sees: block 1003 > last_deposit_block 1000 → **Updates position.shares = 4000**
   - But this is WRONG! The redeem at block 1005 should be the final shares (3000)

**Issue:** The trigger compares within event types (deposit vs deposit, redeem vs redeem) but not across types (deposit vs redeem).

### Recommendation: HIGH PRIORITY

**Priority: P0 - Critical**

The trigger logic needs to track a single `last_event_block` and `last_event_log_index` for the position, not separate ones for deposits and redeems.

**Proposed Fix:**

```sql
CREATE TABLE position (
    ...
    -- Remove: last_deposit_block, last_deposit_log_index
    -- Remove: last_redeem_block, last_redeem_log_index
    -- Add: single tracking for ANY event affecting shares
    last_shares_event_block BIGINT,
    last_shares_event_log_index BIGINT,
    ...
);

-- Update deposit trigger to compare against last_shares_event_block
IF is_event_newer(NEW.block_number, NEW.log_index, last_shares_event_block, last_shares_event_log_index) THEN
    UPDATE position SET
        shares = NEW.shares::NUMERIC(78, 0),
        last_shares_event_block = NEW.block_number,
        last_shares_event_log_index = NEW.log_index,
        ...
```

Add explicit test:
```rust
#[tokio::test]
async fn test_interleaved_deposits_and_redeems_out_of_order() {
    // Publish events: Deposit@1000, Redeem@1005, Deposit@1003 (out of order)
    // Expected: position.shares should be 3000 (from Redeem@1005, the latest)
    // Current behavior: might incorrectly use Deposit@1003
}
```

---

## 4. Logic Comparison: Vault Updates

### 4.1 Position Count Calculation

#### Materialized View
```sql
-- Lines 48-56: Count positions with shares > 0
position_counts AS (
    SELECT
        term_id,
        curve_id,
        COUNT(*) AS position_count
    FROM public.position
    WHERE shares > 0
    GROUP BY term_id, curve_id
),
```

#### Trigger (Rust Cascade)
```sql
-- File: src/processors/vault_updater.rs:28-44
SELECT
    COUNT(*) FILTER (WHERE shares > 0) as position_count,
    ...
FROM position
WHERE term_id = $1 AND curve_id = $2
```

**Comparison:** ✅ **EQUIVALENT LOGIC**

Both filter by `shares > 0` when counting active positions.

### 4.2 Market Cap Calculation

#### Materialized View
```sql
-- Lines 76-77: Calculate market cap
CAST((lsp.total_shares * lsp.current_share_price / 1000000000000000000) AS NUMERIC(78, 0)) AS market_cap,
```

#### Trigger
```sql
-- File: 20250131000003_create_triggers.sql:396-397
(NEW.total_shares::NUMERIC(78, 0) * NEW.share_price::NUMERIC(78, 0)) / 1000000000000000000,
```

**Comparison:** ✅ **EQUIVALENT LOGIC**

Both use the same formula: `(total_shares * share_price) / 1e18`

### 4.3 Overflow Protection

#### Materialized View
No explicit overflow protection.

#### Trigger (Rust Cascade)
```sql
-- File: src/processors/vault_updater.rs:69-72
WHEN total_shares > (10^78 - 1) / NULLIF(current_share_price, 0) THEN NULL
ELSE (total_shares * current_share_price) / 1000000000000000000
```

**Comparison:** ✅ **IMPROVEMENT IN TRIGGER**

The trigger approach adds overflow protection, which is better.

---

## 5. Non-Sequential Event Handling Analysis

### 5.1 Ordering Mechanism

Both approaches use `(block_number, log_index)` tuples for ordering:

#### Helper Function (Trigger)
```sql
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
```

**Behavior:**
- If no previous event: accept new event
- If `new_block > old_block`: accept new event
- If `new_block == old_block AND new_log_index > old_log_index`: accept new event
- Otherwise: reject (event is older)

This is **correct** for blockchain event ordering.

### 5.2 Test Coverage

Existing tests in `tests/integration/out_of_order.rs`:

✅ **test_deposits_processed_correctly_despite_out_of_order_arrival**
- Tests: deposits arriving in order 1005, 1001, 1003
- Verifies: shares from block 1005, total deposits = 9000
- Status: PASSES

✅ **test_share_price_changes_use_latest_block**
- Tests: price changes arriving in order 1008, 1002, 1005
- Verifies: price from block 1008
- Status: PASSES

### 5.3 Missing Test Cases

The following scenarios are NOT explicitly tested:

❌ **Cross-type out-of-order (deposits vs redeems)**
```
Scenario: Deposit@1000 → Redeem@1005 → Deposit@1003 (out of order)
Expected: shares from Redeem@1005 (most recent across ALL event types)
Risk: HIGH (see section 3.2)
```

❌ **Same block, different log indices**
```
Scenario: Deposit@1000:5 → Deposit@1000:2 (same block, different log_index)
Expected: shares from Deposit@1000:5 (higher log_index)
Risk: MEDIUM
```

❌ **Multiple redeems to zero**
```
Scenario: Deposit@1000 → Redeem(to 0)@1005 → Redeem(to 0)@1003 (out of order)
Expected: shares = 0, position_count should reflect this
Risk: MEDIUM
```

❌ **First event is a redeem (edge case)**
```
Scenario: Redeem arrives before any deposit
Expected: Position created with negative balance or error handling
Risk: LOW (unlikely in practice)
Test: Lines 310-334 in create_triggers.sql handle this, but no explicit test
```

❌ **Accumulation with out-of-order events**
```
Scenario:
  1. Deposit 5000@1005 (arrives first)
  2. Deposit 1000@1001 (arrives second, older)
  3. Deposit 3000@1003 (arrives third, older)
Expected:
  - shares = 5000 (from latest block 1005)
  - total_deposit_assets_after_total_fees = 9000 (sum of ALL)
Current test: PASSES (test_deposits_processed_correctly_despite_out_of_order_arrival)
Status: ✅ COVERED
```

---

## 6. Concurrent Update Analysis

### 6.1 Concurrency Protection

#### Materialized View
- REFRESH MATERIALIZED VIEW CONCURRENTLY allows reads during refresh
- Atomic replacement via unique index
- Single writer (refresh operation)

#### Trigger (Rust Cascade)
- Advisory locks prevent concurrent updates to same position
- Lock based on stable hash of (account_id, term_id, curve_id)
```rust
// File: src/processors/cascade.rs:44-48
let lock_id = Self::hash_position(account_id, term_id, curve_id);
sqlx::query("SELECT pg_advisory_xact_lock($1)")
    .bind(lock_id)
    .execute(&mut **tx)
    .await
```

**Comparison:** ✅ **EQUIVALENT PROTECTION**

Both approaches prevent race conditions:
- Materialized view: atomic refresh
- Triggers: advisory locks within transactions

### 6.2 Test Coverage

Existing tests in `tests/integration/concurrent_updates.rs`:

✅ **test_concurrent_deposits_to_same_vault_maintain_consistency**
- Tests: 10 accounts depositing concurrently
- Verifies: position_count = 10, all positions exist
- Status: PASSES

✅ **test_concurrent_deposits_and_redeems_maintain_consistency**
- Tests: 3 deposits + 2 redeems for same account
- Verifies: final shares, deposit total, redeem total
- Status: PASSES

✅ **test_position_count_updates_correctly_with_concurrent_deposits**
- Tests: 5 deposits, 2 redeems to zero
- Verifies: position_count = 3 (only non-zero positions)
- Status: PASSES

### 6.3 Advisory Lock Hash Collisions

The advisory lock uses a custom hash function:

```rust
// File: src/processors/cascade.rs:135-153
fn hash_position(account_id: &str, term_id: &str, curve_id: &str) -> i64 {
    let mut hash = 0xcbf29ce484222325u64; // FNV offset basis
    // ... FNV-1a hashing ...
    ((hash >> 32) ^ (hash & 0xFFFFFFFF)) as i64 & 0x7FFFFFFFFFFFFFFF
}
```

**Analysis:**
- Uses FNV-1a hash (good distribution)
- XOR-folds to 63 bits (avoids sign bit issues)
- Deterministic (same inputs always produce same lock_id)

**Risk:** Hash collisions could cause unrelated positions to wait for each other's locks.

**Mitigation:** FNV-1a has good distribution properties. Collisions are rare with 63-bit space.

**Recommendation:** Monitor lock wait times in production. Consider logging lock_id for debugging.

---

## 7. Edge Cases Analysis

### 7.1 Covered Edge Cases

✅ **Duplicate events (same transaction_hash, log_index)**
- Handled by PRIMARY KEY constraint
- ON CONFLICT in event insertion
- Status: HANDLED

✅ **NULL timestamp handling**
- `block_timestamp TIMESTAMPTZ` (nullable in event tables)
- Status: HANDLED

✅ **Zero shares position**
- Filtered correctly in position_count queries (`shares > 0`)
- Status: HANDLED

✅ **Large numbers (NUMERIC(78,0))**
- Overflow protection in market_cap calculation
- Status: HANDLED

### 7.2 Uncovered Edge Cases

❌ **Vault created before any SharePriceChanged event**
- Trigger creates vault on first deposit with default values
```sql
-- File: create_triggers.sql:256-258
total_shares => 0,  -- Updated by SharePriceChanged only
current_share_price => 0,  -- Updated by SharePriceChanged only
```
- Risk: Division by zero in cascade processor?
- Check: vault_updater.rs:62 uses `WHEN current_share_price > 0`
- Status: ✅ HANDLED (but not explicitly tested)

❌ **Atom/Triple created but no vault activity**
- Term initialized with zero values
- Risk: Queries might return terms with no real activity
- Impact: LOW (filtering on market_cap > 0 would exclude these)
- Status: ACCEPTABLE

❌ **Very high concurrency on single position**
- Advisory lock serializes updates
- Risk: Lock wait timeout if processing is slow
- Mitigation: Monitor lock_wait_time metric
- Status: MONITORING NEEDED

❌ **Cascade processor failure mid-transaction**
- Entire transaction rolls back (good!)
- But: Event stays in Redis stream, will retry
- Risk: Repeated failures could cause backlog
- Status: HANDLED (circuit breaker in pipeline)

---

## 8. Aggregation Logic Comparison

### 8.1 Term Aggregation

#### Materialized View
```sql
-- Lines 27-36 in term_view.sql
SELECT
    term_id,
    vault_type,
    SUM(total_assets) AS total_assets,
    SUM(market_cap) AS total_market_cap,
    MIN(created_at) AS created_at,
    MAX(updated_at) AS updated_at
FROM public.vault
GROUP BY term_id, vault_type
```

#### Trigger (Rust Cascade)
```rust
// File: src/processors/term_updater.rs:28-46
SELECT
    COALESCE(SUM(total_assets), 0)::text as total_assets,
    COALESCE(SUM(market_cap), 0)::text as total_market_cap
FROM vault
WHERE term_id = $1
```

**Difference:** The materialized view groups by `vault_type`, but the Rust cascade doesn't.

**Impact:** Minimal - term table design uses `type` field to distinguish Atom/Triple/CounterTriple.

**Comparison:** ✅ **FUNCTIONALLY EQUIVALENT**

### 8.2 Analytics Tables (predicate_object, subject_predicate)

#### Materialized View
```sql
-- Lines 24-32 in predicate_aggregates.sql
SELECT
    t.predicate_id,
    t.object_id,
    COUNT(DISTINCT t.term_id)::INTEGER AS triple_count,
    COALESCE(SUM(tt.total_position_count), 0)::INTEGER AS total_position_count,
    COALESCE(SUM(tt.total_market_cap), 0) AS total_market_cap
FROM triple t
LEFT JOIN triple_term tt ON tt.term_id = t.term_id
GROUP BY t.predicate_id, t.object_id
```

#### Trigger Approach
- Analytics tables updated by separate analytics worker (out of critical path)
- Processes `term_updates` Redis stream
- File: `src/analytics/processor.rs` (not in this analysis)

**Comparison:** ⚠️ **ARCHITECTURAL CHANGE**

The trigger approach moves analytics to an eventual consistency model:
- Pros: Main pipeline not blocked by analytics calculations
- Cons: Analytics data may lag behind real-time data

**Impact:** Acceptable trade-off for performance.

---

## 9. Performance Considerations

### 9.1 Write Performance

| Metric | Materialized View | Trigger Approach |
|--------|-------------------|------------------|
| Event insertion | Fast (just INSERT) | Fast (INSERT + trigger) |
| Aggregation | Expensive (full refresh) | Efficient (incremental) |
| Lock contention | Low (single writer) | Medium (per-position locks) |
| Latency | High (up to refresh interval) | Low (immediate) |

**Recommendation:** Trigger approach is better for write-heavy workloads.

### 9.2 Read Performance

| Metric | Materialized View | Trigger Approach |
|--------|-------------------|------------------|
| Query speed | Fast (indexed matview) | Fast (indexed tables) |
| Data freshness | Stale (up to refresh) | Real-time |
| Read consistency | Atomic (single snapshot) | Eventual (cascade lag) |

**Recommendation:** Similar read performance, but trigger approach provides fresher data.

### 9.3 Advisory Lock Performance

Potential bottleneck: Multiple events for the same position must serialize.

**Mitigation:**
- Events are processed in parallel by worker pool
- Advisory locks only serialize updates to SAME position
- Different positions can update concurrently

**Monitoring:** Track `pg_stat_database.blk_read_time` and `pg_locks` table.

---

## 10. Recommendations

### Priority P0 - CRITICAL (Must Fix Immediately)

1. **Verify shares vs total_shares field semantics**
   - Check actual blockchain event structure
   - Update trigger to use correct field (likely `total_shares`)
   - File: `create_triggers.sql:189`

2. **Fix cross-type event ordering (deposits vs redeems)**
   - Replace separate `last_deposit_block`/`last_redeem_block` with single `last_shares_event_block`
   - Update both deposit and redeem triggers to compare against unified tracking
   - Add explicit test for interleaved deposit/redeem out-of-order scenario
   - Files: `create_triggers.sql`, `create_regular_tables.sql`

### Priority P1 - HIGH (Fix This Sprint)

3. **Add missing test cases**
   - Cross-type out-of-order events (deposit vs redeem)
   - Same block, different log indices
   - Multiple redeems to zero
   - First event is a redeem (edge case)

4. **Add integration test for field semantics**
   - Test that verifies `shares` vs `total_shares` produces expected results
   - Use real-world event data if possible
   - Add assertions on cumulative share balance

5. **Add monitoring for advisory locks**
   - Track lock wait times
   - Alert on lock contention
   - Log lock_id for debugging

### Priority P2 - MEDIUM (Nice to Have)

6. **Document event semantics**
   - Create documentation explaining each event field
   - Include examples from actual blockchain data
   - Document assumptions about event ordering

7. **Add stress tests**
   - Very high concurrency (100+ workers)
   - Very large batches (10K+ events)
   - Measure lock contention under load

8. **Consider adding event replay capability**
   - For testing: ability to replay production events
   - For recovery: reprocess events after bug fixes

### Priority P3 - LOW (Future Optimization)

9. **Optimize advisory lock hash function**
   - Consider using PostgreSQL's built-in hash functions
   - Profile for collision rates in production

10. **Add cascade processor metrics**
    - Track cascade processing time
    - Monitor backpressure from cascade updates

---

## 11. Conclusion

### Summary

The trigger-based approach is **architecturally superior** to the materialized views approach for real-time event processing. However, there are **two critical issues** that must be addressed:

1. **shares vs total_shares field discrepancy** (P0)
2. **Cross-type event ordering** (P0)

Once these are fixed, the trigger approach will be:
- ✅ More efficient (incremental updates)
- ✅ Lower latency (real-time updates)
- ✅ Better scalability (parallel processing)
- ✅ Properly handles out-of-order events (with fixes)

### Test Coverage Assessment

Current test coverage is **GOOD** but has gaps:

| Category | Coverage | Status |
|----------|----------|--------|
| Basic deposit/redeem | ✅ Covered | GOOD |
| Out-of-order deposits | ✅ Covered | GOOD |
| Out-of-order price changes | ✅ Covered | GOOD |
| Concurrent updates | ✅ Covered | GOOD |
| Cross-type ordering | ❌ Missing | CRITICAL GAP |
| Same-block events | ❌ Missing | GAP |
| Field semantics | ❌ Missing | CRITICAL GAP |

### Recommendation

**DO NOT DEPLOY** trigger-based approach to production until:
1. ✅ Verify and fix shares/total_shares field usage (P0)
2. ✅ Fix cross-type event ordering (P0)
3. ✅ Add missing test cases (P1)
4. ✅ All tests pass with real-world event data

Once fixed and tested, the trigger-based approach is **recommended for production use**.

---

## Appendix A: Key Files Analyzed

### Migrations
- `20250130000001_initial_schema.sql` - Event table schemas
- `20250130000002_position_view.sql` - Original position logic
- `20250130000003_vault_view.sql` - Original vault logic
- `20250131000002_create_regular_tables.sql` - New table structure
- `20250131000003_create_triggers.sql` - Trigger implementation

### Source Code
- `src/processors/cascade.rs` - Cascade orchestration
- `src/processors/vault_updater.rs` - Vault aggregation
- `src/processors/term_updater.rs` - Term aggregation
- `src/sync/deposited.rs` - Deposit event handling

### Tests
- `tests/integration/out_of_order.rs` - Non-sequential event tests
- `tests/integration/concurrent_updates.rs` - Concurrency tests
- `TESTING_PROPOSAL.md` - Test design document

---

## Appendix B: Verification Queries

Use these queries to verify correctness in a test environment:

### Check Position Shares Match Latest Event
```sql
-- Should return zero rows if logic is correct
WITH latest_event_per_position AS (
  SELECT DISTINCT ON (receiver, term_id, curve_id)
    receiver, term_id, curve_id,
    total_shares,  -- Or 'shares' - depends on which is correct!
    block_number, log_index
  FROM deposited_events
  ORDER BY receiver, term_id, curve_id, block_number DESC, log_index DESC
)
SELECT
  p.account_id, p.term_id, p.curve_id,
  p.shares AS current_shares,
  e.total_shares AS expected_shares,
  p.shares != e.total_shares::numeric AS mismatch
FROM position p
JOIN latest_event_per_position e
  ON p.account_id = e.receiver
  AND p.term_id = e.term_id
  AND p.curve_id::text = e.curve_id
WHERE p.shares != e.total_shares::numeric;
```

### Check Deposit Accumulation
```sql
-- Verify total deposits are accumulated correctly
WITH deposit_sums AS (
  SELECT
    receiver, term_id, curve_id,
    SUM(assets_after_fees::numeric) AS expected_total
  FROM deposited_events
  GROUP BY receiver, term_id, curve_id
)
SELECT
  p.account_id, p.term_id, p.curve_id,
  p.total_deposit_assets_after_total_fees AS current_total,
  d.expected_total,
  p.total_deposit_assets_after_total_fees != d.expected_total AS mismatch
FROM position p
JOIN deposit_sums d
  ON p.account_id = d.receiver
  AND p.term_id = d.term_id
  AND p.curve_id::text = d.curve_id
WHERE p.total_deposit_assets_after_total_fees != d.expected_total;
```

### Check Cross-Type Event Ordering
```sql
-- Find positions where last deposit > last redeem but redeem exists
-- This could indicate ordering issues
SELECT
  account_id, term_id, curve_id,
  last_deposit_block, last_deposit_log_index,
  last_redeem_block, last_redeem_log_index
FROM position
WHERE last_redeem_block IS NOT NULL
  AND last_deposit_block IS NOT NULL
  AND (last_deposit_block > last_redeem_block
       OR (last_deposit_block = last_redeem_block AND last_deposit_log_index > last_redeem_log_index));
```

---

**Report End**
