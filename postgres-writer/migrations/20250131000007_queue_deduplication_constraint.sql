-- Add unique constraint for term_id in term_update_queue
-- This ensures only one unprocessed entry exists per term_id at any time
-- When processed_at is set, the constraint no longer applies, allowing new entries for the same term_id

-- First, clean up any existing duplicate unprocessed entries
-- Keep only the oldest entry for each term_id (by id)
DELETE FROM term_update_queue
WHERE processed_at IS NULL
  AND id NOT IN (
    SELECT MIN(id)
    FROM term_update_queue
    WHERE processed_at IS NULL
    GROUP BY term_id
  );

-- Create a unique partial index for unprocessed entries
CREATE UNIQUE INDEX idx_term_update_queue_unique_pending
ON term_update_queue (term_id)
WHERE processed_at IS NULL;

COMMENT ON INDEX idx_term_update_queue_unique_pending IS
'Ensures only one unprocessed queue entry exists per term_id. When an entry is processed (processed_at IS NOT NULL), the constraint no longer applies, allowing new entries for the same term_id.';
