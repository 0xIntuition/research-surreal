-- Term update queue for analytics processing
CREATE TABLE term_update_queue (
    id BIGSERIAL PRIMARY KEY,
    term_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ NULL,
    attempts INT NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    last_attempt_at TIMESTAMPTZ NULL
);

-- Index for efficient polling of unprocessed messages
CREATE INDEX idx_term_update_queue_pending
ON term_update_queue (processed_at, created_at)
WHERE processed_at IS NULL;

-- Index for debugging and monitoring
CREATE INDEX idx_term_update_queue_term_id
ON term_update_queue (term_id);

-- Index for cleanup of processed messages
CREATE INDEX idx_term_update_queue_processed
ON term_update_queue (processed_at)
WHERE processed_at IS NOT NULL;

COMMENT ON TABLE term_update_queue IS 'Queue for term updates that need analytics processing';
COMMENT ON COLUMN term_update_queue.term_id IS 'Hex-prefixed term ID that needs analytics update';
COMMENT ON COLUMN term_update_queue.attempts IS 'Number of processing attempts (for retry logic)';
COMMENT ON COLUMN term_update_queue.last_error IS 'Error message from last failed attempt';
