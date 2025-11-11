// Term update message type definition
// Used by analytics worker to process term updates received via mpsc channel
//
// Note: RabbitMQPublisher was removed as term updates now use Tokio mpsc channels
// for in-process communication instead of RabbitMQ. See migration_to_rabbitmq.md
// for the rationale behind this architectural decision.

use serde::{Deserialize, Serialize};

/// Message sent via mpsc channel for analytics worker
/// Represents a term that needs analytics table updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TermUpdateMessage {
    pub term_id: String,
    pub timestamp: i64,
}
