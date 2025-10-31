// Analytics worker module
// Processes term update messages from Redis and updates analytics tables

mod processor;
mod worker;

pub use worker::start_analytics_worker;
