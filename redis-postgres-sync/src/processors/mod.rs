// Processors module for cascading updates after event processing
// Handles complex aggregations and maintains derived tables

pub mod cascade;
pub mod vault_updater;
pub mod term_updater;

pub use cascade::CascadeProcessor;
pub use vault_updater::VaultUpdater;
pub use term_updater::TermUpdater;
