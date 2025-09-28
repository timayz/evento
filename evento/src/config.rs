//! Configuration constants and defaults for the evento library
//!
//! This module contains configurable constants that were previously hardcoded
//! throughout the codebase. Users can customize these values for their specific needs.

use std::time::Duration;

/// Default configuration for evento operations
#[derive(Debug, Clone)]
pub struct EventoConfig {
    /// Default batch size for reading events
    pub default_batch_size: u32,
    
    /// Maximum number of pagination loops before giving up
    pub max_pagination_loops: u32,
    
    /// Interval between pagination batches
    pub pagination_interval: Duration,
    
    /// Default chunk size for subscriptions
    pub default_subscription_chunk_size: u16,
}

impl Default for EventoConfig {
    fn default() -> Self {
        Self {
            default_batch_size: DEFAULT_BATCH_SIZE,
            max_pagination_loops: MAX_PAGINATION_LOOPS,
            pagination_interval: PAGINATION_INTERVAL,
            default_subscription_chunk_size: DEFAULT_SUBSCRIPTION_CHUNK_SIZE,
        }
    }
}

/// Default batch size for reading events from the store
/// 
/// This determines how many events are loaded at once when reconstructing aggregates.
/// Larger values reduce the number of database round trips but use more memory.
pub const DEFAULT_BATCH_SIZE: u32 = 1000;

/// Maximum number of pagination loops before giving up
/// 
/// This prevents infinite loops when reading large numbers of events.
/// If an aggregate has more events than this limit allows, a TooManyEvents error is returned.
pub const MAX_PAGINATION_LOOPS: u32 = 10;

/// Default interval between pagination batches
/// 
/// This small delay helps prevent overwhelming the database with rapid consecutive queries
/// when processing large numbers of events.
pub const PAGINATION_INTERVAL: Duration = Duration::from_secs(1);

/// Default chunk size for subscription processing
/// 
/// This determines how many events are processed in each batch during subscription handling.
pub const DEFAULT_SUBSCRIPTION_CHUNK_SIZE: u16 = 100;

/// Configuration builder for customizing evento behavior
#[derive(Debug)]
pub struct ConfigBuilder {
    config: EventoConfig,
}

impl ConfigBuilder {
    /// Create a new configuration builder with default values
    pub fn new() -> Self {
        Self {
            config: EventoConfig::default(),
        }
    }

    /// Set the default batch size for reading events
    pub fn batch_size(mut self, size: u32) -> Self {
        self.config.default_batch_size = size;
        self
    }

    /// Set the maximum number of pagination loops
    pub fn max_loops(mut self, max: u32) -> Self {
        self.config.max_pagination_loops = max;
        self
    }

    /// Set the interval between pagination batches
    pub fn pagination_interval(mut self, interval: Duration) -> Self {
        self.config.pagination_interval = interval;
        self
    }

    /// Set the default subscription chunk size
    pub fn subscription_chunk_size(mut self, size: u16) -> Self {
        self.config.default_subscription_chunk_size = size;
        self
    }

    /// Build the final configuration
    pub fn build(self) -> EventoConfig {
        self.config
    }
}

impl Default for ConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}