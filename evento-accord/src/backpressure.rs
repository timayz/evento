//! Backpressure and rate limiting for ACCORD.
//!
//! This module provides mechanisms to prevent overload:
//!
//! - **Semaphore-based concurrency limit**: Limits concurrent transactions
//! - **Queue depth limiting**: Rejects requests when queue is full
//! - **Client-side timeouts**: Built into config
//!
//! # Example
//!
//! ```ignore
//! use evento_accord::backpressure::{ConcurrencyLimiter, BackpressureConfig};
//!
//! let config = BackpressureConfig::default();
//! let limiter = ConcurrencyLimiter::new(config);
//!
//! // Acquire a permit before processing
//! let permit = limiter.acquire().await?;
//!
//! // Process transaction...
//!
//! // Permit is released when dropped
//! drop(permit);
//! ```

use crate::error::{AccordError, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Backpressure configuration.
#[derive(Clone, Debug)]
pub struct BackpressureConfig {
    /// Maximum concurrent transactions.
    pub max_concurrent: usize,

    /// Maximum queue depth before rejecting requests.
    pub max_queue_depth: usize,

    /// Timeout for acquiring a permit.
    pub acquire_timeout: Duration,

    /// Whether to enable backpressure (false = unlimited).
    pub enabled: bool,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 1000,
            max_queue_depth: 10000,
            acquire_timeout: Duration::from_secs(30),
            enabled: true,
        }
    }
}

impl BackpressureConfig {
    /// Create a config with no limits (for testing).
    pub fn unlimited() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    /// Create a config with strict limits (for production).
    pub fn strict() -> Self {
        Self {
            max_concurrent: 100,
            max_queue_depth: 1000,
            acquire_timeout: Duration::from_secs(5),
            enabled: true,
        }
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.enabled {
            if self.max_concurrent == 0 {
                return Err(AccordError::Config(
                    "max_concurrent must be positive".into(),
                ));
            }
            if self.max_queue_depth == 0 {
                return Err(AccordError::Config(
                    "max_queue_depth must be positive".into(),
                ));
            }
            if self.acquire_timeout.is_zero() {
                return Err(AccordError::Config(
                    "acquire_timeout must be positive".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Concurrency limiter using a semaphore.
///
/// Limits the number of concurrent transactions being processed.
pub struct ConcurrencyLimiter {
    semaphore: Arc<Semaphore>,
    config: BackpressureConfig,
    waiting: Arc<AtomicUsize>,
    rejected: Arc<AtomicUsize>,
}

/// Maximum permits for "unlimited" mode (1 million).
const MAX_UNLIMITED_PERMITS: usize = 1_000_000;

impl ConcurrencyLimiter {
    /// Create a new concurrency limiter.
    pub fn new(config: BackpressureConfig) -> Self {
        let permits = if config.enabled {
            config.max_concurrent
        } else {
            // Effectively unlimited (1 million concurrent)
            MAX_UNLIMITED_PERMITS
        };

        Self {
            semaphore: Arc::new(Semaphore::new(permits)),
            config,
            waiting: Arc::new(AtomicUsize::new(0)),
            rejected: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Acquire a permit to process a transaction.
    ///
    /// Returns a permit that must be held while processing.
    /// Returns error if queue is full or timeout expires.
    pub async fn acquire(&self) -> Result<Permit> {
        if !self.config.enabled {
            return Ok(Permit {
                _inner: None,
                waiting: None,
            });
        }

        // Check queue depth
        let waiting = self.waiting.fetch_add(1, Ordering::SeqCst);
        if waiting >= self.config.max_queue_depth {
            self.waiting.fetch_sub(1, Ordering::SeqCst);
            self.rejected.fetch_add(1, Ordering::SeqCst);
            crate::metrics::record_error("backpressure_rejected");
            return Err(AccordError::Config(format!(
                "queue depth exceeded: {} >= {}",
                waiting, self.config.max_queue_depth
            )));
        }

        // Try to acquire with timeout
        let permit = tokio::time::timeout(
            self.config.acquire_timeout,
            self.semaphore.clone().acquire_owned(),
        )
        .await
        .map_err(|_| {
            self.waiting.fetch_sub(1, Ordering::SeqCst);
            self.rejected.fetch_add(1, Ordering::SeqCst);
            AccordError::Timeout("acquiring backpressure permit".into())
        })?
        .map_err(|_| {
            self.waiting.fetch_sub(1, Ordering::SeqCst);
            AccordError::Internal(anyhow::anyhow!("semaphore closed"))
        })?;

        Ok(Permit {
            _inner: Some(permit),
            waiting: Some(self.waiting.clone()),
        })
    }

    /// Try to acquire a permit without waiting.
    ///
    /// Returns `None` if no permit is available.
    pub fn try_acquire(&self) -> Option<Permit> {
        if !self.config.enabled {
            return Some(Permit {
                _inner: None,
                waiting: None,
            });
        }

        // Check queue depth
        let waiting = self.waiting.load(Ordering::SeqCst);
        if waiting >= self.config.max_queue_depth {
            self.rejected.fetch_add(1, Ordering::SeqCst);
            return None;
        }

        self.semaphore
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| {
                self.waiting.fetch_add(1, Ordering::SeqCst);
                Permit {
                    _inner: Some(permit),
                    waiting: Some(self.waiting.clone()),
                }
            })
    }

    /// Get the number of currently waiting requests.
    pub fn waiting(&self) -> usize {
        self.waiting.load(Ordering::SeqCst)
    }

    /// Get the number of rejected requests.
    pub fn rejected(&self) -> usize {
        self.rejected.load(Ordering::SeqCst)
    }

    /// Get the number of available permits.
    pub fn available(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Get statistics about the limiter.
    pub fn stats(&self) -> LimiterStats {
        LimiterStats {
            waiting: self.waiting(),
            rejected: self.rejected(),
            available: self.available(),
            max_concurrent: self.config.max_concurrent,
        }
    }
}

/// A permit to process a transaction.
///
/// The permit is released when dropped.
pub struct Permit {
    _inner: Option<OwnedSemaphorePermit>,
    waiting: Option<Arc<AtomicUsize>>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        if let Some(ref waiting) = self.waiting {
            waiting.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// Statistics about the concurrency limiter.
#[derive(Debug, Clone)]
pub struct LimiterStats {
    /// Number of requests currently waiting.
    pub waiting: usize,
    /// Total number of rejected requests.
    pub rejected: usize,
    /// Number of available permits.
    pub available: usize,
    /// Maximum concurrent requests.
    pub max_concurrent: usize,
}

impl LimiterStats {
    /// Get the current utilization (0.0 to 1.0).
    pub fn utilization(&self) -> f64 {
        if self.max_concurrent == 0 {
            return 0.0;
        }
        let in_use = self.max_concurrent.saturating_sub(self.available);
        in_use as f64 / self.max_concurrent as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backpressure_config_default() {
        let config = BackpressureConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_concurrent, 1000);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_backpressure_config_unlimited() {
        let config = BackpressureConfig::unlimited();
        assert!(!config.enabled);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_backpressure_config_strict() {
        let config = BackpressureConfig::strict();
        assert!(config.enabled);
        assert_eq!(config.max_concurrent, 100);
    }

    #[test]
    fn test_backpressure_config_validation() {
        let invalid = BackpressureConfig {
            max_concurrent: 0,
            enabled: true,
            ..Default::default()
        };
        assert!(invalid.validate().is_err());
    }

    #[tokio::test]
    async fn test_limiter_acquire() {
        let config = BackpressureConfig {
            max_concurrent: 2,
            max_queue_depth: 10,
            acquire_timeout: Duration::from_secs(1),
            enabled: true,
        };
        let limiter = ConcurrencyLimiter::new(config);

        // Acquire permits
        let p1 = limiter.acquire().await.unwrap();
        let p2 = limiter.acquire().await.unwrap();

        assert_eq!(limiter.available(), 0);

        // Release one
        drop(p1);
        assert_eq!(limiter.available(), 1);

        // Acquire another
        let _p3 = limiter.acquire().await.unwrap();
        assert_eq!(limiter.available(), 0);

        drop(p2);
    }

    #[tokio::test]
    async fn test_limiter_try_acquire() {
        let config = BackpressureConfig {
            max_concurrent: 1,
            max_queue_depth: 10,
            acquire_timeout: Duration::from_secs(1),
            enabled: true,
        };
        let limiter = ConcurrencyLimiter::new(config);

        // First should succeed
        let p1 = limiter.try_acquire();
        assert!(p1.is_some());

        // Second should fail (no waiting)
        let p2 = limiter.try_acquire();
        assert!(p2.is_none());

        // After release, should succeed
        drop(p1);
        let p3 = limiter.try_acquire();
        assert!(p3.is_some());
    }

    #[tokio::test]
    async fn test_limiter_unlimited() {
        let config = BackpressureConfig::unlimited();
        let limiter = ConcurrencyLimiter::new(config);

        // Should always succeed
        let _p1 = limiter.acquire().await.unwrap();
        let _p2 = limiter.acquire().await.unwrap();
        let _p3 = limiter.try_acquire().unwrap();
    }

    #[test]
    fn test_limiter_stats() {
        let config = BackpressureConfig {
            max_concurrent: 10,
            ..Default::default()
        };
        let limiter = ConcurrencyLimiter::new(config);

        let stats = limiter.stats();
        assert_eq!(stats.waiting, 0);
        assert_eq!(stats.rejected, 0);
        assert_eq!(stats.available, 10);
        assert_eq!(stats.max_concurrent, 10);
        assert_eq!(stats.utilization(), 0.0);
    }
}
