//! Key extraction and conflict detection for ACCORD.
//!
//! Keys identify which aggregates a transaction touches. Two transactions
//! conflict if they share at least one key.

use evento_core::Event;
use std::collections::BTreeSet;

/// A key representing an aggregate instance.
///
/// Format: "aggregator_type:aggregator_id"
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(pub String);

impl Key {
    /// Create a key from aggregator type and id
    pub fn new(aggregator_type: &str, aggregator_id: &str) -> Self {
        Self(format!("{}:{}", aggregator_type, aggregator_id))
    }

    /// Extract keys from a list of events.
    ///
    /// Returns a sorted, deduplicated list of keys.
    pub fn from_events(events: &[Event]) -> Vec<String> {
        events
            .iter()
            .map(|e| format!("{}:{}", e.aggregator_type, e.aggregator_id))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Get the aggregator type from the key
    pub fn aggregator_type(&self) -> Option<&str> {
        self.0.split(':').next()
    }

    /// Get the aggregator id from the key
    pub fn aggregator_id(&self) -> Option<&str> {
        self.0.split(':').nth(1)
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for Key {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for Key {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

/// Check if two key sets have any overlap (conflict).
///
/// Both key slices must be sorted for O(n+m) performance.
pub fn conflicts(a: &[String], b: &[String]) -> bool {
    let mut i = 0;
    let mut j = 0;

    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Equal => return true,
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
        }
    }

    false
}

/// Find common keys between two key sets.
///
/// Both key slices must be sorted.
pub fn intersection(a: &[String], b: &[String]) -> Vec<String> {
    let mut result = Vec::new();
    let mut i = 0;
    let mut j = 0;

    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Equal => {
                result.push(a[i].clone());
                i += 1;
                j += 1;
            }
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_new() {
        let key = Key::new("bank/Account", "acc-123");
        assert_eq!(key.0, "bank/Account:acc-123");
    }

    #[test]
    fn test_key_parts() {
        let key = Key::new("bank/Account", "acc-123");
        assert_eq!(key.aggregator_type(), Some("bank/Account"));
        assert_eq!(key.aggregator_id(), Some("acc-123"));
    }

    #[test]
    fn test_conflicts_with_overlap() {
        let a = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
        let b = vec!["key2".to_string(), "key4".to_string()];

        assert!(conflicts(&a, &b));
    }

    #[test]
    fn test_conflicts_no_overlap() {
        let a = vec!["key1".to_string(), "key2".to_string()];
        let b = vec!["key3".to_string(), "key4".to_string()];

        assert!(!conflicts(&a, &b));
    }

    #[test]
    fn test_conflicts_empty() {
        let a: Vec<String> = vec![];
        let b = vec!["key1".to_string()];

        assert!(!conflicts(&a, &b));
        assert!(!conflicts(&b, &a));
    }

    #[test]
    fn test_intersection() {
        let a = vec![
            "key1".to_string(),
            "key2".to_string(),
            "key3".to_string(),
            "key5".to_string(),
        ];
        let b = vec!["key2".to_string(), "key3".to_string(), "key4".to_string()];

        let common = intersection(&a, &b);
        assert_eq!(common, vec!["key2".to_string(), "key3".to_string()]);
    }

    #[test]
    fn test_intersection_empty() {
        let a = vec!["key1".to_string()];
        let b = vec!["key2".to_string()];

        let common = intersection(&a, &b);
        assert!(common.is_empty());
    }
}
