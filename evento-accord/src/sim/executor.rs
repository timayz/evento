//! Simulated in-memory executor for testing.
//!
//! Provides a simple in-memory implementation of the Executor trait
//! for use in simulation testing.

use async_trait::async_trait;
use evento_core::cursor::{Args, Edge, PageInfo, ReadResult, Value};
use evento_core::{Event, Executor, ReadAggregator, RoutingKey, WriteError};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use ulid::Ulid;

/// Type alias for event storage.
type EventStore = Arc<RwLock<HashMap<(String, String), Vec<Event>>>>;

/// In-memory executor for simulation testing.
///
/// Stores all events in memory with no persistence.
/// Designed for fast, deterministic testing.
#[derive(Clone)]
pub struct SimExecutor {
    /// Events stored by (aggregator_type, aggregator_id) -> Vec<Event>
    events: EventStore,

    /// Subscriber cursors
    cursors: Arc<RwLock<HashMap<String, Value>>>,

    /// Active subscribers
    subscribers: Arc<RwLock<HashMap<String, Ulid>>>,

    /// Write counter for statistics
    write_count: Arc<RwLock<u64>>,
}

impl SimExecutor {
    /// Create a new in-memory executor.
    pub fn new() -> Self {
        Self {
            events: Arc::new(RwLock::new(HashMap::new())),
            cursors: Arc::new(RwLock::new(HashMap::new())),
            subscribers: Arc::new(RwLock::new(HashMap::new())),
            write_count: Arc::new(RwLock::new(0)),
        }
    }

    /// Get all events for an aggregate.
    pub fn get_events(&self, agg_type: &str, agg_id: &str) -> Vec<Event> {
        self.events
            .read()
            .unwrap()
            .get(&(agg_type.to_string(), agg_id.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    /// Get all events across all aggregates.
    pub fn all_events(&self) -> Vec<Event> {
        self.events
            .read()
            .unwrap()
            .values()
            .flatten()
            .cloned()
            .collect()
    }

    /// Get total event count.
    pub fn event_count(&self) -> usize {
        self.events.read().unwrap().values().map(|v| v.len()).sum()
    }

    /// Get number of distinct aggregates.
    pub fn aggregate_count(&self) -> usize {
        self.events.read().unwrap().len()
    }

    /// Get write operation count.
    pub fn write_count(&self) -> u64 {
        *self.write_count.read().unwrap()
    }

    /// Clear all data (for test reset).
    pub fn clear(&self) {
        self.events.write().unwrap().clear();
        self.cursors.write().unwrap().clear();
        self.subscribers.write().unwrap().clear();
        *self.write_count.write().unwrap() = 0;
    }

    /// Snapshot current state for comparison.
    pub fn snapshot(&self) -> HashMap<(String, String), Vec<Event>> {
        self.events.read().unwrap().clone()
    }

    /// Check if this executor has the same events as another.
    pub fn matches(&self, other: &SimExecutor) -> bool {
        let self_events = self.events.read().unwrap();
        let other_events = other.events.read().unwrap();

        if self_events.len() != other_events.len() {
            return false;
        }

        for (key, events) in self_events.iter() {
            match other_events.get(key) {
                Some(other_evts) if events.len() == other_evts.len() => {
                    // Compare event IDs (ordering matters)
                    for (e1, e2) in events.iter().zip(other_evts.iter()) {
                        if e1.id != e2.id {
                            return false;
                        }
                    }
                }
                _ => return false,
            }
        }

        true
    }

    /// Get differences between this executor and another.
    pub fn diff(&self, other: &SimExecutor) -> ExecutorDiff {
        let self_events = self.events.read().unwrap();
        let other_events = other.events.read().unwrap();

        let mut only_in_self = Vec::new();
        let mut only_in_other = Vec::new();
        let mut different_order = Vec::new();

        // Find events only in self or with different order
        for (key, events) in self_events.iter() {
            match other_events.get(key) {
                Some(other_evts) => {
                    let self_ids: Vec<_> = events.iter().map(|e| &e.id).collect();
                    let other_ids: Vec<_> = other_evts.iter().map(|e| &e.id).collect();
                    if self_ids != other_ids {
                        different_order.push(key.clone());
                    }
                }
                None => {
                    only_in_self.push(key.clone());
                }
            }
        }

        // Find events only in other
        for key in other_events.keys() {
            if !self_events.contains_key(key) {
                only_in_other.push(key.clone());
            }
        }

        ExecutorDiff {
            only_in_self,
            only_in_other,
            different_order,
        }
    }
}

/// Differences between two executors.
#[derive(Debug)]
pub struct ExecutorDiff {
    /// Aggregates only in the first executor.
    pub only_in_self: Vec<(String, String)>,
    /// Aggregates only in the second executor.
    pub only_in_other: Vec<(String, String)>,
    /// Aggregates with different event ordering.
    pub different_order: Vec<(String, String)>,
}

impl ExecutorDiff {
    /// Check if executors are identical.
    pub fn is_empty(&self) -> bool {
        self.only_in_self.is_empty()
            && self.only_in_other.is_empty()
            && self.different_order.is_empty()
    }
}

impl Default for SimExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Executor for SimExecutor {
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        let mut store = self.events.write().unwrap();
        *self.write_count.write().unwrap() += 1;

        for event in events {
            let key = (event.aggregator_type.clone(), event.aggregator_id.clone());
            store.entry(key).or_default().push(event);
        }

        Ok(())
    }

    async fn read(
        &self,
        aggregators: Option<Vec<ReadAggregator>>,
        _routing_key: Option<RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<Event>> {
        let store = self.events.read().unwrap();

        let mut all_events: Vec<Event> = if let Some(aggs) = aggregators {
            aggs.iter()
                .flat_map(|agg| {
                    if let Some(agg_id) = &agg.aggregator_id {
                        store
                            .get(&(agg.aggregator_type.clone(), agg_id.clone()))
                            .cloned()
                            .unwrap_or_default()
                    } else {
                        // No aggregator_id specified, return all events for this type
                        store
                            .iter()
                            .filter(|((t, _), _)| t == &agg.aggregator_type)
                            .flat_map(|(_, events)| events.clone())
                            .collect()
                    }
                })
                .collect()
        } else {
            store.values().flatten().cloned().collect()
        };

        // Sort by version
        all_events.sort_by_key(|e| e.version);

        // Apply limit
        let limit = args.first.or(args.last).unwrap_or(100) as usize;
        let events: Vec<_> = all_events.into_iter().take(limit).collect();

        let edges = events
            .into_iter()
            .map(|e| Edge {
                cursor: Value(e.id.to_string()),
                node: e,
            })
            .collect();

        Ok(ReadResult {
            edges,
            page_info: PageInfo::default(),
        })
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        Ok(self.cursors.read().unwrap().get(&key).cloned())
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        Ok(self
            .subscribers
            .read()
            .unwrap()
            .get(&key)
            .map(|id| *id == worker_id)
            .unwrap_or(false))
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        self.subscribers.write().unwrap().insert(key, worker_id);
        Ok(())
    }

    async fn acknowledge(&self, key: String, cursor: Value, _lag: u64) -> anyhow::Result<()> {
        self.cursors.write().unwrap().insert(key, cursor);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(agg_type: &str, agg_id: &str, version: u16) -> Event {
        Event {
            id: Ulid::new(),
            name: "TestEvent".to_string(),
            aggregator_type: agg_type.to_string(),
            aggregator_id: agg_id.to_string(),
            version,
            routing_key: None,
            data: vec![],
            metadata: vec![],
            timestamp: 0,
            timestamp_subsec: 0,
        }
    }

    #[tokio::test]
    async fn test_write_and_read() {
        let executor = SimExecutor::new();

        let events = vec![make_event("Order", "123", 1), make_event("Order", "123", 2)];

        executor.write(events).await.unwrap();

        assert_eq!(executor.event_count(), 2);
        assert_eq!(executor.write_count(), 1);

        let stored = executor.get_events("Order", "123");
        assert_eq!(stored.len(), 2);
    }

    #[tokio::test]
    async fn test_multiple_aggregates() {
        let executor = SimExecutor::new();

        executor
            .write(vec![make_event("Order", "1", 1)])
            .await
            .unwrap();
        executor
            .write(vec![make_event("Order", "2", 1)])
            .await
            .unwrap();
        executor
            .write(vec![make_event("User", "1", 1)])
            .await
            .unwrap();

        assert_eq!(executor.event_count(), 3);
        assert_eq!(executor.aggregate_count(), 3);
    }

    #[tokio::test]
    async fn test_clear() {
        let executor = SimExecutor::new();

        executor
            .write(vec![make_event("Order", "1", 1)])
            .await
            .unwrap();
        assert_eq!(executor.event_count(), 1);

        executor.clear();
        assert_eq!(executor.event_count(), 0);
        assert_eq!(executor.write_count(), 0);
    }

    #[tokio::test]
    async fn test_matches() {
        let exec1 = SimExecutor::new();
        let exec2 = SimExecutor::new();

        // Both empty
        assert!(exec1.matches(&exec2));

        // Same events - use the same event instance
        let event1 = make_event("Order", "1", 1);
        exec1.write(vec![event1.clone()]).await.unwrap();
        exec2.write(vec![event1]).await.unwrap();
        assert!(exec1.matches(&exec2));

        // Different events
        exec1
            .write(vec![make_event("Order", "1", 2)])
            .await
            .unwrap();
        assert!(!exec1.matches(&exec2));
    }

    #[tokio::test]
    async fn test_diff() {
        let exec1 = SimExecutor::new();
        let exec2 = SimExecutor::new();

        exec1
            .write(vec![make_event("Order", "1", 1)])
            .await
            .unwrap();
        exec2
            .write(vec![make_event("Order", "2", 1)])
            .await
            .unwrap();

        let diff = exec1.diff(&exec2);
        assert!(!diff.is_empty());
        assert!(diff
            .only_in_self
            .contains(&("Order".to_string(), "1".to_string())));
        assert!(diff
            .only_in_other
            .contains(&("Order".to_string(), "2".to_string())));
    }

    #[tokio::test]
    async fn test_snapshot() {
        let executor = SimExecutor::new();

        executor
            .write(vec![make_event("Order", "1", 1)])
            .await
            .unwrap();

        let snapshot = executor.snapshot();
        assert_eq!(snapshot.len(), 1);

        // Modify original
        executor
            .write(vec![make_event("Order", "2", 1)])
            .await
            .unwrap();

        // Snapshot unchanged
        assert_eq!(snapshot.len(), 1);
    }
}
