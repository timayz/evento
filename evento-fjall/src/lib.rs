//! Fjall embedded key-value store implementation for evento.
//!
//! This crate provides an [`Executor`] implementation using [fjall](https://crates.io/crates/fjall),
//! an LSM-tree based embedded key-value storage engine.
//!
//! # Features
//!
//! - **Embedded storage** - No external database server required
//! - **LSM-tree based** - Optimized for write-heavy workloads
//! - **Atomic writes** - Cross-partition transactional semantics
//! - **Efficient range scans** - Fast prefix and range queries
//!
//! # Example
//!
//! ```rust,ignore
//! use evento_fjall::Fjall;
//! use evento_core::{Executor, metadata::Metadata, cursor::Args, EventFilter};
//!
//! // Define events using an enum
//! #[evento::aggregate]
//! pub enum User {
//!     UserCreated { name: String },
//! }
//!
//! // Open the database
//! let executor = Fjall::open("./my-events")?;
//!
//! // Create events
//! let id = evento::create()
//!     .event(&UserCreated { name: "Alice".into() })
//!     .metadata(&Metadata::default())
//!     .commit(&executor)
//!     .await?;
//!
//! // Query events
//! let events = executor.read(
//!     Some(vec![EventFilter::by_id("user/User", &id)]),
//!     None,
//!     Args::forward(10, None),
//! ).await?;
//! ```
//!
//! # Data Model
//!
//! Events are stored across multiple partitions for efficient querying:
//!
//! - `events` - Primary storage: `ULID -> Event`
//! - `agg_index` - Aggregate index: `{type}\0{id}\0{version}` -> `ULID`
//! - `agg_name_index` - Aggregate-name index: `{type}\0{id}\0{name}\0{ULID}` -> `()`
//! - `routing_index` - Routing key index: `{routing_key}\0{ULID}` -> `()`
//! - `type_index` - Event type index: `{type}\0{name}\0{ULID}` -> `()`
//! - `subscribers` - Subscription state: `{key}` -> `SubscriberState`
//! - `snapshots` - Aggregate snapshots: `{type}\0{id}` -> `StoredSnapshot`

use std::path::Path;
use std::sync::{Arc, Mutex};

use evento_core::{
    cursor::{Args, ReadResult, Value},
    metadata::Metadata,
    Event, EventFilter, Executor, RoutingKey, WriteError,
};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use ulid::Ulid;

/// Subscriber state stored in the database.
#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
struct SubscriberState {
    worker_id: String,
    cursor: Option<String>,
    lag: u64,
}

/// Snapshot record stored in the database.
#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
struct StoredSnapshot {
    revision: String,
    data: Vec<u8>,
    cursor: String,
}

/// Stored event in fjall format.
#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
struct StoredEvent {
    id: String,
    aggregate_id: String,
    aggregate_type: String,
    version: u16,
    name: String,
    routing_key: Option<String>,
    data: Vec<u8>,
    metadata: Metadata,
    timestamp: u64,
    timestamp_subsec: u32,
}

impl From<&Event> for StoredEvent {
    fn from(event: &Event) -> Self {
        Self {
            id: event.id.to_string(),
            aggregate_id: event.aggregate_id.clone(),
            aggregate_type: event.aggregate_type.clone(),
            version: event.version,
            name: event.name.clone(),
            routing_key: event.routing_key.clone(),
            data: event.data.clone(),
            metadata: event.metadata.clone(),
            timestamp: event.timestamp,
            timestamp_subsec: event.timestamp_subsec,
        }
    }
}

impl TryFrom<StoredEvent> for Event {
    type Error = ulid::DecodeError;

    fn try_from(stored: StoredEvent) -> Result<Self, Self::Error> {
        Ok(Self {
            id: Ulid::from_string(&stored.id)?,
            aggregate_id: stored.aggregate_id,
            aggregate_type: stored.aggregate_type,
            version: stored.version,
            name: stored.name,
            routing_key: stored.routing_key,
            data: stored.data,
            metadata: stored.metadata,
            timestamp: stored.timestamp,
            timestamp_subsec: stored.timestamp_subsec,
        })
    }
}

/// Fjall-based event store executor.
///
/// Implements the [`Executor`] trait using fjall for embedded storage.
/// Events are stored in an LSM-tree structure with secondary indexes
/// for efficient querying by aggregate, routing key, and event type.
///
/// # Example
///
/// ```rust,ignore
/// use evento_fjall::Fjall;
///
/// // Open with default options
/// let executor = Fjall::open("./events.db")?;
///
/// // Or with custom configuration
/// let db = fjall::Database::builder("./events.db")
///     .open()?;
/// let executor = Fjall::from_database(db)?;
/// ```
pub struct Fjall {
    db: Database,
    events: Keyspace,
    agg_index: Keyspace,
    agg_name_index: Keyspace,
    routing_index: Keyspace,
    type_index: Keyspace,
    subscribers: Keyspace,
    snapshots: Keyspace,
    /// Serializes the read-validate-write critical section of `write` so that
    /// concurrent appends cannot both pass the optimistic version check (the
    /// version is read non-atomically before the batch commits).
    write_lock: Arc<Mutex<()>>,
}

impl Clone for Fjall {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            events: self.events.clone(),
            agg_index: self.agg_index.clone(),
            agg_name_index: self.agg_name_index.clone(),
            routing_index: self.routing_index.clone(),
            type_index: self.type_index.clone(),
            subscribers: self.subscribers.clone(),
            snapshots: self.snapshots.clone(),
            write_lock: self.write_lock.clone(),
        }
    }
}

impl Fjall {
    /// Opens a new fjall database at the specified path.
    ///
    /// Creates the database directory if it doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened or keyspaces
    /// cannot be created.
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = Database::builder(path).open()?;
        Self::from_database(db)
    }

    /// Creates an executor from an existing database.
    ///
    /// Use this when you need custom database configuration.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let db = fjall::Database::builder("./events.db")
    ///     .open()?;
    /// let executor = Fjall::from_database(db)?;
    /// ```
    pub fn from_database(db: Database) -> anyhow::Result<Self> {
        Ok(Self {
            events: db.keyspace("events", KeyspaceCreateOptions::default)?,
            agg_index: db.keyspace("agg_index", KeyspaceCreateOptions::default)?,
            agg_name_index: db.keyspace("agg_name_index", KeyspaceCreateOptions::default)?,
            routing_index: db.keyspace("routing_index", KeyspaceCreateOptions::default)?,
            type_index: db.keyspace("type_index", KeyspaceCreateOptions::default)?,
            subscribers: db.keyspace("subscribers", KeyspaceCreateOptions::default)?,
            snapshots: db.keyspace("snapshots", KeyspaceCreateOptions::default)?,
            write_lock: Arc::new(Mutex::new(())),
            db,
        })
    }

    /// Returns a reference to the underlying database.
    pub fn database(&self) -> &Database {
        &self.db
    }

    /// Persists all pending writes to disk.
    ///
    /// By default, writes are persisted after each batch. Call this
    /// if you need to ensure durability at a specific point.
    pub fn persist(&self) -> anyhow::Result<()> {
        self.db.persist(PersistMode::SyncAll)?;
        Ok(())
    }

    /// Builds the aggregate index key.
    fn agg_key(aggregate_type: &str, aggregate_id: &str, version: u16) -> Vec<u8> {
        let mut key = format!("{}\x00{}\x00", aggregate_type, aggregate_id).into_bytes();
        key.extend_from_slice(&version.to_be_bytes());
        key
    }

    /// Builds the aggregate index prefix (without version).
    fn agg_prefix(aggregate_type: &str, aggregate_id: &str) -> String {
        format!("{}\x00{}\x00", aggregate_type, aggregate_id)
    }

    /// Builds the aggregate-name index key: `{type}\0{id}\0{name}\0{ULID}`.
    fn agg_name_key(aggregate_type: &str, aggregate_id: &str, name: &str, id: &Ulid) -> Vec<u8> {
        let mut key =
            format!("{}\x00{}\x00{}\x00", aggregate_type, aggregate_id, name).into_bytes();
        key.extend_from_slice(&id.to_bytes());
        key
    }

    /// Builds the aggregate-name index prefix: `{type}\0{id}\0{name}\0`.
    fn agg_name_prefix(aggregate_type: &str, aggregate_id: &str, name: &str) -> String {
        format!("{}\x00{}\x00{}\x00", aggregate_type, aggregate_id, name)
    }

    /// Builds the type index key.
    fn type_key(aggregate_type: &str, name: &str, id: &Ulid) -> Vec<u8> {
        let mut key = format!("{}\x00{}\x00", aggregate_type, name).into_bytes();
        key.extend_from_slice(&id.to_bytes());
        key
    }

    /// Builds the type index prefix.
    fn type_prefix(aggregate_type: &str, name: &str) -> String {
        format!("{}\x00{}\x00", aggregate_type, name)
    }

    /// Builds the routing index key.
    fn routing_key(routing_key: &str, id: &Ulid) -> Vec<u8> {
        let mut key = format!("{}\x00", routing_key).into_bytes();
        key.extend_from_slice(&id.to_bytes());
        key
    }

    /// Builds the routing index prefix.
    fn routing_prefix(routing_key: &str) -> String {
        format!("{}\x00", routing_key)
    }

    /// Builds the snapshot key.
    fn snapshot_key(aggregate_type: &str, id: &str) -> Vec<u8> {
        format!("{}\x00{}", aggregate_type, id).into_bytes()
    }

    /// Gets the last version for an aggregate.
    fn get_last_version(
        &self,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> anyhow::Result<Option<u16>> {
        let prefix = Self::agg_prefix(aggregate_type, aggregate_id);

        if let Some(guard) = self.agg_index.prefix(&prefix).next_back() {
            let (key, _) = guard.into_inner()?;
            // Version is the last 2 bytes of the key
            let key_bytes = key.as_ref();
            if key_bytes.len() >= 2 {
                let version_bytes: [u8; 2] = key_bytes[key_bytes.len() - 2..].try_into().unwrap();
                return Ok(Some(u16::from_be_bytes(version_bytes)));
            }
        }

        Ok(None)
    }

    /// Loads an event by its ULID.
    fn load_event(&self, id: &Ulid) -> anyhow::Result<Option<Event>> {
        match self.events.get(id.to_bytes())? {
            Some(bytes) => {
                let stored: StoredEvent = bitcode::decode(bytes.as_ref())
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize event: {}", e))?;
                Ok(Some(stored.try_into()?))
            }
            None => Ok(None),
        }
    }

    /// Collects event IDs matching the given filters.
    fn collect_event_ids(
        &self,
        aggregators: &Option<Vec<EventFilter>>,
        routing_key: &Option<RoutingKey>,
    ) -> anyhow::Result<Vec<Ulid>> {
        use std::collections::HashSet;
        let mut event_ids_set = HashSet::new();
        let mut event_ids = Vec::new();

        // Helper macro to add unique event IDs
        macro_rules! add_unique {
            ($ulid:expr) => {
                if event_ids_set.insert($ulid) {
                    event_ids.push($ulid);
                }
            };
        }

        match (aggregators, routing_key) {
            // Query by specific aggregator ID and optionally event name
            (Some(aggs), _) => {
                for agg in aggs {
                    match (&agg.aggregate_id, &agg.name) {
                        // Specific aggregate ID with event name filter.
                        // The agg_name_index stores the ULID in the key tail, so we
                        // can resolve matches by prefix scan without loading events.
                        (Some(id), Some(name)) => {
                            let prefix = Self::agg_name_prefix(&agg.aggregate_type, id, name);
                            for guard in self.agg_name_index.prefix(&prefix) {
                                let (key, _) = guard.into_inner()?;
                                let key_bytes = key.as_ref();
                                if key_bytes.len() >= 16 {
                                    let ulid_bytes: [u8; 16] =
                                        key_bytes[key_bytes.len() - 16..].try_into()?;
                                    add_unique!(Ulid::from_bytes(ulid_bytes));
                                }
                            }
                        }
                        // Specific aggregate ID, all events
                        (Some(id), None) => {
                            let prefix = Self::agg_prefix(&agg.aggregate_type, id);
                            for guard in self.agg_index.prefix(&prefix) {
                                let (_, value) = guard.into_inner()?;
                                let ulid_bytes: [u8; 16] = value.as_ref().try_into()?;
                                add_unique!(Ulid::from_bytes(ulid_bytes));
                            }
                        }
                        // All aggregates of type, specific event name
                        (None, Some(name)) => {
                            let prefix = Self::type_prefix(&agg.aggregate_type, name);
                            for guard in self.type_index.prefix(&prefix) {
                                let (key, _) = guard.into_inner()?;
                                let key_bytes = key.as_ref();
                                if key_bytes.len() >= 16 {
                                    let ulid_bytes: [u8; 16] =
                                        key_bytes[key_bytes.len() - 16..].try_into()?;
                                    add_unique!(Ulid::from_bytes(ulid_bytes));
                                }
                            }
                        }
                        // All events of aggregator type - scan all
                        (None, None) => {
                            let prefix = format!("{}\x00", agg.aggregate_type);
                            for guard in self.agg_index.prefix(&prefix) {
                                let (_, value) = guard.into_inner()?;
                                let ulid_bytes: [u8; 16] = value.as_ref().try_into()?;
                                add_unique!(Ulid::from_bytes(ulid_bytes));
                            }
                        }
                    }
                }
            }
            // Query by routing key only
            (None, Some(RoutingKey::Value(Some(ref key)))) => {
                let prefix = Self::routing_prefix(key);
                for guard in self.routing_index.prefix(&prefix) {
                    let (key, _) = guard.into_inner()?;
                    let key_bytes = key.as_ref();
                    if key_bytes.len() >= 16 {
                        let ulid_bytes: [u8; 16] = key_bytes[key_bytes.len() - 16..].try_into()?;
                        add_unique!(Ulid::from_bytes(ulid_bytes));
                    }
                }
            }
            // Query all events
            _ => {
                for guard in self.events.iter() {
                    let (key, _) = guard.into_inner()?;
                    let ulid_bytes: [u8; 16] = key.as_ref().try_into()?;
                    add_unique!(Ulid::from_bytes(ulid_bytes));
                }
            }
        }

        Ok(event_ids)
    }
}

#[async_trait::async_trait]
impl Executor for Fjall {
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || {
            // Hold the write lock across validate + commit so concurrent appends
            // cannot both observe the same "last version" and both succeed.
            let _guard = executor
                .write_lock
                .lock()
                .map_err(|_| WriteError::Unknown(anyhow::anyhow!("write lock poisoned")))?;

            // Validate versions first (optimistic concurrency). `seen` tracks the
            // version assigned earlier in THIS batch so multiple events for the same
            // aggregate (e.g. a create() committing several events) validate correctly.
            let mut seen: std::collections::HashMap<(String, String), u16> =
                std::collections::HashMap::new();
            for event in &events {
                let agg = (event.aggregate_type.clone(), event.aggregate_id.clone());
                let last_version = match seen.get(&agg) {
                    Some(v) => Some(*v),
                    None => executor
                        .get_last_version(&event.aggregate_type, &event.aggregate_id)
                        .map_err(WriteError::Unknown)?,
                };

                match last_version {
                    Some(v) if event.version != v + 1 => {
                        return Err(WriteError::InvalidOriginalVersion);
                    }
                    None if event.version != 1 => {
                        return Err(WriteError::InvalidOriginalVersion);
                    }
                    _ => {}
                }

                seen.insert(agg, event.version);
            }

            // Write atomically using batch
            let mut batch = executor.db.batch();

            for event in &events {
                let id_bytes = event.id.to_bytes();
                let stored = StoredEvent::from(event);
                let event_bytes = bitcode::encode(&stored);

                // Primary: ULID -> Event
                batch.insert(&executor.events, id_bytes, event_bytes);

                // Aggregate index: {type}\0{id}\0{version} -> ULID
                let agg_key =
                    Fjall::agg_key(&event.aggregate_type, &event.aggregate_id, event.version);
                batch.insert(&executor.agg_index, agg_key, id_bytes);

                // Aggregate-name index: {type}\0{id}\0{name}\0{ULID} -> ()
                let agg_name_key = Fjall::agg_name_key(
                    &event.aggregate_type,
                    &event.aggregate_id,
                    &event.name,
                    &event.id,
                );
                batch.insert(&executor.agg_name_index, agg_name_key, []);

                // Type index: {type}\0{name}\0{ULID} -> ()
                let type_key = Fjall::type_key(&event.aggregate_type, &event.name, &event.id);
                batch.insert(&executor.type_index, type_key, []);

                // Routing index (if routing key exists): {routing}\0{ULID} -> ()
                if let Some(ref routing_key) = event.routing_key {
                    let routing_key = Fjall::routing_key(routing_key, &event.id);
                    batch.insert(&executor.routing_index, routing_key, []);
                }
            }

            batch.commit().map_err(|e| WriteError::Unknown(e.into()))?;
            executor
                .db
                .persist(PersistMode::SyncAll)
                .map_err(|e| WriteError::Unknown(e.into()))?;

            Ok(())
        })
        .await
        .map_err(|e| WriteError::Unknown(e.into()))?
    }

    async fn read(
        &self,
        aggregators: Option<Vec<EventFilter>>,
        routing_key: Option<RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<Event>> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || {
            // Collect matching event IDs (deduplicated across the aggregator filters).
            let event_ids = executor.collect_event_ids(&aggregators, &routing_key)?;

            // Load every matching event and apply the routing-key filter. The cursor
            // is intentionally NOT pre-filtered here: `Event`'s cursor uses
            // (timestamp, subsec, version, id), which can disagree with raw ULID
            // ordering when events land in the same millisecond. `Reader::execute`
            // applies the canonical sort, cursor predicate, and limit.
            let mut events = Vec::with_capacity(event_ids.len());
            for id in event_ids {
                if let Some(event) = executor.load_event(&id)? {
                    let matches = match &routing_key {
                        Some(RoutingKey::Value(Some(ref key))) => {
                            event.routing_key.as_ref() == Some(key)
                        }
                        Some(RoutingKey::Value(None)) => event.routing_key.is_none(),
                        Some(RoutingKey::All) | None => true,
                    };

                    if matches {
                        events.push(event);
                    }
                }
            }

            evento_core::cursor::Reader::new(events)
                .args(args)
                .execute()
                .map_err(|e| anyhow::anyhow!("{}", e))
        })
        .await?
    }

    async fn latest_timestamp(
        &self,
        aggregators: Option<Vec<EventFilter>>,
        routing_key: Option<RoutingKey>,
    ) -> anyhow::Result<u64> {
        let result = self
            .read(aggregators, routing_key, Args::backward(1, None))
            .await?;
        Ok(result.edges.last().map(|e| e.node.timestamp).unwrap_or(0))
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || match executor.subscribers.get(&key)? {
            Some(bytes) => {
                let state: SubscriberState = bitcode::decode(bytes.as_ref())
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize subscriber: {}", e))?;
                Ok(state.cursor.map(Value))
            }
            None => Ok(None),
        })
        .await?
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || match executor.subscribers.get(&key)? {
            Some(bytes) => {
                let state: SubscriberState = bitcode::decode(bytes.as_ref())
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize subscriber: {}", e))?;
                Ok(state.worker_id == worker_id.to_string())
            }
            None => Ok(false),
        })
        .await?
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || {
            // Try to preserve existing cursor if subscriber exists
            let cursor = match executor.subscribers.get(&key)? {
                Some(bytes) => {
                    let state: SubscriberState = bitcode::decode(bytes.as_ref())
                        .map_err(|e| anyhow::anyhow!("Failed to deserialize subscriber: {}", e))?;
                    state.cursor
                }
                None => None,
            };

            let state = SubscriberState {
                worker_id: worker_id.to_string(),
                cursor,
                lag: 0,
            };

            executor
                .subscribers
                .insert(key.as_bytes(), bitcode::encode(&state))?;
            Ok(())
        })
        .await?
    }

    async fn acknowledge(&self, key: String, cursor: Value, lag: u64) -> anyhow::Result<()> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || {
            let state = match executor.subscribers.get(&key)? {
                Some(bytes) => {
                    let mut state: SubscriberState = bitcode::decode(bytes.as_ref())
                        .map_err(|e| anyhow::anyhow!("Failed to deserialize subscriber: {}", e))?;
                    state.cursor = Some(cursor.0);
                    state.lag = lag;
                    state
                }
                None => anyhow::bail!("Subscriber not found: {}", key),
            };

            executor
                .subscribers
                .insert(key.as_bytes(), bitcode::encode(&state))?;
            Ok(())
        })
        .await?
    }

    async fn get_snapshot(
        &self,
        aggregate_type: String,
        aggregate_revision: String,
        id: String,
    ) -> anyhow::Result<Option<(Vec<u8>, Value)>> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || {
            let key = Fjall::snapshot_key(&aggregate_type, &id);
            match executor.snapshots.get(&key)? {
                Some(bytes) => {
                    let stored: StoredSnapshot = bitcode::decode(bytes.as_ref())
                        .map_err(|e| anyhow::anyhow!("Failed to deserialize snapshot: {}", e))?;

                    // Revision mismatch invalidates the snapshot (forces a rebuild).
                    if stored.revision != aggregate_revision {
                        return Ok(None);
                    }

                    Ok(Some((stored.data, Value(stored.cursor))))
                }
                None => Ok(None),
            }
        })
        .await?
    }

    async fn save_snapshot(
        &self,
        aggregate_type: String,
        aggregate_revision: String,
        id: String,
        data: Vec<u8>,
        cursor: Value,
    ) -> anyhow::Result<()> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || {
            let key = Fjall::snapshot_key(&aggregate_type, &id);
            let stored = StoredSnapshot {
                revision: aggregate_revision,
                data,
                cursor: cursor.0,
            };

            executor.snapshots.insert(key, bitcode::encode(&stored))?;
            Ok(())
        })
        .await?
    }

    async fn delete_snapshot(&self, aggregate_type: String, id: String) -> anyhow::Result<()> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || {
            let key = Fjall::snapshot_key(&aggregate_type, &id);
            executor.snapshots.remove(key)?;
            Ok(())
        })
        .await?
    }
}

impl From<Database> for Fjall {
    fn from(db: Database) -> Self {
        Self::from_database(db).expect("Failed to create Fjall from database")
    }
}

/// Read-write executor pair for Fjall.
///
/// Used in CQRS patterns where reads and writes are routed to the same
/// embedded store; both halves share a single Fjall handle.
#[cfg(feature = "rw")]
pub type RwFjall = evento_core::Rw<Fjall, Fjall>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_test_event(aggregate_id: &str, version: u16, name: &str) -> Event {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        Event {
            id: Ulid::new(),
            aggregate_id: aggregate_id.to_string(),
            aggregate_type: "test/Account".to_string(),
            version,
            name: name.to_string(),
            routing_key: Some("test-routing".to_string()),
            data: vec![1, 2, 3],
            metadata: Metadata::default(),
            timestamp: now.as_secs(),
            timestamp_subsec: now.subsec_millis(),
        }
    }

    #[tokio::test]
    async fn test_write_and_read_events() {
        let temp_dir = tempfile::tempdir().unwrap();
        let executor = Fjall::open(temp_dir.path()).unwrap();

        let event1 = create_test_event("agg-1", 1, "Created");
        let event2 = create_test_event("agg-1", 2, "Updated");

        // Write events
        executor.write(vec![event1.clone()]).await.unwrap();
        executor.write(vec![event2.clone()]).await.unwrap();

        // Read all events
        let result = executor
            .read(
                Some(vec![EventFilter::by_id("test/Account", "agg-1")]),
                None,
                Args::forward(10, None),
            )
            .await
            .unwrap();

        assert_eq!(result.edges.len(), 2);
        assert_eq!(result.edges[0].node.version, 1);
        assert_eq!(result.edges[1].node.version, 2);
    }

    #[tokio::test]
    async fn test_version_conflict() {
        let temp_dir = tempfile::tempdir().unwrap();
        let executor = Fjall::open(temp_dir.path()).unwrap();

        let event1 = create_test_event("agg-1", 1, "Created");
        executor.write(vec![event1]).await.unwrap();

        // Try to write with wrong version
        let event2 = create_test_event("agg-1", 1, "Duplicate");
        let result = executor.write(vec![event2]).await;

        assert!(matches!(result, Err(WriteError::InvalidOriginalVersion)));
    }

    #[tokio::test]
    async fn test_subscriber_lifecycle() {
        let temp_dir = tempfile::tempdir().unwrap();
        let executor = Fjall::open(temp_dir.path()).unwrap();

        let worker_id = Ulid::new();
        let key = "test-subscriber".to_string();

        // Create subscriber
        executor
            .upsert_subscriber(key.clone(), worker_id)
            .await
            .unwrap();

        // Check if running
        assert!(executor
            .is_subscriber_running(key.clone(), worker_id)
            .await
            .unwrap());

        // Check cursor is None initially
        assert!(executor
            .get_subscriber_cursor(key.clone())
            .await
            .unwrap()
            .is_none());

        // Acknowledge with cursor
        executor
            .acknowledge(key.clone(), Value("test-cursor".to_string()), 0)
            .await
            .unwrap();

        // Check cursor is updated
        let cursor = executor.get_subscriber_cursor(key).await.unwrap();
        assert_eq!(cursor.unwrap().0, "test-cursor");
    }

    #[tokio::test]
    async fn test_snapshot_lifecycle() {
        let temp_dir = tempfile::tempdir().unwrap();
        let executor = Fjall::open(temp_dir.path()).unwrap();

        let aggregate_type = "test/Account".to_string();
        let revision = "1".to_string();
        let id = "agg-1".to_string();
        let data = vec![10, 20, 30];
        let cursor = Value("cursor-1".to_string());

        // Initially: no snapshot
        let result = executor
            .get_snapshot(aggregate_type.clone(), revision.clone(), id.clone())
            .await
            .unwrap();
        assert!(result.is_none());

        // Save snapshot
        executor
            .save_snapshot(
                aggregate_type.clone(),
                revision.clone(),
                id.clone(),
                data.clone(),
                cursor.clone(),
            )
            .await
            .unwrap();

        // Get matching revision returns the snapshot
        let (got_data, got_cursor) = executor
            .get_snapshot(aggregate_type.clone(), revision.clone(), id.clone())
            .await
            .unwrap()
            .expect("snapshot should exist");
        assert_eq!(got_data, data);
        assert_eq!(got_cursor.0, cursor.0);

        // Get with different revision returns None (revision invalidation)
        let result = executor
            .get_snapshot(aggregate_type.clone(), "2".to_string(), id.clone())
            .await
            .unwrap();
        assert!(result.is_none());

        // Overwriting with a new revision works
        let new_data = vec![40, 50];
        let new_cursor = Value("cursor-2".to_string());
        executor
            .save_snapshot(
                aggregate_type.clone(),
                "2".to_string(),
                id.clone(),
                new_data.clone(),
                new_cursor.clone(),
            )
            .await
            .unwrap();

        let (got_data, got_cursor) = executor
            .get_snapshot(aggregate_type.clone(), "2".to_string(), id.clone())
            .await
            .unwrap()
            .expect("snapshot should exist");
        assert_eq!(got_data, new_data);
        assert_eq!(got_cursor.0, new_cursor.0);

        // Delete snapshot
        executor
            .delete_snapshot(aggregate_type.clone(), id.clone())
            .await
            .unwrap();

        let result = executor
            .get_snapshot(aggregate_type, "2".to_string(), id.clone())
            .await
            .unwrap();
        assert!(result.is_none());

        // Delete is idempotent
        executor
            .delete_snapshot("test/Account".to_string(), id)
            .await
            .unwrap();
    }
}
