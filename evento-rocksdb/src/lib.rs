//! RocksDB embedded key-value store implementation for evento.
//!
//! This crate provides an [`Executor`] implementation using [RocksDB](https://crates.io/crates/rocksdb),
//! Facebook's high-performance embedded key-value storage engine.
//!
//! # Features
//!
//! - **Embedded storage** - No external database server required
//! - **LSM-tree based** - Optimized for write-heavy workloads
//! - **Column families** - Efficient data organization with separate column families
//! - **Atomic writes** - WriteBatch for transactional semantics
//! - **Compression** - Built-in support for Snappy, LZ4, Zstd, Zlib, and Bzip2
//!
//! # Example
//!
//! ```rust,ignore
//! use evento_rocksdb::Rocks;
//! use evento_core::{Executor, metadata::Metadata, cursor::Args, ReadAggregator};
//!
//! // Define events using an enum
//! #[evento::aggregator]
//! pub enum User {
//!     UserCreated { name: String },
//! }
//!
//! // Open the database
//! let executor = Rocks::open("./my-events")?;
//!
//! // Create events
//! let id = evento::create()
//!     .event(&UserCreated { name: "Alice".into() })?
//!     .metadata(&Metadata::default())?
//!     .commit(&executor)
//!     .await?;
//!
//! // Query events
//! let events = executor.read(
//!     Some(vec![ReadAggregator::id("user/User", &id)]),
//!     None,
//!     Args::forward(10, None),
//! ).await?;
//! ```
//!
//! # Data Model
//!
//! Events are stored across multiple column families for efficient querying:
//!
//! - `events` - Primary storage: `ULID -> Event`
//! - `agg_index` - Aggregate index: `{type}\0{id}\0{version}` -> `ULID`
//! - `routing_index` - Routing key index: `{routing_key}\0{ULID}` -> `()`
//! - `type_index` - Event type index: `{type}\0{name}\0{ULID}` -> `()`
//! - `subscribers` - Subscription state: `{key}` -> `SubscriberState`

use std::path::Path;
use std::sync::Arc;

use evento_core::{
    cursor::{Args, Cursor, ReadResult, Value},
    projection::RoutingKey,
    Event, Executor, ReadAggregator, WriteError,
};
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode, MultiThreaded,
    Options, WriteBatch,
};
use ulid::Ulid;

/// Column family names
const CF_EVENTS: &str = "events";
const CF_AGG_INDEX: &str = "agg_index";
const CF_ROUTING_INDEX: &str = "routing_index";
const CF_TYPE_INDEX: &str = "type_index";
const CF_SUBSCRIBERS: &str = "subscribers";

/// Subscriber state stored in the database.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct SubscriberState {
    worker_id: String,
    cursor: Option<String>,
    lag: u64,
}

/// Stored event in RocksDB format.
#[derive(Debug, Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct StoredEvent {
    id: String,
    aggregator_id: String,
    aggregator_type: String,
    version: u16,
    name: String,
    routing_key: Option<String>,
    data: Vec<u8>,
    metadata: Vec<u8>,
    timestamp: u64,
    timestamp_subsec: u32,
}

impl From<&Event> for StoredEvent {
    fn from(event: &Event) -> Self {
        Self {
            id: event.id.to_string(),
            aggregator_id: event.aggregator_id.clone(),
            aggregator_type: event.aggregator_type.clone(),
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
            aggregator_id: stored.aggregator_id,
            aggregator_type: stored.aggregator_type,
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

/// RocksDB-based event store executor.
///
/// Implements the [`Executor`] trait using RocksDB for embedded storage.
/// Events are stored with secondary indexes in separate column families
/// for efficient querying by aggregate, routing key, and event type.
///
/// # Example
///
/// ```rust,ignore
/// use evento_rocksdb::Rocks;
///
/// // Open with default options
/// let executor = Rocks::open("./events.db")?;
///
/// // Or with custom configuration
/// let mut opts = rocksdb::Options::default();
/// opts.set_max_write_buffer_number(4);
/// opts.set_write_buffer_size(64 * 1024 * 1024);
/// let executor = Rocks::open_with_opts("./events.db", opts)?;
/// ```
pub struct Rocks {
    db: Arc<DBWithThreadMode<MultiThreaded>>,
}

impl Clone for Rocks {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
        }
    }
}

impl Rocks {
    /// Opens a RocksDB database at the specified path with default options.
    ///
    /// Creates the database directory if it doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened or column families
    /// cannot be created.
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let opts = Options::default();
        Self::open_with_opts(path, opts)
    }

    /// Opens a RocksDB database with custom options.
    ///
    /// Use this when you need to configure RocksDB settings like
    /// write buffer size, compression, or cache settings.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut opts = rocksdb::Options::default();
    /// opts.set_max_write_buffer_number(4);
    /// opts.set_write_buffer_size(128 * 1024 * 1024);
    /// opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    ///
    /// let executor = Rocks::open_with_opts("./events.db", opts)?;
    /// ```
    pub fn open_with_opts(path: impl AsRef<Path>, mut opts: Options) -> anyhow::Result<Self> {
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_names = [
            CF_EVENTS,
            CF_AGG_INDEX,
            CF_ROUTING_INDEX,
            CF_TYPE_INDEX,
            CF_SUBSCRIBERS,
        ];
        let cfs: Vec<_> = cf_names
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
            .collect();

        let db = DBWithThreadMode::<MultiThreaded>::open_cf_descriptors(&opts, path, cfs)?;
        Ok(Self { db: Arc::new(db) })
    }

    /// Returns a reference to the underlying RocksDB instance.
    pub fn db(&self) -> &DBWithThreadMode<MultiThreaded> {
        &self.db
    }

    /// Gets a column family handle by name.
    fn cf(&self, name: &str) -> Arc<BoundColumnFamily<'_>> {
        self.db.cf_handle(name).expect("Column family must exist")
    }

    /// Builds the aggregate index key.
    fn agg_key(aggregator_type: &str, aggregator_id: &str, version: u16) -> Vec<u8> {
        let mut key = format!("{}\x00{}\x00", aggregator_type, aggregator_id).into_bytes();
        key.extend_from_slice(&version.to_be_bytes());
        key
    }

    /// Builds the aggregate index prefix (without version).
    fn agg_prefix(aggregator_type: &str, aggregator_id: &str) -> Vec<u8> {
        format!("{}\x00{}\x00", aggregator_type, aggregator_id).into_bytes()
    }

    /// Builds the type index key.
    fn type_key(aggregator_type: &str, name: &str, id: &Ulid) -> Vec<u8> {
        let mut key = format!("{}\x00{}\x00", aggregator_type, name).into_bytes();
        key.extend_from_slice(&id.to_bytes());
        key
    }

    /// Builds the type index prefix.
    fn type_prefix(aggregator_type: &str, name: &str) -> Vec<u8> {
        format!("{}\x00{}\x00", aggregator_type, name).into_bytes()
    }

    /// Builds the routing index key.
    fn routing_key_index(routing_key: &str, id: &Ulid) -> Vec<u8> {
        let mut key = format!("{}\x00", routing_key).into_bytes();
        key.extend_from_slice(&id.to_bytes());
        key
    }

    /// Builds the routing index prefix.
    fn routing_prefix(routing_key: &str) -> Vec<u8> {
        format!("{}\x00", routing_key).into_bytes()
    }

    /// Gets the last version for an aggregate.
    fn get_last_version(
        &self,
        aggregator_type: &str,
        aggregator_id: &str,
    ) -> anyhow::Result<Option<u16>> {
        let prefix = Self::agg_prefix(aggregator_type, aggregator_id);
        let cf = self.cf(CF_AGG_INDEX);

        let mut last_version = None;

        let iter = self.db.iterator_cf(
            &cf,
            IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );

        for item in iter {
            let (key, _) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            if key.len() >= 2 {
                let version_bytes: [u8; 2] = key[key.len() - 2..].try_into().unwrap();
                last_version = Some(u16::from_be_bytes(version_bytes));
            }
        }

        Ok(last_version)
    }

    /// Loads an event by its ULID.
    fn load_event(&self, id: &Ulid) -> anyhow::Result<Option<Event>> {
        let cf = self.cf(CF_EVENTS);
        match self.db.get_cf(&cf, id.to_bytes())? {
            Some(bytes) => {
                let stored: StoredEvent =
                    rkyv::from_bytes::<StoredEvent, rkyv::rancor::Error>(&bytes)
                        .map_err(|e| anyhow::anyhow!("Failed to deserialize event: {}", e))?;
                Ok(Some(stored.try_into()?))
            }
            None => Ok(None),
        }
    }

    /// Collects event IDs matching the given filters.
    fn collect_event_ids(
        &self,
        aggregators: &Option<Vec<ReadAggregator>>,
        routing_key: &Option<RoutingKey>,
    ) -> anyhow::Result<Vec<Ulid>> {
        use std::collections::HashSet;
        let mut event_ids_set = HashSet::new();
        let mut event_ids = Vec::new();

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
                    match (&agg.aggregator_id, &agg.name) {
                        // Specific aggregate ID with event name filter
                        (Some(id), Some(name)) => {
                            let prefix = Self::agg_prefix(&agg.aggregator_type, id);
                            let cf = self.cf(CF_AGG_INDEX);
                            let iter = self.db.iterator_cf(
                                &cf,
                                IteratorMode::From(&prefix, rocksdb::Direction::Forward),
                            );

                            for item in iter {
                                let (key, value) = item?;
                                if !key.starts_with(&prefix) {
                                    break;
                                }
                                let ulid_bytes: [u8; 16] = value.as_ref().try_into()?;
                                let ulid = Ulid::from_bytes(ulid_bytes);

                                if let Some(event) = self.load_event(&ulid)? {
                                    if &event.name == name {
                                        add_unique!(ulid);
                                    }
                                }
                            }
                        }
                        // Specific aggregate ID, all events
                        (Some(id), None) => {
                            let prefix = Self::agg_prefix(&agg.aggregator_type, id);
                            let cf = self.cf(CF_AGG_INDEX);
                            let iter = self.db.iterator_cf(
                                &cf,
                                IteratorMode::From(&prefix, rocksdb::Direction::Forward),
                            );

                            for item in iter {
                                let (key, value) = item?;
                                if !key.starts_with(&prefix) {
                                    break;
                                }
                                let ulid_bytes: [u8; 16] = value.as_ref().try_into()?;
                                add_unique!(Ulid::from_bytes(ulid_bytes));
                            }
                        }
                        // All aggregates of type, specific event name
                        (None, Some(name)) => {
                            let prefix = Self::type_prefix(&agg.aggregator_type, name);
                            let cf = self.cf(CF_TYPE_INDEX);
                            let iter = self.db.iterator_cf(
                                &cf,
                                IteratorMode::From(&prefix, rocksdb::Direction::Forward),
                            );

                            for item in iter {
                                let (key, _) = item?;
                                if !key.starts_with(&prefix) {
                                    break;
                                }
                                if key.len() >= 16 {
                                    let ulid_bytes: [u8; 16] = key[key.len() - 16..].try_into()?;
                                    add_unique!(Ulid::from_bytes(ulid_bytes));
                                }
                            }
                        }
                        // All events of aggregator type
                        (None, None) => {
                            let prefix = format!("{}\x00", agg.aggregator_type).into_bytes();
                            let cf = self.cf(CF_AGG_INDEX);
                            let iter = self.db.iterator_cf(
                                &cf,
                                IteratorMode::From(&prefix, rocksdb::Direction::Forward),
                            );

                            for item in iter {
                                let (key, value) = item?;
                                if !key.starts_with(&prefix) {
                                    break;
                                }
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
                let cf = self.cf(CF_ROUTING_INDEX);
                let iter = self.db.iterator_cf(
                    &cf,
                    IteratorMode::From(&prefix, rocksdb::Direction::Forward),
                );

                for item in iter {
                    let (key_bytes, _) = item?;
                    if !key_bytes.starts_with(&prefix) {
                        break;
                    }
                    if key_bytes.len() >= 16 {
                        let ulid_bytes: [u8; 16] = key_bytes[key_bytes.len() - 16..].try_into()?;
                        add_unique!(Ulid::from_bytes(ulid_bytes));
                    }
                }
            }
            // Query all events
            _ => {
                let cf = self.cf(CF_EVENTS);
                let iter = self.db.iterator_cf(&cf, IteratorMode::Start);

                for item in iter {
                    let (key, _) = item?;
                    let ulid_bytes: [u8; 16] = key.as_ref().try_into()?;
                    add_unique!(Ulid::from_bytes(ulid_bytes));
                }
            }
        }

        Ok(event_ids)
    }
}

#[async_trait::async_trait]
impl Executor for Rocks {
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || {
            // Validate versions first (optimistic concurrency)
            for event in &events {
                let last_version = executor
                    .get_last_version(&event.aggregator_type, &event.aggregator_id)
                    .map_err(WriteError::Unknown)?;

                match last_version {
                    Some(v) if event.version != v + 1 => {
                        return Err(WriteError::InvalidOriginalVersion);
                    }
                    None if event.version != 1 => {
                        return Err(WriteError::InvalidOriginalVersion);
                    }
                    _ => {}
                }
            }

            // Write atomically using WriteBatch
            let mut batch = WriteBatch::default();

            for event in &events {
                let id_bytes = event.id.to_bytes();
                let stored = StoredEvent::from(event);
                let event_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&stored)
                    .map_err(|e| WriteError::RkyvEncode(e.to_string()))?;

                // Primary: ULID -> Event
                batch.put_cf(&executor.cf(CF_EVENTS), id_bytes, event_bytes.as_slice());

                // Aggregate index: {type}\0{id}\0{version} -> ULID
                let agg_key =
                    Rocks::agg_key(&event.aggregator_type, &event.aggregator_id, event.version);
                batch.put_cf(&executor.cf(CF_AGG_INDEX), agg_key, id_bytes);

                // Type index: {type}\0{name}\0{ULID} -> ()
                let type_key = Rocks::type_key(&event.aggregator_type, &event.name, &event.id);
                batch.put_cf(&executor.cf(CF_TYPE_INDEX), type_key, []);

                // Routing index (if routing key exists): {routing}\0{ULID} -> ()
                if let Some(ref routing_key) = event.routing_key {
                    let routing_key = Rocks::routing_key_index(routing_key, &event.id);
                    batch.put_cf(&executor.cf(CF_ROUTING_INDEX), routing_key, []);
                }
            }

            executor
                .db
                .write(batch)
                .map_err(|e| WriteError::Unknown(e.into()))?;

            Ok(())
        })
        .await
        .map_err(|e| WriteError::Unknown(e.into()))?
    }

    async fn read(
        &self,
        aggregators: Option<Vec<ReadAggregator>>,
        routing_key: Option<RoutingKey>,
        args: Args,
    ) -> anyhow::Result<ReadResult<Event>> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || {
            let is_backward = args.is_backward();
            let (limit, cursor) = args.get_info();

            // Collect matching event IDs
            let mut event_ids = executor.collect_event_ids(&aggregators, &routing_key)?;

            // Sort by ULID (time-ordered)
            event_ids.sort();
            if is_backward {
                event_ids.reverse();
            }

            // Apply cursor filter
            if let Some(ref cursor_value) = cursor {
                let cursor_data = Event::deserialize_cursor(cursor_value)?;
                let cursor_ulid = Ulid::from_string(&cursor_data.i)?;

                event_ids.retain(|id| {
                    if is_backward {
                        *id < cursor_ulid
                    } else {
                        *id > cursor_ulid
                    }
                });
            }

            // Load events until we have enough
            let target_count = (limit + 1) as usize;
            let mut events = Vec::new();

            for id in event_ids {
                if events.len() >= target_count {
                    break;
                }

                if let Some(event) = executor.load_event(&id)? {
                    // Apply routing key filter if specified
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

            // Build paginated result
            evento_core::cursor::Reader::new(events)
                .args(args)
                .execute()
                .map_err(|e| anyhow::anyhow!("{}", e))
        })
        .await?
    }

    async fn get_subscriber_cursor(&self, key: String) -> anyhow::Result<Option<Value>> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || {
            let cf = executor.cf(CF_SUBSCRIBERS);
            match executor.db.get_cf(&cf, &key)? {
                Some(bytes) => {
                    let state: SubscriberState =
                        rkyv::from_bytes::<SubscriberState, rkyv::rancor::Error>(&bytes).map_err(
                            |e| anyhow::anyhow!("Failed to deserialize subscriber: {}", e),
                        )?;
                    Ok(state.cursor.map(Value))
                }
                None => Ok(None),
            }
        })
        .await?
    }

    async fn is_subscriber_running(&self, key: String, worker_id: Ulid) -> anyhow::Result<bool> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || {
            let cf = executor.cf(CF_SUBSCRIBERS);
            match executor.db.get_cf(&cf, &key)? {
                Some(bytes) => {
                    let state: SubscriberState =
                        rkyv::from_bytes::<SubscriberState, rkyv::rancor::Error>(&bytes).map_err(
                            |e| anyhow::anyhow!("Failed to deserialize subscriber: {}", e),
                        )?;
                    Ok(state.worker_id == worker_id.to_string())
                }
                None => Ok(false),
            }
        })
        .await?
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || {
            let cf = executor.cf(CF_SUBSCRIBERS);

            // Try to preserve existing cursor if subscriber exists
            let cursor = match executor.db.get_cf(&cf, &key)? {
                Some(bytes) => {
                    let state: SubscriberState =
                        rkyv::from_bytes::<SubscriberState, rkyv::rancor::Error>(&bytes).map_err(
                            |e| anyhow::anyhow!("Failed to deserialize subscriber: {}", e),
                        )?;
                    state.cursor
                }
                None => None,
            };

            let state = SubscriberState {
                worker_id: worker_id.to_string(),
                cursor,
                lag: 0,
            };

            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&state)
                .map_err(|e| anyhow::anyhow!("Failed to serialize subscriber: {}", e))?;

            executor.db.put_cf(&cf, &key, bytes.as_slice())?;
            Ok(())
        })
        .await?
    }

    async fn acknowledge(&self, key: String, cursor: Value, lag: u64) -> anyhow::Result<()> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || {
            let cf = executor.cf(CF_SUBSCRIBERS);

            let state = match executor.db.get_cf(&cf, &key)? {
                Some(bytes) => {
                    let mut state: SubscriberState =
                        rkyv::from_bytes::<SubscriberState, rkyv::rancor::Error>(&bytes).map_err(
                            |e| anyhow::anyhow!("Failed to deserialize subscriber: {}", e),
                        )?;
                    state.cursor = Some(cursor.0);
                    state.lag = lag;
                    state
                }
                None => anyhow::bail!("Subscriber not found: {}", key),
            };

            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&state)
                .map_err(|e| anyhow::anyhow!("Failed to serialize subscriber: {}", e))?;

            executor.db.put_cf(&cf, &key, bytes.as_slice())?;
            Ok(())
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn create_test_event(aggregator_id: &str, version: u16, name: &str) -> Event {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        Event {
            id: Ulid::new(),
            aggregator_id: aggregator_id.to_string(),
            aggregator_type: "test/Account".to_string(),
            version,
            name: name.to_string(),
            routing_key: Some("test-routing".to_string()),
            data: vec![1, 2, 3],
            metadata: vec![4, 5, 6],
            timestamp: now.as_secs(),
            timestamp_subsec: now.subsec_millis(),
        }
    }

    #[tokio::test]
    async fn test_write_and_read_events() {
        let temp_dir = tempfile::tempdir().unwrap();
        let executor = Rocks::open(temp_dir.path()).unwrap();

        let event1 = create_test_event("agg-1", 1, "Created");
        let event2 = create_test_event("agg-1", 2, "Updated");

        // Write events
        executor.write(vec![event1.clone()]).await.unwrap();
        executor.write(vec![event2.clone()]).await.unwrap();

        // Read all events
        let result = executor
            .read(
                Some(vec![ReadAggregator::id("test/Account", "agg-1")]),
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
        let executor = Rocks::open(temp_dir.path()).unwrap();

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
        let executor = Rocks::open(temp_dir.path()).unwrap();

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
    async fn test_routing_key_query() {
        let temp_dir = tempfile::tempdir().unwrap();
        let executor = Rocks::open(temp_dir.path()).unwrap();

        let mut event1 = create_test_event("agg-1", 1, "Created");
        event1.routing_key = Some("routing-a".to_string());

        let mut event2 = create_test_event("agg-2", 1, "Created");
        event2.routing_key = Some("routing-b".to_string());

        executor.write(vec![event1]).await.unwrap();
        executor.write(vec![event2]).await.unwrap();

        // Query by routing key
        let result = executor
            .read(
                None,
                Some(RoutingKey::Value(Some("routing-a".to_string()))),
                Args::forward(10, None),
            )
            .await
            .unwrap();

        assert_eq!(result.edges.len(), 1);
        assert_eq!(result.edges[0].node.aggregator_id, "agg-1");
    }
}
