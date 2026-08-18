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
//! Events are stored across multiple partitions for efficient querying. Index
//! keys are built from **length-prefixed** components (`{u32 len}{bytes}` per
//! component), so caller-supplied strings can contain any byte — including
//! NUL — without one aggregate's keys colliding with or shadowing another's:
//!
//! - `events` - Primary storage: `ULID -> Event`
//! - `agg_index` - Aggregate version index: `enc(type, id) + {version BE}` -> `ULID`
//! - `subscribers` - Subscription state: `{key}` -> `SubscriberState`
//! - `snapshots` - Aggregate snapshots: `enc(type, id)` -> `StoredSnapshot`
//! - `meta` - Store metadata: the monotonic commit clock (`last_stamp`) and the
//!   on-disk index-layout version (`index_version`)
//!
//! Reads are served by **cursor-ordered** index keyspaces: each key is a filter
//! prefix followed by the 30-byte `cursor_key` = `{timestamp BE u64}{subsec BE
//! u32}{version BE u16}{ULID bytes}`, whose lexicographic order equals the
//! canonical cursor order — so a page is a seek plus `limit` steps, never a
//! whole-prefix scan:
//!
//! - `cursor_all` - `{cursor_key}` -> `()`
//! - `cursor_agg` - `enc(type, id) + {cursor_key}` -> `()`
//! - `cursor_agg_name` - `enc(type, id, name) + {cursor_key}` -> `()`
//! - `cursor_type` - `enc(type) + {cursor_key}` -> `()`
//! - `cursor_type_name` - `enc(type, name) + {cursor_key}` -> `()`
//! - `cursor_routing` - `{0x01}enc(routing_key) + {cursor_key}` (or
//!   `{0x00} + {cursor_key}` for events without a routing key) -> `()`
//!
//! Opening a database whose `index_version` predates this layout rebuilds the
//! cursor keyspaces from `events` (one O(total events) pass, then never again).
//!
//! # Ordering
//!
//! `write` re-stamps events with a **monotonic commit clock** held under the
//! write lock and persisted in the same batch, so subscription cursor order
//! always equals commit order — even across restarts and wall-clock
//! regressions. `replicate` persists caller timestamps verbatim for
//! replication layers that own ordering themselves.

use std::path::Path;
use std::sync::{Arc, Mutex};

use evento_core::{
    cursor::{Args, ReadResult, Value},
    metadata::Metadata,
    Event, EventFilter, Executor, RoutingKey, SubscriberStatus, WriteError,
};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};
use ulid::Ulid;

/// A fjall-backed [`evento_accord::Journal`] (the Accord consensus log), behind the
/// optional `accord` feature.
#[cfg(feature = "accord")]
mod accord_journal;
#[cfg(feature = "accord")]
pub use accord_journal::FjallJournal;

/// Subscriber state stored in the database.
#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
struct SubscriberState {
    worker_id: String,
    cursor: Option<String>,
    lag: u64,
    /// Kill switch: a disabled subscription reports "not running" to its
    /// worker, mirroring the SQL backend's `enabled` column.
    enabled: bool,
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
    /// Cursor-ordered index keyspaces (see the module docs): keys end with the
    /// 30-byte `cursor_key`, so a read is a seek + `limit` steps in cursor
    /// order instead of a whole-prefix scan sorted in memory.
    cursor_all: Keyspace,
    cursor_agg: Keyspace,
    cursor_agg_name: Keyspace,
    cursor_type: Keyspace,
    cursor_type_name: Keyspace,
    cursor_routing: Keyspace,
    subscribers: Keyspace,
    snapshots: Keyspace,
    meta: Keyspace,
    /// Serializes the read-validate-write critical section of `write` (so
    /// concurrent appends cannot both pass the optimistic version check) and
    /// all subscriber read-modify-write updates. Deliberately NOT held across
    /// the fsync at the end of `write` — that would serialize writers on disk
    /// latency and block `latest_timestamp` readers for the fsync duration.
    write_lock: Arc<Mutex<()>>,
    /// The monotonic commit clock in **milliseconds** since the Unix epoch:
    /// each write batch is stamped `max(now, last + 1)` (updated only while
    /// `write_lock` is held), persisted under [`LAST_STAMP_KEY`] in the same
    /// batch so a wall-clock regression across restarts cannot mint cursors
    /// below already-acknowledged ones. Atomic so `latest_timestamp` reads it
    /// lock-free.
    last_stamp: Arc<std::sync::atomic::AtomicU64>,
    /// Durability mode applied after each write batch (see
    /// [`persist_mode`](Self::persist_mode)).
    write_persist_mode: PersistMode,
    /// Notifies in-process subscriptions after each successful `write` so they
    /// wake immediately instead of waiting for their next poll tick. Carries a
    /// monotonically increasing write generation.
    write_tx: tokio::sync::watch::Sender<u64>,
}

/// `meta` keyspace key holding the monotonic commit clock (millis, BE u64).
const LAST_STAMP_KEY: &[u8] = b"last_stamp";

/// `meta` keyspace key holding the on-disk index-layout version (BE u64).
/// Absent (version 0) means the legacy ULID-ordered index layout.
const INDEX_VERSION_KEY: &[u8] = b"index_version";

/// The current index layout: cursor-ordered `cursor_*` keyspaces. Opening a
/// database stamped with a different version rebuilds them from `events`.
const INDEX_VERSION: u64 = 2;

/// Length of the cursor-ordered key suffix:
/// `{timestamp BE u64}{subsec BE u32}{version BE u16}{ULID bytes}`.
const CURSOR_KEY_LEN: usize = 8 + 4 + 2 + 16;

/// Names of the cursor-index keyspaces plus the legacy (pre-`INDEX_VERSION` 2)
/// index keyspaces — everything a rebuild deletes before re-indexing. Legacy
/// names stay listed so an upgraded database reclaims their space.
const REBUILT_KEYSPACES: &[&str] = &[
    "cursor_all",
    "cursor_agg",
    "cursor_agg_name",
    "cursor_type",
    "cursor_type_name",
    "cursor_routing",
    // Legacy ULID-ordered indexes, superseded by the cursor keyspaces.
    "agg_name_index",
    "type_index",
    "routing_index",
];

impl Clone for Fjall {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            events: self.events.clone(),
            agg_index: self.agg_index.clone(),
            cursor_all: self.cursor_all.clone(),
            cursor_agg: self.cursor_agg.clone(),
            cursor_agg_name: self.cursor_agg_name.clone(),
            cursor_type: self.cursor_type.clone(),
            cursor_type_name: self.cursor_type_name.clone(),
            cursor_routing: self.cursor_routing.clone(),
            subscribers: self.subscribers.clone(),
            snapshots: self.snapshots.clone(),
            meta: self.meta.clone(),
            write_lock: self.write_lock.clone(),
            last_stamp: self.last_stamp.clone(),
            write_persist_mode: self.write_persist_mode,
            // `watch::Sender` clones share the same channel, so all clones of
            // this executor notify the same subscription receivers on write.
            write_tx: self.write_tx.clone(),
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
    /// The first open of a database written by an older layout rebuilds the
    /// cursor-index keyspaces from `events` — one O(total events) pass, made
    /// durable before this returns; subsequent opens skip it.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let db = fjall::Database::builder("./events.db")
    ///     .open()?;
    /// let executor = Fjall::from_database(db)?;
    /// ```
    pub fn from_database(db: Database) -> anyhow::Result<Self> {
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default)?;

        // Restore the monotonic commit clock so stamps stay strictly
        // increasing across restarts even if the wall clock went backwards.
        let last_stamp = match meta.get(LAST_STAMP_KEY)? {
            Some(bytes) => {
                let bytes: [u8; 8] = bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("corrupt last_stamp meta entry"))?;
                u64::from_be_bytes(bytes)
            }
            None => 0,
        };

        let index_version = match meta.get(INDEX_VERSION_KEY)? {
            Some(bytes) => {
                let bytes: [u8; 8] = bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("corrupt index_version meta entry"))?;
                u64::from_be_bytes(bytes)
            }
            None => 0,
        };

        if index_version != INDEX_VERSION {
            // Drop stale/partial index keyspaces before re-indexing. Only our
            // own named keyspaces — the database may be shared (e.g. with a
            // `FjallJournal`'s `accord_*` keyspaces), so never wipe by
            // discovery.
            for name in REBUILT_KEYSPACES {
                if db.keyspace_exists(name) {
                    let keyspace = db.keyspace(name, KeyspaceCreateOptions::default)?;
                    db.delete_keyspace(keyspace)?;
                }
            }
        }

        let executor = Self {
            events: db.keyspace("events", KeyspaceCreateOptions::default)?,
            agg_index: db.keyspace("agg_index", KeyspaceCreateOptions::default)?,
            cursor_all: db.keyspace("cursor_all", KeyspaceCreateOptions::default)?,
            cursor_agg: db.keyspace("cursor_agg", KeyspaceCreateOptions::default)?,
            cursor_agg_name: db.keyspace("cursor_agg_name", KeyspaceCreateOptions::default)?,
            cursor_type: db.keyspace("cursor_type", KeyspaceCreateOptions::default)?,
            cursor_type_name: db.keyspace("cursor_type_name", KeyspaceCreateOptions::default)?,
            cursor_routing: db.keyspace("cursor_routing", KeyspaceCreateOptions::default)?,
            subscribers: db.keyspace("subscribers", KeyspaceCreateOptions::default)?,
            snapshots: db.keyspace("snapshots", KeyspaceCreateOptions::default)?,
            meta,
            write_lock: Arc::new(Mutex::new(())),
            last_stamp: Arc::new(std::sync::atomic::AtomicU64::new(last_stamp)),
            write_persist_mode: PersistMode::SyncAll,
            write_tx: tokio::sync::watch::channel(0).0,
            db,
        };

        if index_version != INDEX_VERSION {
            executor.rebuild_cursor_indexes()?;
            // Stamp + fsync last: a crash mid-rebuild leaves the version
            // absent, so the next open simply redoes the (idempotent) rebuild.
            executor
                .meta
                .insert(INDEX_VERSION_KEY, INDEX_VERSION.to_be_bytes())?;
            executor.db.persist(PersistMode::SyncAll)?;
        }

        Ok(executor)
    }

    /// Re-derives every cursor-index entry from the `events` keyspace, in
    /// batched (unsynced) commits; the caller persists once afterwards.
    fn rebuild_cursor_indexes(&self) -> anyhow::Result<()> {
        const REBUILD_BATCH: usize = 8_192;

        let mut batch = self.db.batch();
        let mut pending = 0usize;
        for guard in self.events.iter() {
            let (_, value) = guard.into_inner()?;
            let stored: StoredEvent = bitcode::decode(value.as_ref())
                .map_err(|e| anyhow::anyhow!("Failed to deserialize event: {}", e))?;
            let id = Ulid::from_string(&stored.id)?;
            let suffix = Self::cursor_key_suffix(
                stored.timestamp,
                stored.timestamp_subsec,
                stored.version,
                &id,
            );
            self.insert_cursor_entries(
                &mut batch,
                &stored.aggregate_type,
                &stored.aggregate_id,
                &stored.name,
                stored.routing_key.as_deref(),
                &suffix,
            );
            pending += 1;
            if pending >= REBUILD_BATCH {
                batch.commit()?;
                batch = self.db.batch();
                pending = 0;
            }
        }
        if pending > 0 {
            batch.commit()?;
        }
        Ok(())
    }

    /// Sets the durability mode applied after each write batch (default
    /// [`PersistMode::SyncAll`]).
    ///
    /// `SyncAll` fsyncs once per `write`/`replicate` call, capping throughput
    /// at the disk's fsync rate. A weaker mode (e.g. `Buffer`) trades
    /// crash-durability of the most recent writes for much higher write
    /// throughput; batch ordering and atomicity are unaffected.
    pub fn persist_mode(mut self, mode: PersistMode) -> Self {
        self.write_persist_mode = mode;
        self
    }

    /// Enables or disables a subscription (the kill switch).
    ///
    /// A disabled subscription reports "not running" to its worker, which
    /// stops without processing further events. Missing subscribers are
    /// ignored.
    pub async fn set_subscriber_enabled(&self, key: String, enabled: bool) -> anyhow::Result<()> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || {
            let _guard = executor
                .write_lock
                .lock()
                .map_err(|_| anyhow::anyhow!("write lock poisoned"))?;
            let Some(bytes) = executor.subscribers.get(&key)? else {
                return Ok(());
            };
            let mut state: SubscriberState = bitcode::decode(bytes.as_ref())
                .map_err(|e| anyhow::anyhow!("Failed to deserialize subscriber: {}", e))?;
            state.enabled = enabled;
            executor
                .subscribers
                .insert(key.as_bytes(), bitcode::encode(&state))?;
            Ok(())
        })
        .await?
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

    /// Encodes key components with a length prefix per component
    /// (`{u32 BE len}{bytes}`).
    ///
    /// Unlike a separator byte, this cannot be confused by components that
    /// themselves contain the separator: `("Ab","c")`, `("A","bc")`, and
    /// `("A","b\0c")` all encode to distinct, non-prefixing keys, and
    /// `encode_components(parts)` is a byte-prefix exactly of keys built from
    /// `parts` plus more data — the property prefix scans rely on.
    fn encode_components(parts: &[&[u8]]) -> Vec<u8> {
        let mut key = Vec::with_capacity(parts.iter().map(|p| p.len() + 4).sum());
        for part in parts {
            key.extend_from_slice(&(part.len() as u32).to_be_bytes());
            key.extend_from_slice(part);
        }
        key
    }

    /// Builds the aggregate index key.
    fn agg_key(aggregate_type: &str, aggregate_id: &str, version: u16) -> Vec<u8> {
        let mut key = Self::agg_prefix(aggregate_type, aggregate_id);
        key.extend_from_slice(&version.to_be_bytes());
        key
    }

    /// Builds the aggregate index prefix (without version).
    fn agg_prefix(aggregate_type: &str, aggregate_id: &str) -> Vec<u8> {
        Self::encode_components(&[aggregate_type.as_bytes(), aggregate_id.as_bytes()])
    }

    /// Builds the aggregate-name cursor-index prefix: `enc(type, id, name)`.
    fn agg_name_prefix(aggregate_type: &str, aggregate_id: &str, name: &str) -> Vec<u8> {
        Self::encode_components(&[
            aggregate_type.as_bytes(),
            aggregate_id.as_bytes(),
            name.as_bytes(),
        ])
    }

    /// Builds the type-name cursor-index prefix: `enc(type, name)`.
    fn type_prefix(aggregate_type: &str, name: &str) -> Vec<u8> {
        Self::encode_components(&[aggregate_type.as_bytes(), name.as_bytes()])
    }

    /// Builds the type cursor-index prefix: `enc(type)`.
    fn type_only_prefix(aggregate_type: &str) -> Vec<u8> {
        Self::encode_components(&[aggregate_type.as_bytes()])
    }

    /// Builds the routing cursor-index prefix. A discriminator byte separates
    /// keyed events (`0x01` + `enc(key)`) from events without a routing key
    /// (`0x00`), so the null range is unambiguous even against a real `""` key.
    fn routing_cursor_prefix(routing_key: Option<&str>) -> Vec<u8> {
        match routing_key {
            Some(key) => {
                let enc = Self::encode_components(&[key.as_bytes()]);
                let mut prefix = Vec::with_capacity(1 + enc.len());
                prefix.push(0x01);
                prefix.extend_from_slice(&enc);
                prefix
            }
            None => vec![0x00],
        }
    }

    /// Builds the 30-byte cursor-ordered key suffix. Its lexicographic order
    /// equals the canonical cursor order `(timestamp, subsec, version, id)` —
    /// ULID bytes compare identically to the ULID's Crockford string, which is
    /// what `Event`'s cursor predicate compares.
    fn cursor_key_suffix(
        timestamp: u64,
        timestamp_subsec: u32,
        version: u16,
        id: &Ulid,
    ) -> [u8; CURSOR_KEY_LEN] {
        let mut suffix = [0u8; CURSOR_KEY_LEN];
        suffix[..8].copy_from_slice(&timestamp.to_be_bytes());
        suffix[8..12].copy_from_slice(&timestamp_subsec.to_be_bytes());
        suffix[12..14].copy_from_slice(&version.to_be_bytes());
        suffix[14..].copy_from_slice(&id.to_bytes());
        suffix
    }

    /// Appends `suffix` to `prefix`, yielding a full cursor-index key.
    fn with_suffix(mut prefix: Vec<u8>, suffix: &[u8]) -> Vec<u8> {
        prefix.extend_from_slice(suffix);
        prefix
    }

    /// Stages one event's entry into each cursor-index keyspace.
    fn insert_cursor_entries(
        &self,
        batch: &mut fjall::OwnedWriteBatch,
        aggregate_type: &str,
        aggregate_id: &str,
        name: &str,
        routing_key: Option<&str>,
        suffix: &[u8; CURSOR_KEY_LEN],
    ) {
        batch.insert(&self.cursor_all, *suffix, []);
        batch.insert(
            &self.cursor_agg,
            Self::with_suffix(Self::agg_prefix(aggregate_type, aggregate_id), suffix),
            [],
        );
        batch.insert(
            &self.cursor_agg_name,
            Self::with_suffix(
                Self::agg_name_prefix(aggregate_type, aggregate_id, name),
                suffix,
            ),
            [],
        );
        batch.insert(
            &self.cursor_type,
            Self::with_suffix(Self::type_only_prefix(aggregate_type), suffix),
            [],
        );
        batch.insert(
            &self.cursor_type_name,
            Self::with_suffix(Self::type_prefix(aggregate_type, name), suffix),
            [],
        );
        batch.insert(
            &self.cursor_routing,
            Self::with_suffix(Self::routing_cursor_prefix(routing_key), suffix),
            [],
        );
    }

    /// Builds the snapshot key.
    fn snapshot_key(aggregate_type: &str, id: &str) -> Vec<u8> {
        Self::encode_components(&[aggregate_type.as_bytes(), id.as_bytes()])
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

    /// Resolves the read filters to cursor-index scan sources — `(keyspace,
    /// prefix)` pairs whose keys under `prefix` are in cursor order. The flag
    /// says whether the routing key must still be checked per loaded event
    /// (the aggregator indexes don't encode it).
    fn cursor_sources(
        &self,
        aggregators: &Option<Vec<EventFilter>>,
        routing_key: &Option<RoutingKey>,
    ) -> (Vec<(&Keyspace, Vec<u8>)>, bool) {
        match (aggregators, routing_key) {
            (Some(aggs), routing) => {
                // Subscriptions emit one filter per handler, and those often
                // collapse to identical values (e.g. `by_type` once per
                // handler): scan each distinct filter once, not once per
                // handler.
                let mut seen_filters = std::collections::HashSet::new();
                let mut sources = Vec::new();
                for agg in aggs {
                    if !seen_filters.insert(agg) {
                        continue;
                    }
                    sources.push(match (&agg.aggregate_id, &agg.name) {
                        (Some(id), Some(name)) => (
                            &self.cursor_agg_name,
                            Self::agg_name_prefix(&agg.aggregate_type, id, name),
                        ),
                        (Some(id), None) => {
                            (&self.cursor_agg, Self::agg_prefix(&agg.aggregate_type, id))
                        }
                        (None, Some(name)) => (
                            &self.cursor_type_name,
                            Self::type_prefix(&agg.aggregate_type, name),
                        ),
                        (None, None) => (
                            &self.cursor_type,
                            Self::type_only_prefix(&agg.aggregate_type),
                        ),
                    });
                }
                (sources, matches!(routing, Some(RoutingKey::Value(_))))
            }
            (None, Some(RoutingKey::Value(key))) => (
                vec![(
                    &self.cursor_routing,
                    Self::routing_cursor_prefix(key.as_deref()),
                )],
                false,
            ),
            (None, Some(RoutingKey::All) | None) => (vec![(&self.cursor_all, Vec::new())], false),
        }
    }

    /// Serves a `read` off the cursor-ordered indexes: seek each distinct
    /// filter's range past the cursor, take `limit + 1` matches, merge. Cost is
    /// O(sources × limit) key steps plus one point-get per returned event —
    /// never a whole-prefix scan.
    fn read_indexed(
        &self,
        aggregators: Option<Vec<EventFilter>>,
        routing_key: Option<RoutingKey>,
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        use std::collections::HashSet;
        use std::ops::Bound;

        let finish = |events: Vec<Event>, args: Args| {
            evento_core::cursor::Reader::new(events)
                .args(args)
                .execute()
                .map_err(|e| anyhow::anyhow!("{}", e))
        };

        // An exclusive stamp bound of 0 admits nothing.
        if to_micros == Some(0) {
            return finish(Vec::new(), args);
        }
        // The exclusive micro bound as an inclusive key suffix: stamps have
        // milli precision, so `stamp_micros < bound` ⇔ `stamp_millis ≤
        // (bound - 1) / 1000` — the scan stops at the watermark instead of
        // fetching and discarding gated events.
        let bound_suffix = to_micros.map(|bound| {
            let millis = (bound - 1) / 1000;
            Self::cursor_key_suffix(
                millis / 1000,
                (millis % 1000) as u32,
                u16::MAX,
                &Ulid::from_bytes([0xFF; 16]),
            )
        });

        let (limit, cursor_value) = args.get_info();
        let backward = args.is_backward();
        // `limit + 1` per source: the extra row is the `has_more` probe, and
        // each source's first `limit + 1` matches are a superset of the merged
        // page (standard top-k merge property).
        let target = usize::from(limit) + 1;

        let cursor_suffix = match &cursor_value {
            Some(value) => {
                let cursor = <Event as evento_core::cursor::Cursor>::deserialize_cursor(value)
                    .map_err(|e| anyhow::anyhow!("{}", e))?;
                let id = Ulid::from_string(&cursor.i)?;
                Some(Self::cursor_key_suffix(cursor.t, cursor.s, cursor.v, &id))
            }
            None => None,
        };

        let (sources, check_routing) = self.cursor_sources(&aggregators, &routing_key);

        let mut picked: HashSet<Ulid> = HashSet::new();
        let mut rejected: HashSet<Ulid> = HashSet::new();
        let mut events: Vec<Event> = Vec::new();

        for (keyspace, prefix) in sources {
            // Every key in the range is `prefix` + a 30-byte suffix, so
            // `prefix` (shorter than any real key) and `prefix + [0xFF; 30]`
            // bracket exactly this filter's entries. The cursor bound is
            // exclusive in both directions (the canonical predicate is
            // strictly-beyond-cursor).
            let max_suffix = [0xFF; CURSOR_KEY_LEN];
            let upper_suffix = bound_suffix.as_ref().unwrap_or(&max_suffix);
            let (lower, upper) = if backward {
                let upper = match &cursor_suffix {
                    Some(suffix) => Bound::Excluded(Self::with_suffix(prefix.clone(), suffix)),
                    None => Bound::Included(Self::with_suffix(prefix.clone(), upper_suffix)),
                };
                (Bound::Included(prefix), upper)
            } else {
                let lower = match &cursor_suffix {
                    Some(suffix) => Bound::Excluded(Self::with_suffix(prefix.clone(), suffix)),
                    None => Bound::Included(prefix.clone()),
                };
                (
                    lower,
                    Bound::Included(Self::with_suffix(prefix, upper_suffix)),
                )
            };

            let mut range = keyspace.range((lower, upper));
            let mut matches = 0usize;
            loop {
                let guard = if backward {
                    range.next_back()
                } else {
                    range.next()
                };
                let Some(guard) = guard else { break };
                let (key, _) = guard.into_inner()?;
                let key = key.as_ref();
                let ulid_bytes: [u8; 16] = key[key.len() - 16..].try_into()?;
                let ulid = Ulid::from_bytes(ulid_bytes);

                if picked.contains(&ulid) {
                    // Already collected via another filter — still one of this
                    // source's matches, so it counts toward the cap.
                    matches += 1;
                    if matches >= target {
                        break;
                    }
                    continue;
                }
                if rejected.contains(&ulid) {
                    continue;
                }
                // An index entry without its event would be corruption; skip
                // it rather than serving a page with a hole.
                let Some(event) = self.load_event(&ulid)? else {
                    continue;
                };

                let routing_matches = !check_routing
                    || match &routing_key {
                        Some(RoutingKey::Value(Some(key))) => {
                            event.routing_key.as_ref() == Some(key)
                        }
                        Some(RoutingKey::Value(None)) => event.routing_key.is_none(),
                        Some(RoutingKey::All) | None => true,
                    };
                // Defensive re-check of the stamp bound (the range's upper
                // bound already enforces it).
                let below_bound = to_micros.is_none_or(|bound| {
                    event
                        .timestamp
                        .saturating_mul(1_000_000)
                        .saturating_add(u64::from(event.timestamp_subsec) * 1_000)
                        < bound
                });

                if routing_matches && below_bound {
                    picked.insert(ulid);
                    events.push(event);
                    matches += 1;
                    if matches >= target {
                        break;
                    }
                } else {
                    rejected.insert(ulid);
                }
            }
        }

        // `Reader` re-applies the canonical sort, cursor predicate, and limit,
        // so a seek bug degrades to a short page — never wrong order or
        // duplicates.
        finish(events, args)
    }
}

impl Fjall {
    /// Shared body of `write`/`replicate`: validates version contiguity and
    /// commits the batch atomically under the write lock. With `restamp`, all
    /// events are stamped from the monotonic commit clock (`max(now, last+1)`
    /// millis), which is persisted in the same batch.
    fn write_events(&self, mut events: Vec<Event>, restamp: bool) -> Result<(), WriteError> {
        use std::sync::atomic::Ordering;

        // Hold the write lock across validate + stamp + commit so concurrent
        // appends cannot both observe the same "last version" and the commit
        // clock stays strictly increasing. The fsync below runs *after* the
        // guard is dropped: `persist` is database-global, so a later writer's
        // fsync covers every earlier committed batch, and keeping it outside
        // the lock lets concurrent writers group-commit instead of
        // serializing on disk latency.
        let guard = self
            .write_lock
            .lock()
            .map_err(|_| WriteError::Unknown(anyhow::anyhow!("write lock poisoned")))?;

        if restamp {
            let now_millis = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
                .unwrap_or(0);
            // Only ever written under `write_lock`, so load-then-store is
            // race-free; the atomic exists for lock-free readers.
            let stamp = now_millis.max(self.last_stamp.load(Ordering::Acquire).saturating_add(1));
            for event in &mut events {
                event.timestamp = stamp / 1000;
                event.timestamp_subsec = (stamp % 1000) as u32;
            }
            self.last_stamp.store(stamp, Ordering::Release);
        }

        // Validate versions first (optimistic concurrency). `seen` tracks the
        // version assigned earlier in THIS batch so multiple events for the same
        // aggregate (e.g. a create() committing several events) validate correctly.
        let mut seen: std::collections::HashMap<(String, String), u16> =
            std::collections::HashMap::new();
        for event in &events {
            let agg = (event.aggregate_type.clone(), event.aggregate_id.clone());
            let last_version = match seen.get(&agg) {
                Some(v) => Some(*v),
                None => self
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

            // A reused ULID would silently overwrite the original event row
            // while both index entries survive — reject it instead.
            if self
                .events
                .get(event.id.to_bytes())
                .map_err(|e| WriteError::Unknown(e.into()))?
                .is_some()
            {
                return Err(WriteError::Unknown(anyhow::anyhow!(
                    "duplicate event id {}",
                    event.id
                )));
            }
        }

        // Write atomically using batch
        let mut batch = self.db.batch();

        for event in &events {
            let id_bytes = event.id.to_bytes();
            let stored = StoredEvent::from(event);
            let event_bytes = bitcode::encode(&stored);

            // Primary: ULID -> Event
            batch.insert(&self.events, id_bytes, event_bytes);

            // Aggregate version index: enc(type, id) + version -> ULID
            let agg_key = Fjall::agg_key(&event.aggregate_type, &event.aggregate_id, event.version);
            batch.insert(&self.agg_index, agg_key, id_bytes);

            // Cursor-ordered indexes (uses the final stamp, so entries land in
            // cursor order for both `write` and `replicate`).
            let suffix = Self::cursor_key_suffix(
                event.timestamp,
                event.timestamp_subsec,
                event.version,
                &event.id,
            );
            self.insert_cursor_entries(
                &mut batch,
                &event.aggregate_type,
                &event.aggregate_id,
                &event.name,
                event.routing_key.as_deref(),
                &suffix,
            );
        }

        if restamp {
            // Persist the commit clock atomically with the events it stamped.
            batch.insert(
                &self.meta,
                LAST_STAMP_KEY,
                self.last_stamp.load(Ordering::Acquire).to_be_bytes(),
            );
        }

        batch.commit().map_err(|e| WriteError::Unknown(e.into()))?;

        // Commit order is fixed; durability doesn't need the lock (see above).
        drop(guard);
        self.db
            .persist(self.write_persist_mode)
            .map_err(|e| WriteError::Unknown(e.into()))?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl Executor for Fjall {
    async fn write(&self, events: Vec<Event>) -> Result<(), WriteError> {
        if events.is_empty() {
            return Ok(());
        }
        let executor = self.clone();

        tokio::task::spawn_blocking(move || executor.write_events(events, true))
            .await
            .map_err(|e| WriteError::Unknown(e.into()))??;

        // Wake any in-process subscriptions immediately instead of waiting for
        // their next poll tick.
        self.write_tx.send_modify(|v| *v += 1);

        Ok(())
    }

    async fn replicate(&self, events: Vec<Event>) -> Result<(), WriteError> {
        if events.is_empty() {
            return Ok(());
        }
        let executor = self.clone();

        // Replication layers own ordering: persist caller timestamps verbatim.
        tokio::task::spawn_blocking(move || executor.write_events(events, false))
            .await
            .map_err(|e| WriteError::Unknown(e.into()))??;

        self.write_tx.send_modify(|v| *v += 1);

        Ok(())
    }

    fn write_watch(&self) -> Option<tokio::sync::watch::Receiver<u64>> {
        Some(self.write_tx.subscribe())
    }

    async fn read(
        &self,
        aggregators: Option<Vec<EventFilter>>,
        routing_key: Option<RoutingKey>,
        args: Args,
        to_micros: Option<u64>,
    ) -> anyhow::Result<ReadResult<Event>> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || {
            executor.read_indexed(aggregators, routing_key, args, to_micros)
        })
        .await?
    }

    async fn latest_timestamp(
        &self,
        _aggregators: Option<Vec<EventFilter>>,
        _routing_key: Option<RoutingKey>,
    ) -> anyhow::Result<u64> {
        // The commit clock is the max timestamp over ALL events. Using it
        // instead of a filtered lookup makes this O(1) rather than a full
        // load-and-sort of every matching event on every subscription poll;
        // the cost is that lag reported for a quiet stream can reflect writes
        // to other streams (an upper bound, never an undercount). Lock-free:
        // this must never wait on a writer's critical section from the async
        // runtime.
        Ok(self.last_stamp.load(std::sync::atomic::Ordering::Acquire) / 1000)
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
                Ok(state.worker_id == worker_id.to_string() && state.enabled)
            }
            None => Ok(false),
        })
        .await?
    }

    async fn subscriber_status(
        &self,
        key: String,
        worker_id: Ulid,
    ) -> anyhow::Result<SubscriberStatus> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || match executor.subscribers.get(&key)? {
            Some(bytes) => {
                let state: SubscriberState = bitcode::decode(bytes.as_ref())
                    .map_err(|e| anyhow::anyhow!("Failed to deserialize subscriber: {}", e))?;
                Ok(SubscriberStatus {
                    running: state.worker_id == worker_id.to_string() && state.enabled,
                    cursor: state.cursor.map(Value),
                })
            }
            None => Ok(SubscriberStatus::default()),
        })
        .await?
    }

    async fn latest_version(
        &self,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<u16> {
        let executor = self.clone();

        // A single reverse seek on the version-ordered aggregate index —
        // exact under `replicate` too, since it orders by version, not by
        // timestamp.
        tokio::task::spawn_blocking(move || {
            Ok(executor
                .get_last_version(&aggregate_type, &aggregate_id)?
                .unwrap_or(0))
        })
        .await?
    }

    async fn stream_routing_key(
        &self,
        aggregate_type: String,
        aggregate_id: String,
    ) -> anyhow::Result<Option<Option<String>>> {
        let executor = self.clone();

        // Version 1 is the stream's first event and carries the routing key
        // the stream was created with: one index point-get + one event load.
        tokio::task::spawn_blocking(move || {
            let key = Fjall::agg_key(&aggregate_type, &aggregate_id, 1);
            let Some(id_bytes) = executor.agg_index.get(&key)? else {
                return Ok(None);
            };
            let id_bytes: [u8; 16] = id_bytes
                .as_ref()
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid ULID in aggregate index"))?;
            Ok(executor
                .load_event(&Ulid::from_bytes(id_bytes))?
                .map(|e| e.routing_key))
        })
        .await?
    }

    async fn upsert_subscriber(&self, key: String, worker_id: Ulid) -> anyhow::Result<()> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || {
            // Read-modify-write under the write lock so a concurrent upsert or
            // acknowledge cannot lose the cursor. Cursor, lag, and enabled all
            // survive a worker takeover (mirroring the SQL upsert, which only
            // touches worker_id).
            let _guard = executor
                .write_lock
                .lock()
                .map_err(|_| anyhow::anyhow!("write lock poisoned"))?;

            let state = match executor.subscribers.get(&key)? {
                Some(bytes) => {
                    let mut state: SubscriberState = bitcode::decode(bytes.as_ref())
                        .map_err(|e| anyhow::anyhow!("Failed to deserialize subscriber: {}", e))?;
                    state.worker_id = worker_id.to_string();
                    state
                }
                None => SubscriberState {
                    worker_id: worker_id.to_string(),
                    cursor: None,
                    lag: 0,
                    enabled: true,
                },
            };

            executor
                .subscribers
                .insert(key.as_bytes(), bitcode::encode(&state))?;
            Ok(())
        })
        .await?
    }

    async fn acknowledge(
        &self,
        key: String,
        worker_id: Ulid,
        cursor: Value,
        lag: u64,
    ) -> anyhow::Result<bool> {
        let executor = self.clone();

        tokio::task::spawn_blocking(move || {
            // Fenced read-modify-write under the write lock: a superseded
            // worker's ack must not rewind the cursor the new owner is
            // advancing. A missing subscriber (deleted to stop the
            // subscription) also reads as lost ownership rather than an error.
            let _guard = executor
                .write_lock
                .lock()
                .map_err(|_| anyhow::anyhow!("write lock poisoned"))?;

            let Some(bytes) = executor.subscribers.get(&key)? else {
                return Ok(false);
            };
            let mut state: SubscriberState = bitcode::decode(bytes.as_ref())
                .map_err(|e| anyhow::anyhow!("Failed to deserialize subscriber: {}", e))?;
            // A disabled subscriber reads as lost ownership too, matching
            // `is_subscriber_running` (and the SQL backend's fenced UPDATE).
            if state.worker_id != worker_id.to_string() || !state.enabled {
                return Ok(false);
            }
            state.cursor = Some(cursor.0);
            state.lag = lag;

            executor
                .subscribers
                .insert(key.as_bytes(), bitcode::encode(&state))?;
            Ok(true)
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
            id: Ulid::generate(),
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
                None,
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

        let worker_id = Ulid::generate();
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

        // Acknowledge with cursor (fenced on the owning worker id)
        assert!(executor
            .acknowledge(key.clone(), worker_id, Value("test-cursor".to_string()), 0)
            .await
            .unwrap());

        // A superseded worker's ack is rejected and does not move the cursor
        assert!(!executor
            .acknowledge(
                key.clone(),
                Ulid::generate(),
                Value("stale-cursor".to_string()),
                0
            )
            .await
            .unwrap());

        // Check cursor is updated
        let cursor = executor.get_subscriber_cursor(key.clone()).await.unwrap();
        assert_eq!(cursor.unwrap().0, "test-cursor");

        // Disabling the subscription reports "not running"
        executor
            .set_subscriber_enabled(key.clone(), false)
            .await
            .unwrap();
        assert!(!executor
            .is_subscriber_running(key, worker_id)
            .await
            .unwrap());
    }

    /// Length-prefixed index keys: an aggregate id containing the old NUL
    /// separator (or a type/id boundary shift) must not leak into another
    /// aggregate's prefix scans or version lookup.
    #[tokio::test]
    async fn test_key_isolation_with_hostile_ids() {
        let temp_dir = tempfile::tempdir().unwrap();
        let executor = Fjall::open(temp_dir.path()).unwrap();

        // "a" and "a\0x" collided under the NUL-separator scheme: the prefix
        // for ("test/Account", "a") was a byte-prefix of ("test/Account", "a\0x").
        let mut tricky = create_test_event("a x", 1, "Created");
        tricky.aggregate_type = "test/Account".to_string();
        executor.write(vec![tricky]).await.unwrap();

        let plain = executor
            .read(
                Some(vec![EventFilter::by_id("test/Account", "a")]),
                None,
                Args::forward(10, None),
                None,
            )
            .await
            .unwrap();
        assert!(
            plain.edges.is_empty(),
            "aggregate 'a' must not see events of aggregate 'a\0x'"
        );

        // And the version check for "a" starts fresh (no bleed-through from
        // the hostile neighbour's index entries).
        let fresh = create_test_event("a", 1, "Created");
        executor.write(vec![fresh]).await.unwrap();
    }

    /// The commit clock survives a reopen: stamps stay strictly increasing
    /// even if events are written back-to-back across a close/open cycle.
    #[tokio::test]
    async fn test_commit_clock_is_monotonic_across_reopen() {
        let temp_dir = tempfile::tempdir().unwrap();

        let first_stamp = {
            let executor = Fjall::open(temp_dir.path()).unwrap();
            executor
                .write(vec![create_test_event("agg-mono", 1, "Created")])
                .await
                .unwrap();
            let read = executor
                .read(
                    Some(vec![EventFilter::by_id("test/Account", "agg-mono")]),
                    None,
                    Args::forward(10, None),
                    None,
                )
                .await
                .unwrap();
            let e = &read.edges[0].node;
            (e.timestamp, e.timestamp_subsec)
        };

        let executor = Fjall::open(temp_dir.path()).unwrap();
        executor
            .write(vec![create_test_event("agg-mono", 2, "Updated")])
            .await
            .unwrap();
        let read = executor
            .read(
                Some(vec![EventFilter::by_id("test/Account", "agg-mono")]),
                None,
                Args::forward(10, None),
                None,
            )
            .await
            .unwrap();
        let second = &read.edges[1].node;
        assert!(
            (second.timestamp, second.timestamp_subsec) > first_stamp,
            "commit stamps must stay strictly increasing across a reopen"
        );
    }

    /// `write` re-stamps with the commit clock; `replicate` persists the
    /// caller's timestamps verbatim.
    #[tokio::test]
    async fn test_write_restamps_and_replicate_preserves() {
        let temp_dir = tempfile::tempdir().unwrap();
        let executor = Fjall::open(temp_dir.path()).unwrap();

        let mut stale = create_test_event("agg-restamp", 1, "Created");
        stale.timestamp = 42; // long in the past
        stale.timestamp_subsec = 7;
        executor.write(vec![stale]).await.unwrap();

        let mut verbatim = create_test_event("agg-verbatim", 1, "Created");
        verbatim.timestamp = 42;
        verbatim.timestamp_subsec = 7;
        executor.replicate(vec![verbatim]).await.unwrap();

        let restamped = executor
            .read(
                Some(vec![EventFilter::by_id("test/Account", "agg-restamp")]),
                None,
                Args::forward(1, None),
                None,
            )
            .await
            .unwrap();
        assert!(
            restamped.edges[0].node.timestamp > 42,
            "write must replace a stale client stamp with the commit clock"
        );

        let preserved = executor
            .read(
                Some(vec![EventFilter::by_id("test/Account", "agg-verbatim")]),
                None,
                Args::forward(1, None),
                None,
            )
            .await
            .unwrap();
        assert_eq!(preserved.edges[0].node.timestamp, 42);
        assert_eq!(preserved.edges[0].node.timestamp_subsec, 7);
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

    /// An event with a caller-controlled stamp and routing key, for
    /// `replicate` (which persists stamps verbatim).
    fn stamped_event(
        aggregate_id: &str,
        version: u16,
        timestamp: u64,
        timestamp_subsec: u32,
        routing_key: Option<&str>,
    ) -> Event {
        Event {
            id: Ulid::generate(),
            aggregate_id: aggregate_id.to_string(),
            aggregate_type: "test/Account".to_string(),
            version,
            name: "Stamped".to_string(),
            routing_key: routing_key.map(str::to_string),
            data: vec![],
            metadata: Metadata::default(),
            timestamp,
            timestamp_subsec,
        }
    }

    /// Opening a database stamped with an older index layout rebuilds the
    /// cursor keyspaces from `events` and serves reads correctly afterwards.
    #[tokio::test]
    async fn test_rebuild_on_open_reindexes_legacy_database() {
        let temp_dir = tempfile::tempdir().unwrap();

        {
            let executor = Fjall::open(temp_dir.path()).unwrap();
            // Out-of-order stamps prove the rebuilt index orders by cursor,
            // not by insertion or ULID order.
            executor
                .replicate(vec![stamped_event("agg-r", 1, 300, 0, None)])
                .await
                .unwrap();
            executor
                .replicate(vec![stamped_event("agg-m", 1, 100, 0, Some("k"))])
                .await
                .unwrap();
            executor
                .replicate(vec![stamped_event("agg-m", 2, 200, 0, Some("k"))])
                .await
                .unwrap();
        }

        // Simulate a pre-migration database: no index version, no cursor
        // keyspaces (their content would have been the legacy layout).
        {
            let db = Database::builder(temp_dir.path()).open().unwrap();
            let meta = db.keyspace("meta", KeyspaceCreateOptions::default).unwrap();
            meta.remove(INDEX_VERSION_KEY).unwrap();
            for name in REBUILT_KEYSPACES {
                if db.keyspace_exists(name) {
                    let keyspace = db.keyspace(name, KeyspaceCreateOptions::default).unwrap();
                    db.delete_keyspace(keyspace).unwrap();
                }
            }
            db.persist(PersistMode::SyncAll).unwrap();
        }

        let executor = Fjall::open(temp_dir.path()).unwrap();

        // Paged read across all events comes back in cursor (stamp) order.
        let page = executor
            .read(None, None, Args::forward(2, None), None)
            .await
            .unwrap();
        assert_eq!(page.edges.len(), 2);
        assert_eq!(page.edges[0].node.timestamp, 100);
        assert_eq!(page.edges[1].node.timestamp, 200);
        assert!(page.page_info.has_next_page);
        let rest = executor
            .read(
                None,
                None,
                Args::forward(2, page.page_info.end_cursor.clone()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(rest.edges.len(), 1);
        assert_eq!(rest.edges[0].node.timestamp, 300);
        assert!(!rest.page_info.has_next_page);

        // Filtered and routing reads work off the rebuilt indexes too.
        let by_id = executor
            .read(
                Some(vec![EventFilter::by_id("test/Account", "agg-m")]),
                None,
                Args::forward(10, None),
                None,
            )
            .await
            .unwrap();
        assert_eq!(by_id.edges.len(), 2);
        let null_routing = executor
            .read(
                None,
                Some(RoutingKey::Value(None)),
                Args::forward(10, None),
                None,
            )
            .await
            .unwrap();
        assert_eq!(null_routing.edges.len(), 1);
        assert_eq!(null_routing.edges[0].node.aggregate_id, "agg-r");

        // The rebuild is stamped: a plain reopen keeps serving reads.
        drop(executor);
        let executor = Fjall::open(temp_dir.path()).unwrap();
        let all = executor
            .read(None, None, Args::forward(10, None), None)
            .await
            .unwrap();
        assert_eq!(all.edges.len(), 3);
    }

    /// The exclusive `to_micros` bound converts to an inclusive milli bound on
    /// the index scan — probe the off-by-one edges in both directions.
    #[tokio::test]
    async fn test_to_micros_watermark_milli_boundary() {
        let temp_dir = tempfile::tempdir().unwrap();
        let executor = Fjall::open(temp_dir.path()).unwrap();

        // Event stamp: 100 s + 500 ms = 100_500_000 µs.
        executor
            .replicate(vec![stamped_event("agg-b", 1, 100, 500, None)])
            .await
            .unwrap();

        for (bound, expect) in [
            (Some(100_499_999), 0),
            (Some(100_500_000), 0), // exclusive: == stamp is gated
            (Some(100_500_001), 1),
            (Some(0), 0),
            (None, 1),
        ] {
            let forward = executor
                .read(
                    Some(vec![EventFilter::by_id("test/Account", "agg-b")]),
                    None,
                    Args::forward(10, None),
                    bound,
                )
                .await
                .unwrap();
            assert_eq!(forward.edges.len(), expect, "forward, bound {bound:?}");
            let backward = executor
                .read(
                    Some(vec![EventFilter::by_id("test/Account", "agg-b")]),
                    None,
                    Args::backward(10, None),
                    bound,
                )
                .await
                .unwrap();
            assert_eq!(backward.edges.len(), expect, "backward, bound {bound:?}");
        }
    }

    /// Routing-only reads: the null range (`0x00` discriminator) pages
    /// correctly through a store mixing keyed and unkeyed events, and hostile
    /// routing keys (NUL bytes, the empty string) stay isolated from each
    /// other and from the null range.
    #[tokio::test]
    async fn test_routing_ranges_page_and_stay_isolated() {
        let temp_dir = tempfile::tempdir().unwrap();
        let executor = Fjall::open(temp_dir.path()).unwrap();

        for (agg, stamp, routing) in [
            ("agg-1", 10, None),
            ("agg-2", 20, Some("a")),
            ("agg-3", 30, None),
            ("agg-4", 40, Some("a\0x")),
            ("agg-5", 50, Some("")),
        ] {
            executor
                .replicate(vec![stamped_event(agg, 1, stamp, 0, routing)])
                .await
                .unwrap();
        }

        // Page through the null range one event at a time.
        let first = executor
            .read(
                None,
                Some(RoutingKey::Value(None)),
                Args::forward(1, None),
                None,
            )
            .await
            .unwrap();
        assert_eq!(first.edges.len(), 1);
        assert_eq!(first.edges[0].node.aggregate_id, "agg-1");
        assert!(first.page_info.has_next_page);
        let second = executor
            .read(
                None,
                Some(RoutingKey::Value(None)),
                Args::forward(1, first.page_info.end_cursor.clone()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(second.edges.len(), 1);
        assert_eq!(second.edges[0].node.aggregate_id, "agg-3");
        assert!(!second.page_info.has_next_page);

        // "a", "a\0x", "" and the null range are four disjoint result sets.
        for (routing, expect_agg) in [
            (Some("a"), "agg-2"),
            (Some("a\0x"), "agg-4"),
            (Some(""), "agg-5"),
        ] {
            let result = executor
                .read(
                    None,
                    Some(RoutingKey::Value(routing.map(str::to_string))),
                    Args::forward(10, None),
                    None,
                )
                .await
                .unwrap();
            assert_eq!(result.edges.len(), 1, "routing {routing:?}");
            assert_eq!(result.edges[0].node.aggregate_id, expect_agg);
        }
    }

    /// Aggregator filter + routing key: matches sparser than the scan must
    /// keep scanning past non-matching entries until the page fills.
    #[tokio::test]
    async fn test_sparse_routing_match_continues_past_limit() {
        let temp_dir = tempfile::tempdir().unwrap();
        let executor = Fjall::open(temp_dir.path()).unwrap();

        // Versions 1-5 are "cold"; only versions 6-8 match "hot".
        for version in 1u16..=8 {
            let routing = if version >= 6 {
                Some("hot")
            } else {
                Some("cold")
            };
            executor
                .replicate(vec![stamped_event(
                    "agg-s",
                    version,
                    u64::from(version) * 10,
                    0,
                    routing,
                )])
                .await
                .unwrap();
        }

        let page = executor
            .read(
                Some(vec![EventFilter::by_id("test/Account", "agg-s")]),
                Some(RoutingKey::Value(Some("hot".to_string()))),
                Args::forward(2, None),
                None,
            )
            .await
            .unwrap();
        assert_eq!(page.edges.len(), 2);
        assert_eq!(page.edges[0].node.version, 6);
        assert_eq!(page.edges[1].node.version, 7);
        assert!(page.page_info.has_next_page);

        let rest = executor
            .read(
                Some(vec![EventFilter::by_id("test/Account", "agg-s")]),
                Some(RoutingKey::Value(Some("hot".to_string()))),
                Args::forward(2, page.page_info.end_cursor.clone()),
                None,
            )
            .await
            .unwrap();
        assert_eq!(rest.edges.len(), 1);
        assert_eq!(rest.edges[0].node.version, 8);
        assert!(!rest.page_info.has_next_page);
    }
}
