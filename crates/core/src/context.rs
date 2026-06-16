//! Type-safe context for storing request-scoped data.
//!
//! This module provides type-erased containers for storing arbitrary data
//! during event processing. Values are stored and retrieved by their Rust type.
//!
//! # Types
//!
//! - [`Context`] - Single-threaded type map (not `Send`/`Sync`)
//! - [`RwContext`] - Thread-safe version wrapped in `Arc<RwLock<_>>`
//! - [`Data`] - Arc-wrapped shared data for cloneable access
//!
//! # Example
//!
//! ```rust,ignore
//! use evento::context::{RwContext, Data};
//!
//! // Create a context
//! let ctx = RwContext::new();
//!
//! // Store data by type
//! ctx.insert(Data::new(MyAppState { ... }));
//! ctx.insert(42u32);
//!
//! // Retrieve data by type
//! let state: Data<MyAppState> = ctx.extract();
//! let number: u32 = ctx.get().unwrap();
//! ```

use serde::Serialize;
use std::{
    any::{type_name, Any, TypeId},
    collections::HashMap,
    fmt,
    hash::{BuildHasherDefault, Hasher},
    ops::Deref,
    sync::{Arc, RwLock},
};

/// A hasher for `TypeId`s that takes advantage of its known characteristics.
///
/// Author of `anymap` crate has done research on the topic:
/// https://github.com/chris-morgan/anymap/blob/2e9a5704/src/lib.rs#L599
#[derive(Debug, Default)]
struct NoOpHasher(u64);

impl Hasher for NoOpHasher {
    fn write(&mut self, _bytes: &[u8]) {
        unimplemented!("This NoOpHasher can only handle u64s")
    }

    fn write_u64(&mut self, i: u64) {
        self.0 = i;
    }

    fn finish(&self) -> u64 {
        self.0
    }
}

/// A type map for storing request-scoped data.
///
/// `Context` stores values by their Rust type, allowing type-safe retrieval.
/// All entries must be owned types that are `Send + Sync + 'static`.
///
/// For thread-safe access, use [`RwContext`] instead.
///
/// # Example
///
/// ```rust,ignore
/// let mut ctx = Context::new();
/// ctx.insert(42u32);
/// ctx.insert("hello".to_string());
///
/// assert_eq!(ctx.get::<u32>(), Some(&42));
/// assert_eq!(ctx.get::<String>(), Some(&"hello".to_string()));
/// ```
#[derive(Default)]
pub struct Context {
    map: HashMap<TypeId, Box<dyn Any + Send + Sync>, BuildHasherDefault<NoOpHasher>>,
}

impl Context {
    /// Creates an empty `Context`.
    #[inline]
    pub fn new() -> Context {
        Context {
            map: HashMap::default(),
        }
    }

    /// Insert an item into the map.
    ///
    /// If an item of this type was already stored, it will be replaced and returned.
    pub fn insert<T: Send + Sync + 'static>(&mut self, val: T) -> Option<T> {
        self.map
            .insert(TypeId::of::<T>(), Box::new(val))
            .and_then(downcast_owned)
    }

    /// Check if map contains an item of a given type.
    pub fn contains<T: 'static>(&self) -> bool {
        self.map.contains_key(&TypeId::of::<T>())
    }

    /// Get a reference to an item of a given type.
    pub fn extract<T: 'static>(&self) -> &T {
        match self.get::<T>() {
            Some(v) => v,
            _ => {
                tracing::debug!(
                    "Failed to extract `Data<{}>` For the Data extractor to work \
        correctly, wrap the data with `Data::new()` and pass it to `evento::data()`. \
        Ensure that types align in both the set and retrieve calls.",
                    type_name::<T>()
                );

                panic!(
                    "Requested application data is not configured correctly. \
    View/enable debug logs for more details."
                );
            }
        }
    }

    /// Get a reference to an item of a given type.
    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_ref())
    }

    /// Get a mutable reference to an item of a given type.
    pub fn get_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.map
            .get_mut(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_mut())
    }

    /// Remove an item from the map of a given type.
    ///
    /// If an item of this type was already stored, it will be returned.
    pub fn remove<T: Send + Sync + 'static>(&mut self) -> Option<T> {
        self.map.remove(&TypeId::of::<T>()).and_then(downcast_owned)
    }

    /// Clear the `Context` of all inserted extensions.
    #[inline]
    pub fn clear(&mut self) {
        self.map.clear();
    }

    /// Extends self with the items from another `Context`.
    pub fn extend(&mut self, other: Context) {
        self.map.extend(other.map);
    }
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Context").finish()
    }
}

fn downcast_owned<T: Send + Sync + 'static>(boxed: Box<dyn Any + Send + Sync>) -> Option<T> {
    boxed.downcast().ok().map(|boxed| *boxed)
}

/// Arc-wrapped shared data for use in contexts.
///
/// `Data<T>` wraps a value in an `Arc` for cheap cloning and sharing
/// across async tasks. It implements `Deref` for transparent access.
///
/// # Example
///
/// ```rust,ignore
/// use evento::context::Data;
///
/// struct AppConfig {
///     database_url: String,
/// }
///
/// let config = Data::new(AppConfig {
///     database_url: "postgres://...".into(),
/// });
///
/// // Clone is cheap (just Arc clone)
/// let config2 = config.clone();
///
/// // Access inner value via Deref
/// println!("{}", config.database_url);
/// ```
#[derive(Debug)]
pub struct Data<T: ?Sized>(Arc<T>);

impl<T> Data<T> {
    /// Create new `Data` instance wrapping the value in an `Arc`.
    pub fn new(state: T) -> Data<T> {
        Data(Arc::new(state))
    }
}

impl<T: ?Sized> Data<T> {
    /// Returns reference to inner `T`.
    pub fn get_ref(&self) -> &T {
        self.0.as_ref()
    }

    /// Unwraps to the internal `Arc<T>`
    pub fn into_inner(self) -> Arc<T> {
        self.0
    }
}

impl<T: ?Sized> Deref for Data<T> {
    type Target = Arc<T>;

    fn deref(&self) -> &Arc<T> {
        &self.0
    }
}

impl<T: ?Sized> Clone for Data<T> {
    fn clone(&self) -> Data<T> {
        Data(Arc::clone(&self.0))
    }
}

impl<T: ?Sized> From<Arc<T>> for Data<T> {
    fn from(arc: Arc<T>) -> Self {
        Data(arc)
    }
}

impl<T> Serialize for Data<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

/// Thread-safe context for storing request-scoped data.
///
/// `RwContext` wraps a [`Context`] in `Arc<RwLock<_>>` for safe concurrent access.
/// It can be cloned cheaply and shared across async tasks.
///
/// # Example
///
/// ```rust,ignore
/// use evento::context::RwContext;
///
/// let ctx = RwContext::new();
///
/// // Insert data (acquires write lock)
/// ctx.insert(42u32);
///
/// // Get data (acquires read lock, clones the value)
/// let value: Option<u32> = ctx.get();
///
/// // Extract panics if not found (useful for required dependencies)
/// let value: u32 = ctx.extract();
/// ```
///
/// # Panics
///
/// Methods will panic if the internal `RwLock` is poisoned.
pub struct RwContext(Arc<RwLock<Context>>);

impl Default for RwContext {
    fn default() -> Self {
        Self::new()
    }
}

impl RwContext {
    /// Creates an empty `RwContext`.
    #[inline]
    pub fn new() -> Self {
        RwContext(Arc::new(RwLock::new(Context::new())))
    }

    /// Insert an item into the map.
    ///
    /// If an item of this type was already stored, it will be replaced and returned.
    pub fn insert<T: Send + Sync + 'static>(&self, val: T) -> Option<T> {
        self.0.write().expect("RwContext lock poisoned").insert(val)
    }

    /// Check if map contains an item of a given type.
    pub fn contains<T: 'static>(&self) -> bool {
        self.0
            .read()
            .expect("RwContext lock poisoned")
            .contains::<T>()
    }

    /// Get a clone of an item of a given type, panics if not found.
    pub fn extract<T: Clone + 'static>(&self) -> T {
        self.0
            .read()
            .expect("RwContext lock poisoned")
            .extract::<T>()
            .clone()
    }

    /// Get a clone of an item of a given type.
    pub fn get<T: Clone + 'static>(&self) -> Option<T> {
        self.0
            .read()
            .expect("RwContext lock poisoned")
            .get::<T>()
            .cloned()
    }

    /// Remove an item from the map of a given type.
    ///
    /// If an item of this type was already stored, it will be returned.
    pub fn remove<T: Send + Sync + 'static>(&self) -> Option<T> {
        self.0
            .write()
            .expect("RwContext lock poisoned")
            .remove::<T>()
    }

    /// Clear the `RwContext` of all inserted extensions.
    #[inline]
    pub fn clear(&self) {
        self.0.write().expect("RwContext lock poisoned").clear();
    }

    /// Extends self with the items from another `Context`.
    pub fn extend(&self, other: Context) {
        self.0
            .write()
            .expect("RwContext lock poisoned")
            .extend(other);
    }
}

impl Clone for RwContext {
    fn clone(&self) -> Self {
        RwContext(Arc::clone(&self.0))
    }
}
