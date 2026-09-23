//! Type-safe context for storing request-scoped data.
//!
//! This module provides type-erased containers for storing arbitrary data
//! during event processing. Values are stored and retrieved by their Rust type.
//!
//! # Types
//!
//! - [`Context`] - Single-threaded type map (not `Send`/`Sync`)
//! - [`RwContext`] - Thread-safe version wrapped in `Arc<RwLock<_>>`
//! - [`Data`] - Arc-wrapped shared data, for values that are not `Clone`
//! - [`MissingData`] - Error for a type that was never registered
//!
//! # Registering and reading data
//!
//! Store the value itself and read it back by its own type. Handlers reach the
//! context through [`RwContext`], whose [`extract`](RwContext::extract) clones
//! the value out of the read lock, so the type must be `Clone` and should be
//! cheap to clone. Wrap anything else in [`Data`].
//!
//! ```rust
//! use evento::context::{Data, RwContext};
//!
//! #[derive(Clone)]
//! struct Smtp { host: &'static str }
//! struct Templates { welcome: String }
//!
//! // A subscription or projection builds this for you from `.data(..)`; here
//! // we drive the context directly.
//! let ctx = RwContext::new();
//!
//! ctx.insert(Smtp { host: "localhost" }); // `.data(smtp)`
//! ctx.insert(Data::new(Templates { welcome: "hi".to_owned() })); // `.data(Data::new(templates))`
//!
//! let smtp: Smtp = ctx.extract();
//! let templates: Data<Templates> = ctx.extract();
//! # assert_eq!(smtp.host, "localhost");
//! # assert_eq!(templates.welcome, "hi");
//! ```
//!
//! A type that was never registered makes [`extract`](RwContext::extract) panic;
//! [`try_extract`](RwContext::try_extract) returns a [`MissingData`] error instead.

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
/// ```rust
/// # use evento::context::Context;
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
    ///
    /// The type must match the one it was registered under exactly: a value
    /// stored as `Data<T>` comes back as `Data<T>`, not as `T`.
    ///
    /// # Panics
    ///
    /// Panics if no value of type `T` was registered, with the message of the
    /// [`MissingData`] error. Use [`try_extract`](Self::try_extract) to handle
    /// that case instead.
    pub fn extract<T: 'static>(&self) -> &T {
        match self.get::<T>() {
            Some(v) => v,
            None => panic!("{}", MissingData::of::<T>()),
        }
    }

    /// Get a reference to an item of a given type, or a [`MissingData`] error.
    ///
    /// The non-panicking counterpart of [`extract`](Self::extract). Handlers
    /// return `anyhow::Result<()>`, so `ctx.try_extract::<T>()?` surfaces a
    /// misconfigured context as a handler error rather than a panic in a
    /// worker task.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use evento::context::Context;
    /// let mut ctx = Context::new();
    /// ctx.insert(42u32);
    ///
    /// assert_eq!(ctx.try_extract::<u32>().unwrap(), &42);
    /// assert!(ctx.try_extract::<String>().is_err());
    /// ```
    pub fn try_extract<T: 'static>(&self) -> Result<&T, MissingData> {
        self.get::<T>().ok_or_else(MissingData::of::<T>)
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

/// Error for a type the context does not hold.
///
/// Returned by [`Context::try_extract`] and [`RwContext::try_extract`]; the same
/// message is what the panicking [`Context::extract`] prints.
#[derive(Debug, thiserror::Error)]
#[error(
    "no `{type_name}` in the evento context: register it with \
     `SubscriptionBuilder::data(..)` or `Projection::data(..)`, and extract the \
     same type it was registered under (a value registered as `Data<T>` is \
     extracted as `Data<T>`, not as `T`)"
)]
pub struct MissingData {
    /// [`std::any::type_name`] of the type that was requested.
    pub type_name: &'static str,
}

impl MissingData {
    /// The error for a missing `T`.
    pub fn of<T: ?Sized>() -> Self {
        MissingData {
            type_name: type_name::<T>(),
        }
    }
}

fn downcast_owned<T: Send + Sync + 'static>(boxed: Box<dyn Any + Send + Sync>) -> Option<T> {
    boxed.downcast().ok().map(|boxed| *boxed)
}

/// Arc-wrapped shared data, for values the bare idiom cannot carry.
///
/// The context hands values back by cloning them, so registering a value
/// directly — `.data(config)` read back as `AppConfig` — needs `AppConfig` to be
/// `Clone` and cheap to clone. `Data<T>` covers everything else: it puts the
/// value in an `Arc`, so any `T` can be shared across async tasks and every
/// extract is one atomic increment. `Deref` gives transparent access to the
/// inner value.
///
/// Register it as `Data<T>` and extract it as `Data<T>` — the two halves must
/// name the same type.
///
/// # Example
///
/// ```rust
/// use evento::context::{Data, RwContext};
///
/// // Neither `Clone` nor cheap to copy.
/// struct Templates {
///     welcome: String,
/// }
///
/// let ctx = RwContext::new();
///
/// // In a subscription: `.data(Data::new(templates))`.
/// ctx.insert(Data::new(Templates {
///     welcome: "welcome!".to_owned(),
/// }));
///
/// // In a handler: `context.extract()`.
/// let templates: Data<Templates> = ctx.extract();
/// assert_eq!(templates.welcome, "welcome!");
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
/// ```rust
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
/// # assert_eq!(value, 42);
///
/// // ...or handle the missing type
/// assert!(ctx.try_extract::<String>().is_err());
/// ```
///
/// # Panics
///
/// Methods will panic if the internal `RwLock` is poisoned, and
/// [`extract`](Self::extract) panics if the requested type was never registered.
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
    ///
    /// The value is cloned out of the read lock, so `T` must be `Clone` and
    /// should be cheap to clone — a pool handle, an `Arc`, a [`Data`]. Wrap
    /// anything else in [`Data`] when registering it.
    ///
    /// # Panics
    ///
    /// Panics if no value of type `T` was registered; see
    /// [`try_extract`](Self::try_extract).
    pub fn extract<T: Clone + 'static>(&self) -> T {
        self.0
            .read()
            .expect("RwContext lock poisoned")
            .extract::<T>()
            .clone()
    }

    /// Get a clone of an item of a given type, or a [`MissingData`] error.
    ///
    /// The non-panicking counterpart of [`extract`](Self::extract), for handlers
    /// that would rather return an error than panic in a worker task.
    pub fn try_extract<T: Clone + 'static>(&self) -> Result<T, MissingData> {
        self.0
            .read()
            .expect("RwContext lock poisoned")
            .try_extract::<T>()
            .cloned()
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
