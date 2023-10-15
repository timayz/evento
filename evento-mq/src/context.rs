use std::{
    any::{type_name, Any, TypeId},
    collections::HashMap,
    fmt,
    hash::{BuildHasherDefault, Hasher},
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

/// A type map for request extensions.
///
/// All entries into this map must be owned types (or static references).
#[derive(Default)]
pub struct Context {
    /// Use AHasher with a std HashMap with for faster lookups on the small `TypeId` keys.
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
    ///
    /// ```
    /// # use evento_mq::Context;
    /// let mut map = Context::new();
    /// assert_eq!(map.insert(""), None);
    /// assert_eq!(map.insert(1u32), None);
    /// assert_eq!(map.insert(2u32), Some(1u32));
    /// assert_eq!(*map.get::<u32>().unwrap(), 2u32);
    /// ```
    pub fn insert<T: Send + Sync + 'static>(&mut self, val: T) -> Option<T> {
        self.map
            .insert(TypeId::of::<T>(), Box::new(val))
            .and_then(downcast_owned)
    }

    /// Check if map contains an item of a given type.
    ///
    /// ```
    /// # use evento_mq::Context;
    /// let mut map = Context::new();
    /// assert!(!map.contains::<u32>());
    ///
    /// assert_eq!(map.insert(1u32), None);
    /// assert!(map.contains::<u32>());
    /// ```
    pub fn contains<T: 'static>(&self) -> bool {
        self.map.contains_key(&TypeId::of::<T>())
    }

    /// Get a reference to an item of a given type.
    ///
    /// ```
    /// # use evento_mq::Context;
    /// let mut map = Context::new();
    /// map.insert(1u32);
    /// assert_eq!(map.get::<u32>(), Some(&1u32));
    /// ```
    pub fn extract<T: 'static>(&self) -> &T {
        match self.get::<T>() {
            Some(v) => v,
            _ => {
                tracing::debug!(
                    "Failed to extract `Data<{}>` For the Data extractor to work \
        correctly, wrap the data with `Data::new()` and pass it to `Evento::data()`. \
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
    ///
    /// ```
    /// # use evento_mq::Context;
    /// let mut map = Context::new();
    /// map.insert(1u32);
    /// assert_eq!(map.get::<u32>(), Some(&1u32));
    /// ```
    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.map
            .get(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_ref())
    }

    /// Get a mutable reference to an item of a given type.
    ///
    /// ```
    /// # use evento_mq::Context;
    /// let mut map = Context::new();
    /// map.insert(1u32);
    /// assert_eq!(map.get_mut::<u32>(), Some(&mut 1u32));
    /// ```
    pub fn get_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.map
            .get_mut(&TypeId::of::<T>())
            .and_then(|boxed| boxed.downcast_mut())
    }

    /// Remove an item from the map of a given type.
    ///
    /// If an item of this type was already stored, it will be returned.
    ///
    /// ```
    /// # use evento_mq::Context;
    /// let mut map = Context::new();
    ///
    /// map.insert(1u32);
    /// assert_eq!(map.get::<u32>(), Some(&1u32));
    ///
    /// assert_eq!(map.remove::<u32>(), Some(1u32));
    /// assert!(!map.contains::<u32>());
    /// ```
    pub fn remove<T: Send + Sync + 'static>(&mut self) -> Option<T> {
        self.map.remove(&TypeId::of::<T>()).and_then(downcast_owned)
    }

    /// Clear the `Context` of all inserted extensions.
    ///
    /// ```
    /// # use evento_mq::Context;
    /// let mut map = Context::new();
    ///
    /// map.insert(1u32);
    /// assert!(map.contains::<u32>());
    ///
    /// map.clear();
    /// assert!(!map.contains::<u32>());
    /// ```
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
