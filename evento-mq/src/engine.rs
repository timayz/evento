#[cfg(feature = "memory")]
mod memory;
use dyn_clone::DynClone;
#[cfg(feature = "memory")]
pub use memory::*;

#[cfg(feature = "pg")]
mod pg;
#[cfg(feature = "pg")]
pub use pg::*;

use async_trait::async_trait;

#[async_trait]
pub trait Engine: DynClone + Send + Sync {}

dyn_clone::clone_trait_object!(Engine);
