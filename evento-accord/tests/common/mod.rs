//! Test doubles shared by the integration suites: a journal and data stores
//! whose storage fails, or loses what it had not made durable, and an
//! in-memory evento executor for the real `ExecutorDataStore`.
//!
//! Each suite compiles this module on its own and uses part of it.
#![allow(dead_code)]

pub mod executor;
pub mod inline;
pub mod journal;
pub mod store;
