//! Test doubles shared by the integration suites: a journal and data stores
//! whose storage fails, or loses what it had not made durable, an in-memory
//! evento executor for the real `ExecutorDataStore`, and the addresses of
//! the nodes opened over real TCP.
//!
//! Each suite compiles this module on its own and uses part of it.
#![allow(dead_code)]

pub mod addrs;
pub mod executor;
pub mod inline;
pub mod journal;
pub mod store;
