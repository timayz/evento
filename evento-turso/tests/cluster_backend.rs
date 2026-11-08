//! [`Turso`] as a `ClusterBackend`: the shared scenario over a store in each
//! write mode. Gated by the `cluster` feature.

#![cfg(feature = "cluster")]

use evento_turso::Turso;

#[tokio::test]
async fn cluster_backend() -> anyhow::Result<()> {
    evento_test::cluster_backend(&Turso::in_memory().await?).await
}

#[tokio::test]
async fn cluster_backend_concurrent() -> anyhow::Result<()> {
    evento_test::cluster_backend(&Turso::in_memory_concurrent().await?).await
}
