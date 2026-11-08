//! [`Fjall`] as a `ClusterBackend`: the shared scenario over a temporary
//! store whose journal lives in the same database. Gated by the `cluster`
//! feature.

#![cfg(feature = "cluster")]

use evento_fjall::Fjall;

#[tokio::test]
async fn cluster_backend() -> anyhow::Result<()> {
    evento_test::cluster_backend(&Fjall::temporary()?).await
}
