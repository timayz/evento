#![allow(clippy::needless_return)]
mod store;

use evento_store::MemoryStore;
use tokio::sync::OnceCell;

static ONCE: OnceCell<MemoryStore> = OnceCell::const_new();

async fn get_store() -> &'static MemoryStore {
    ONCE.get_or_init(|| async {
        let store = MemoryStore::new();
        store::init(&store).await.unwrap();
        store
    })
    .await
}

#[tokio_shared_rt::test]
async fn concurrency() {
    let store = get_store().await;
    store::test_concurrency(store).await.unwrap();
}

#[tokio_shared_rt::test]
async fn save() {
    let store = get_store().await;
    store::test_save(store).await.unwrap();
}

#[tokio_shared_rt::test]
async fn wrong_version() {
    let store = get_store().await;
    store::test_wrong_version(store).await.unwrap();
}
