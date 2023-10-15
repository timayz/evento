#![allow(clippy::needless_return)]
mod consumer;

use evento_mq::MemoryConsumer;
use evento_store::MemoryStore;

#[tokio_shared_rt::test]
async fn cdc() {
    let consumer = MemoryConsumer::new();
    consumer::test_cdc(&consumer).await.unwrap();
}

#[tokio_shared_rt::test]
async fn no_cdc() {
    let consumer = MemoryConsumer::new();
    consumer::test_no_cdc(&consumer).await.unwrap();
}

#[tokio_shared_rt::test]
async fn external_store() {
    let consumer = MemoryConsumer::new();
    let store = MemoryStore::new();

    consumer::test_external_store(&consumer, &store)
        .await
        .unwrap();
}
