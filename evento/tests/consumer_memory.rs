#![allow(clippy::needless_return)]
mod consumer;

use evento::MemoryConsumer;
use evento_store::MemoryStore;
use tracing_test::traced_test;

#[tokio_shared_rt::test]
#[traced_test]
async fn multiple_consumer() {
    let consumer = MemoryConsumer::new();
    consumer::test_multiple_consumer(&consumer).await.unwrap();
}

#[tokio_shared_rt::test]
#[traced_test]
async fn filter() {
    let consumer = MemoryConsumer::new();
    consumer::test_filter(&consumer, false).await.unwrap();
}

#[tokio_shared_rt::test]
#[traced_test]
async fn filter_with_name() {
    let consumer = MemoryConsumer::new();
    consumer::test_filter(&consumer, true).await.unwrap();
}

#[tokio_shared_rt::test]
#[traced_test]
async fn deadletter() {
    let consumer = MemoryConsumer::new();
    consumer::test_deadletter(&consumer, false).await.unwrap();
}

#[tokio_shared_rt::test]
#[traced_test]
async fn deadletter_with_name() {
    let consumer = MemoryConsumer::new();
    consumer::test_deadletter(&consumer, true).await.unwrap();
}

#[tokio_shared_rt::test]
#[traced_test]
async fn post_handler() {
    let consumer = MemoryConsumer::new();
    consumer::test_post_handler(&consumer, false).await.unwrap();
}

#[tokio_shared_rt::test]
#[traced_test]
async fn post_handler_with_name() {
    let consumer = MemoryConsumer::new();
    consumer::test_post_handler(&consumer, true).await.unwrap();
}

#[tokio_shared_rt::test]
#[traced_test]
async fn no_cdc() {
    let consumer = MemoryConsumer::new();
    consumer::test_no_cdc(&consumer, false).await.unwrap();
}

#[tokio_shared_rt::test]
#[traced_test]
async fn no_cdc_with_name() {
    let consumer = MemoryConsumer::new();
    consumer::test_no_cdc(&consumer, true).await.unwrap();
}

#[tokio_shared_rt::test]
#[traced_test]
async fn external_store() {
    let consumer = MemoryConsumer::new();
    let store = MemoryStore::create();

    consumer::test_external_store(&consumer, &store, false)
        .await
        .unwrap();
}

#[tokio_shared_rt::test]
#[traced_test]
async fn external_store_with_name() {
    let consumer = MemoryConsumer::new();
    let store = MemoryStore::create();

    consumer::test_external_store(&consumer, &store, true)
        .await
        .unwrap();
}
