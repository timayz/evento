# evento-test

Shared conformance test suite for [evento](https://github.com/timayz/evento) executor
backends. Not published; used as a dev-dependency by `evento-sql`, `evento-fjall`, and
`evento-remote` so every backend runs the exact same scenarios (writes, optimistic
concurrency, routing keys, subscriptions, snapshots, tombstones, commands, …).

To run the suite against your own `Executor` implementation, mirror how the built-in
backends wire it up (see `evento-fjall/tests/fjall.rs`):

```rust,ignore
#[tokio::test]
async fn load() -> anyhow::Result<()> {
    let executor = my_backend::open()?;
    evento_test::load(&executor.into()).await
}
```

Each public function in `evento_test` is one scenario; call it with your executor and it
asserts the expected behavior end to end.
