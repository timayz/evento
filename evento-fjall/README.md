# evento-fjall

Embedded [Fjall](https://crates.io/crates/fjall) (LSM-tree) storage backend for the
[evento](https://github.com/timayz/evento) event sourcing toolkit — no external
database server required.

Most applications depend on the [`evento`](https://crates.io/crates/evento) facade with
the `fjall` feature:

```toml
[dependencies]
evento = { version = "2.0.0-alpha.27", features = ["fjall"] }
```

Opening a store is one line — no pool, no migrations:

```rust,no_run
# fn run() -> anyhow::Result<()> {
let executor = evento_fjall::Fjall::open("./data")?;
// via the facade: evento::Fjall::open("./data")?
# let _ = executor;
# Ok(())
# }
```

The executor serializes writes in-process and stamps them with a monotonic commit
clock, so events are totally ordered without a stability watermark. See the
[crate docs](https://docs.rs/evento-fjall) for the key-encoding and partition data
model.

## Learn more

- [API documentation](https://docs.rs/evento-fjall)
- [Workspace README](https://github.com/timayz/evento#readme)
- Runnable examples: [`quickstart`](https://github.com/timayz/evento/tree/main/examples/quickstart),
  [`bank-axum-fjall`](https://github.com/timayz/evento/tree/main/examples/bank-axum-fjall)
