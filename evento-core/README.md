# evento-core

Core types and traits for the [evento](https://github.com/timayz/evento) event sourcing
toolkit: the `Executor` trait, the write path (`create`/`append`/`WriteBuilder`),
projections, subscriptions, cursors, and metadata.

Most applications should depend on the [`evento`](https://crates.io/crates/evento)
facade crate instead, which re-exports everything here plus the storage backends behind
feature flags:

```toml
[dependencies]
evento = { version = "2.0.0-alpha.27", features = ["sqlite"] }
```

Storage backends live in their own crates: `evento-sql` (SQLite/MySQL/PostgreSQL),
`evento-fjall` (embedded), `evento-remote` (client/server TCP), and `evento-accord`
(consensus-replicated).

## Learn more

- [API documentation](https://docs.rs/evento-core) — every public item is documented,
  with compile-checked examples
- [Workspace README](https://github.com/timayz/evento#readme) — quick start and the
  full command/projection/subscription walkthrough
- [`examples/bank`](https://github.com/timayz/evento/tree/main/examples/bank) — the
  canonical domain example
