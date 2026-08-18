# evento-remote

Client/server remote executor for [evento](https://github.com/timayz/evento) — serve any
executor over framed TCP.

- `serve` exposes **any** evento `Executor` (SQL, Fjall, Accord, …) over
  length-delimited framed TCP.
- `Client` implements `Executor` by forwarding every call to a server, so
  `evento::create`, projections, and subscriptions work unchanged against a
  remote store.

Most applications depend on the [`evento`](https://crates.io/crates/evento) facade with the
`remote` feature instead of using this crate directly:

```toml
[dependencies]
evento = { version = "2.0.0-alpha.27", features = ["remote"] }
```

```rust,no_run
# async fn run() -> anyhow::Result<()> {
// Server process: serve an existing executor.
let executor = evento_fjall::Fjall::open("./data")?;
let listener = tokio::net::TcpListener::bind("0.0.0.0:4321").await?;
let handle = evento_remote::serve(listener, executor);

// Client process: connect and use like any executor.
let client = evento_remote::Client::connect("127.0.0.1:4321".parse()?).await?;
# let _ = (handle, client);
# Ok(())
# }
```

Writes through the server are pushed to every connected client, so client-side
subscriptions wake with the same low latency as a local `write_watch`. v1 is
plaintext TCP for trusted networks.

## Learn more

- [API documentation](https://docs.rs/evento-remote)
- [Workspace README](https://github.com/timayz/evento#readme)
- Runnable two-process demo: [`examples/bank-axum-remote`](https://github.com/timayz/evento/tree/main/examples/bank-axum-remote)
