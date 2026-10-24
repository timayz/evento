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
evento = { version = "2.0.0-alpha.34", features = ["remote"] }
```

```rust,no_run
# async fn run() -> anyhow::Result<()> {
// Server process: serve an existing executor.
let executor = evento_fjall::Fjall::open("./data")?;
let listener = tokio::net::TcpListener::bind("0.0.0.0:4321").await?;
let handle = evento_remote::serve(listener, executor);

// Client process: connect and use like any executor.
let client = evento_remote::Client::connect("127.0.0.1:4321").await?;
# let _ = (handle, client);
# Ok(())
# }
```

Writes through the server are pushed to every connected client, so client-side
subscriptions wake with the same low latency as a local `write_watch`.

A host authenticating its clients implements `Resolver`: every connection opens
with a `Hello` carrying the client's token (`Client::builder(addr).token(..)`), and
the resolver answers with the `Session` to serve it — the executor, a guard kept
for the connection's lifetime, a signal that ends it — or a `Rejection` the client
sees as its `connect` error. `serve_with(listener, resolver)` takes it; `serve` is
the open single-store case.

Which tenant a client belongs to is the host application's decision: its resolver
maps the token (an API key, say) to one and serves the connection in it,
`Session::new(store.clone()).tenant(tenant)?`. Every request on the connection then
acts in that tenant whatever it names — what it reads and loads, the tenant its
writes are stamped with — and `replicate` and the subscriber calls are refused, so a
client never reaches another tenant's data. The client stays tenant-agnostic: a
tenant application following the store through a `Sidecar` keeps its subscriptions'
cursors and its snapshots in its own store. `serve_tls` / `serve_tls_with` and
`ClientBuilder::tls` wrap connections in rustls; plain TCP is for trusted networks,
where a token travels in clear text.

## Learn more

- [API documentation](https://docs.rs/evento-remote)
- [Workspace README](https://github.com/timayz/evento#readme)
- Runnable two-process demo: [`examples/bank-axum-remote`](https://github.com/timayz/evento/tree/main/examples/bank-axum-remote)
