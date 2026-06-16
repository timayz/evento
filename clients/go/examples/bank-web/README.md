# bank-web (Go SDK example)

A small web app demonstrating the [evento Go SDK](../../). It mirrors the Rust
[`examples/bank-axum-sqlite`](../../../../examples/bank-axum-sqlite) demo —
open accounts, deposit, withdraw, transfer — but instead of embedding the event
store it talks to a running `evento-server` over gRPC.

Because the gRPC surface is the **raw event store**, the event-sourcing logic
lives in the client (see [`bank.go`](bank.go)):

- **Events** are encoded as JSON. The server stores them as opaque bytes, so the
  encoding is entirely the client's choice.
- **Commands** (`openAccount`, `deposit`, `withdraw`, `transfer`) load the
  aggregate, validate, and append new events with optimistic concurrency
  (`Append(..., originalVersion, ...)`). `loadAccount` uses **snapshots**:
  it restores the last saved state, replays only the events after the snapshot
  cursor, and saves a refreshed snapshot — so loads don't replay from zero.
- **A live projection** ([`projection.go`](projection.go)) keeps an in-memory
  list of all accounts current via `Subscribe` (replay-from-zero on boot, then
  live tail). The accounts list page is served straight from it — no per-request
  replay. A single account view still loads by replay, so it's strongly
  consistent right after a write.

## Run

Start an event store, then the app:

The quickest way — from the repo root, start the server and this app together
(Ctrl+C stops both):

```sh
make bank-go
```

Or run them separately:

```sh
# from the repo root — single-node Fjall store on 127.0.0.1:50051
cargo run -p evento-server

# in another shell
cd clients/go/examples/bank-web
go run .
```

Open http://127.0.0.1:3000.

Configuration:

| Env | Default | Meaning |
|-----|---------|---------|
| `EVENTO_GRPC_ADDR` | `127.0.0.1:50051` | address of `evento-server` |
| `ADDR` | `127.0.0.1:3000` | HTTP listen address |

The same app works unchanged against any `evento-server` deployment mode
(single-node, SQL, or an Accord cluster) — only the server's startup config
differs.
