# evento Go client

A Go client SDK for the [evento](https://github.com/timayz/evento) gRPC event
store (`evento-server`). It speaks the `evento.v1.EventStore` service: write
and read events with cursor pagination.

Event payloads (`Data`) and metadata values are **opaque bytes** — you choose
the encoding. Pagination cursors are opaque strings.

## Install

```sh
go get github.com/timayz/evento/clients/go
```

## Generating the gRPC bindings

The generated protobuf/gRPC code (package `eventov1`) is produced from the
shared schema at [`server/proto/evento/v1/store.proto`](../../server/proto/evento/v1/store.proto)
using [buf](https://buf.build):

```sh
make gen        # runs `buf generate` -> ./eventov1
```

Generated files are committed so consumers don't need buf. Regenerate after any
change to the `.proto`.

## Connecting

`Dial` defaults to a plaintext transport plus a retry policy (idempotent RPCs
are retried on transient `UNAVAILABLE` with backoff; `Write` is excluded since
it isn't idempotent — use `Commit` for that). Override the defaults with options:

```go
c, _ := evento.Dial("host:50051", evento.WithTLS())            // TLS via system roots
c, _ := evento.Dial("host:50051", evento.WithTLSConfig(cfg))   // custom CA / mTLS
c, _ := evento.Dial("host:50051", evento.WithWaitForReady())   // block (per ctx) instead of failing fast while the server starts
```

Errors are mapped to sentinels you can match with `errors.Is`:
`ErrInvalidOriginalVersion`, `ErrInvalidArgument`, `ErrUnavailable`.

## Usage

```go
ctx := context.Background()

c, err := evento.Dial("127.0.0.1:50051")
if err != nil { log.Fatal(err) }
defer c.Close()

// Create a new aggregate (server assigns id, version, timestamp).
res, err := c.Create(ctx, "myapp/Account", []evento.NewEvent{
    {Name: "Opened", Data: []byte(`{"owner":"alice"}`)},
}, evento.WithRoutingKey("tenant-a"))

// Append with optimistic concurrency.
_, err = c.Append(ctx, "myapp/Account", res.AggregateID, res.LastVersion,
    []evento.NewEvent{{Name: "Deposited", Data: []byte(`{"amount":100}`)}})
if errors.Is(err, evento.ErrInvalidOriginalVersion) {
    // re-read current version and retry
}

// Read with cursor pagination.
page, err := c.Read(ctx, evento.ReadQuery{
    Filters: []evento.EventFilter{evento.FilterByID("myapp/Account", res.AggregateID)},
    Args:    evento.Forward(50, ""),
})
for _, edge := range page.Edges {
    fmt.Println(edge.Node.Name, edge.Node.Version, edge.Node.Data)
}
if page.PageInfo.HasNextPage {
    next, _ := c.Read(ctx, evento.ReadQuery{
        Filters: []evento.EventFilter{evento.FilterByID("myapp/Account", res.AggregateID)},
        Args:    evento.Forward(50, *page.PageInfo.EndCursor),
    })
    _ = next
}
```

## Subscriptions

`Subscribe` streams events from a durable cursor (replay from the saved
position, then live tail) and blocks, invoking the handler per event. After the
handler succeeds for a batch, the SDK acks its last cursor so the server
persists progress — delivery is **at-least-once** and **resumable**: a reconnect
with the same `Key` continues where it left off rather than replaying
everything.

```go
err := c.Subscribe(ctx, evento.SubscribeOptions{
    Key:     "billing-projection",                 // durable; survives reconnects
    Filters: []evento.EventFilter{evento.FilterByType("myapp/Account")},
}, func(ev evento.Event) error {
    // update a read model; return an error to stop, or nil to ack and continue
    fmt.Println(ev.AggregateID, ev.Name, ev.Version)
    return nil
})
// returns when ctx is cancelled, the handler errors, or the stream ends
```

For an in-memory read model that must be rebuilt on each boot, use a fresh
(e.g. random-suffixed) `Key` so the cursor starts at zero and the whole log
replays. See [`examples/bank-web`](examples/bank-web) for a live projection.

## Snapshots

Snapshots cache an aggregate/projection's folded state plus the cursor it's valid
up to, so a reader can resume from the cursor instead of replaying from zero.
`data` is your serialized state (opaque to the server); `revision` is a schema
version — a mismatch returns "not found", forcing a clean rebuild.

```go
// Restore, then read only the events after the snapshot cursor.
after := ""
if snap, ok, err := c.GetSnapshot(ctx, "myapp/Account", "v1", id); err != nil {
    return err
} else if ok {
    restore(snap.Data) // your decode
    after = snap.Cursor
}
page, _ := c.Read(ctx, evento.ReadQuery{
    Filters: []evento.EventFilter{evento.FilterByID("myapp/Account", id)},
    Args:    evento.Forward(100, after),
})
// fold page.Edges..., then persist a fresh snapshot at the last cursor:
c.SaveSnapshot(ctx, "myapp/Account", "v1", id, encode(state), lastCursor)

c.DeleteSnapshot(ctx, "myapp/Account", id) // idempotent
```

`examples/bank-web` uses this in its command path (`loadAccount`).

## Testing

```sh
make test       # builds evento-server, then `go test ./...`
```

The tests launch `evento-server` (single-node Fjall) on a temp store, or you
can point them at a running server with `EVENTO_GRPC_ADDR=host:port go test ./...`.

## Optimistic concurrency

The server is authoritative for event ids, versions, and timestamps. `Append`
asserts `originalVersion`; on a conflict it returns `ErrInvalidOriginalVersion`.

`Commit` automates the load → decide → append → retry-on-conflict loop. Its
`build` closure re-reads the aggregate on every attempt, so it always appends at
the current version:

```go
_, err := c.Commit(ctx, 5, func() (evento.CommitIntent, error) {
    acc, err := loadAccount(ctx, c, id) // your fold; returns current state + version
    if err != nil { return evento.CommitIntent{}, err }
    if acc.Balance < amount { return evento.CommitIntent{}, errInsufficient }
    ev, _ := newEvent("Withdrawn", Withdrawn{Amount: amount})
    return evento.CommitIntent{
        AggregateType:   "myapp/Account",
        AggregateID:     id,
        OriginalVersion: acc.Version,
        Events:          []evento.NewEvent{ev},
    }, nil
})
```

See `examples/bank-web` (`deposit`/`withdraw`/`transfer`).
