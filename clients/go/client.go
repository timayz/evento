// Package evento is a Go client SDK for the evento gRPC event store.
//
// It wraps the generated gRPC client (package eventov1) with an idiomatic API
// for writing and reading events. Event payloads (`Data`) and metadata values
// are opaque bytes — the encoding is the caller's choice. Pagination cursors
// are opaque strings.
package evento

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	eventov1 "github.com/timayz/evento/clients/go/eventov1"
)

// defaultServiceConfig transparently retries the idempotent RPCs on transient
// UNAVAILABLE failures with capped exponential backoff. Write is deliberately
// excluded — it is not idempotent (a retried append could surface a spurious
// optimistic-concurrency conflict); use Commit for write retries. Subscribe is
// a stream and not covered by retry policy.
const defaultServiceConfig = `{
  "methodConfig": [{
    "name": [
      {"service":"evento.v1.EventStore","method":"Read"},
      {"service":"evento.v1.EventStore","method":"LatestTimestamp"},
      {"service":"evento.v1.EventStore","method":"GetSnapshot"},
      {"service":"evento.v1.EventStore","method":"SaveSnapshot"},
      {"service":"evento.v1.EventStore","method":"DeleteSnapshot"}
    ],
    "retryPolicy": {
      "maxAttempts": 4,
      "initialBackoff": "0.1s",
      "maxBackoff": "2s",
      "backoffMultiplier": 2.0,
      "retryableStatusCodes": ["UNAVAILABLE"]
    }
  }]
}`

// MetadataRequestedBy is the metadata key under which evento stores the
// "requested by" user id.
//
// NOTE: the Rust evento client bitcode-encodes this value, so a raw byte string
// written here is only interoperable with Rust readers if it is bitcode-framed.
// For Go-only deployments any encoding is fine.
const MetadataRequestedBy = "EVENTO_REQUESTED_BY"

// Typed errors surfaced by the client. Match them with errors.Is; the wrapped
// message carries the server's detail.
var (
	// ErrInvalidOriginalVersion is returned by Create/Append/Commit when the
	// optimistic-concurrency check fails (gRPC FAILED_PRECONDITION). Re-read the
	// aggregate's current version and retry (or use Commit, which does this).
	ErrInvalidOriginalVersion = errors.New("evento: invalid original version")
	// ErrInvalidArgument maps gRPC INVALID_ARGUMENT (bad request, e.g. no events).
	ErrInvalidArgument = errors.New("evento: invalid argument")
	// ErrUnavailable maps gRPC UNAVAILABLE (server unreachable after retries).
	ErrUnavailable = errors.New("evento: server unavailable")
)

// Client is a connection to an evento gRPC server.
type Client struct {
	raw  eventov1.EventStoreClient
	conn *grpc.ClientConn // non-nil only when created via Dial
}

// Dial connects to an evento server at target (e.g. "127.0.0.1:50051").
//
// Defaults: plaintext transport and a retry policy for idempotent RPCs. Pass
// options to override — e.g. WithTLS()/WithTLSConfig() for TLS, or
// WithWaitForReady() to block instead of failing fast while the server starts.
// Options are applied after the defaults, so they win.
func Dial(target string, opts ...grpc.DialOption) (*Client, error) {
	base := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(defaultServiceConfig),
	}
	conn, err := grpc.NewClient(target, append(base, opts...)...)
	if err != nil {
		return nil, err
	}
	return &Client{raw: eventov1.NewEventStoreClient(conn), conn: conn}, nil
}

// WithInsecure dials over plaintext (the default). Suitable for localhost/tests.
func WithInsecure() grpc.DialOption {
	return grpc.WithTransportCredentials(insecure.NewCredentials())
}

// WithTLS dials over TLS using the host's root CAs.
func WithTLS() grpc.DialOption {
	return grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{}))
}

// WithTLSConfig dials over TLS with a custom config (custom CA, mutual TLS, ...).
func WithTLSConfig(cfg *tls.Config) grpc.DialOption {
	return grpc.WithTransportCredentials(credentials.NewTLS(cfg))
}

// WithWaitForReady makes RPCs block until the channel is ready (bounded by each
// call's context deadline) instead of failing fast with UNAVAILABLE. Useful when
// the client may start before the server is listening.
func WithWaitForReady() grpc.DialOption {
	return grpc.WithDefaultCallOptions(grpc.WaitForReady(true))
}

// New wraps an existing gRPC connection. The caller owns the connection.
func New(conn grpc.ClientConnInterface) *Client {
	return &Client{raw: eventov1.NewEventStoreClient(conn)}
}

// Close closes the underlying connection if it was created by Dial.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// ---- types ----

// NewEvent is an event to append. The server assigns its id, version, and timestamp.
type NewEvent struct {
	Name string
	Data []byte
}

// WriteResult is returned by Create/Append.
type WriteResult struct {
	AggregateID string
	LastVersion uint32
}

// Metadata is the metadata attached to events. Values are opaque bytes.
type Metadata struct {
	ID   string
	Meta map[string][]byte
}

// Event is a stored event.
type Event struct {
	ID              string
	AggregateID     string
	AggregateType   string
	Version         uint32
	Name            string
	RoutingKey      *string
	Data            []byte
	Metadata        Metadata
	Timestamp       uint64
	TimestampSubsec uint32
}

// Edge pairs an event with its pagination cursor.
type Edge struct {
	Cursor string
	Node   Event
}

// PageInfo is cursor-pagination metadata.
type PageInfo struct {
	HasPreviousPage bool
	HasNextPage     bool
	StartCursor     *string
	EndCursor       *string
}

// ReadResult is a paginated read response.
type ReadResult struct {
	Edges    []Edge
	PageInfo PageInfo
}

// EventFilter selects events for Read / LatestTimestamp.
type EventFilter struct {
	AggregateType string
	AggregateID   *string
	Name          *string
}

// FilterByType matches all events of an aggregate type.
func FilterByType(t string) EventFilter { return EventFilter{AggregateType: t} }

// FilterByID matches all events of a specific aggregate instance.
func FilterByID(t, id string) EventFilter {
	return EventFilter{AggregateType: t, AggregateID: &id}
}

// FilterByEvent matches all events of a given name within an aggregate type.
func FilterByEvent(t, name string) EventFilter {
	return EventFilter{AggregateType: t, Name: &name}
}

// RoutingKey mirrors evento's routing-key filter (All | Value(optional)).
type RoutingKey struct {
	All   bool
	Value *string
}

// Args is cursor pagination: set First/After OR Last/Before.
type Args struct {
	First  *uint32
	After  *string
	Last   *uint32
	Before *string
}

// Forward builds forward-pagination args. Pass an empty `after` for the first page.
func Forward(first uint16, after string) Args {
	f := uint32(first)
	a := Args{First: &f}
	if after != "" {
		a.After = &after
	}
	return a
}

// Backward builds backward-pagination args.
func Backward(last uint16, before string) Args {
	l := uint32(last)
	a := Args{Last: &l}
	if before != "" {
		a.Before = &before
	}
	return a
}

// ReadQuery describes a Read request.
type ReadQuery struct {
	Filters    []EventFilter
	RoutingKey *RoutingKey
	Args       Args
}

// SubscribeOptions configures a subscription.
type SubscribeOptions struct {
	// Key is the durable subscription identity; its cursor persists across
	// reconnects, so a resubscribe resumes rather than replaying everything.
	Key string
	// Filters restricts the events delivered (empty = all events).
	Filters []EventFilter
	// RoutingKey restricts delivery to a routing key (nil = no routing filter).
	RoutingKey *RoutingKey
	// ChunkSize caps events per batch (0 = server default).
	ChunkSize uint16
}

// ---- write options ----

type writeOptions struct {
	routingKey *string
	metadataID string
	meta       map[string][]byte
}

// WriteOption configures Create/Append.
type WriteOption func(*writeOptions)

// WithRoutingKey sets the routing key for the written events.
func WithRoutingKey(k string) WriteOption {
	return func(o *writeOptions) { o.routingKey = &k }
}

// WithMetadataID sets the metadata id (server-generated if unset).
func WithMetadataID(id string) WriteOption {
	return func(o *writeOptions) { o.metadataID = id }
}

// WithMeta sets one opaque metadata key/value pair.
func WithMeta(key string, value []byte) WriteOption {
	return func(o *writeOptions) {
		if o.meta == nil {
			o.meta = map[string][]byte{}
		}
		o.meta[key] = value
	}
}

// WithRequestedBy sets the EVENTO_REQUESTED_BY metadata entry to the raw bytes
// of value. See MetadataRequestedBy for the Rust-interop caveat.
func WithRequestedBy(value string) WriteOption {
	return WithMeta(MetadataRequestedBy, []byte(value))
}

// ---- write ----

// Create writes events to a new aggregate (server-generated id).
func (c *Client) Create(ctx context.Context, aggregateType string, events []NewEvent, opts ...WriteOption) (WriteResult, error) {
	return c.write(ctx, aggregateType, nil, 0, events, opts)
}

// Append writes events to an existing aggregate, asserting originalVersion for
// optimistic concurrency. On conflict it returns ErrInvalidOriginalVersion.
func (c *Client) Append(ctx context.Context, aggregateType, aggregateID string, originalVersion uint16, events []NewEvent, opts ...WriteOption) (WriteResult, error) {
	return c.write(ctx, aggregateType, &aggregateID, uint32(originalVersion), events, opts)
}

// CommitIntent is what a Commit attempt decides to append: the target aggregate,
// the version it observed, and the events to write at that version.
type CommitIntent struct {
	AggregateType   string
	AggregateID     string
	OriginalVersion uint16
	Events          []NewEvent
}

// Commit runs an optimistic-concurrency-safe write with retry. On each attempt
// it calls build to (re)load the aggregate and decide what to append; if the
// append loses the concurrency race (ErrInvalidOriginalVersion), it calls build
// again — so build MUST re-read the current state/version each time, not reuse a
// stale value. build returning an error aborts immediately (no retry).
//
// Returns ErrInvalidOriginalVersion (wrapped) if all attempts conflict.
func (c *Client) Commit(ctx context.Context, attempts int, build func() (CommitIntent, error), opts ...WriteOption) (WriteResult, error) {
	if attempts < 1 {
		attempts = 1
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		intent, err := build()
		if err != nil {
			return WriteResult{}, err
		}
		res, err := c.Append(ctx, intent.AggregateType, intent.AggregateID, intent.OriginalVersion, intent.Events, opts...)
		if err == nil {
			return res, nil
		}
		if !errors.Is(err, ErrInvalidOriginalVersion) {
			return WriteResult{}, err
		}
		lastErr = err
	}
	return WriteResult{}, fmt.Errorf("evento: commit gave up after %d attempts: %w", attempts, lastErr)
}

func (c *Client) write(ctx context.Context, aggregateType string, aggregateID *string, originalVersion uint32, events []NewEvent, opts []WriteOption) (WriteResult, error) {
	var o writeOptions
	for _, f := range opts {
		f(&o)
	}

	pbEvents := make([]*eventov1.NewEvent, len(events))
	for i, e := range events {
		pbEvents[i] = &eventov1.NewEvent{Name: e.Name, Data: e.Data}
	}

	var md *eventov1.Metadata
	if o.metadataID != "" || len(o.meta) > 0 {
		md = &eventov1.Metadata{Id: o.metadataID, Meta: o.meta}
	}

	resp, err := c.raw.Write(ctx, &eventov1.WriteRequest{
		AggregateType:   aggregateType,
		AggregateId:     aggregateID,
		OriginalVersion: originalVersion,
		RoutingKey:      o.routingKey,
		Metadata:        md,
		Events:          pbEvents,
	})
	if err != nil {
		return WriteResult{}, mapErr(err)
	}
	return WriteResult{AggregateID: resp.GetAggregateId(), LastVersion: resp.GetLastVersion()}, nil
}

// ---- read ----

// Read runs a cursor-paginated query.
func (c *Client) Read(ctx context.Context, q ReadQuery) (ReadResult, error) {
	resp, err := c.raw.Read(ctx, &eventov1.ReadRequest{
		Aggregators: toPbFilters(q.Filters),
		RoutingKey:  toPbRoutingKey(q.RoutingKey),
		Args:        toPbArgs(q.Args),
	})
	if err != nil {
		return ReadResult{}, mapErr(err)
	}

	edges := make([]Edge, len(resp.GetEdges()))
	for i, e := range resp.GetEdges() {
		edges[i] = Edge{Cursor: e.GetCursor(), Node: fromPbEvent(e.GetNode())}
	}
	pi := resp.GetPageInfo()
	return ReadResult{
		Edges: edges,
		PageInfo: PageInfo{
			HasPreviousPage: pi.GetHasPreviousPage(),
			HasNextPage:     pi.GetHasNextPage(),
			StartCursor:     pi.StartCursor,
			EndCursor:       pi.EndCursor,
		},
	}, nil
}

// LatestTimestamp returns the unix-seconds timestamp of the most recent matching
// event, or 0 if none match.
func (c *Client) LatestTimestamp(ctx context.Context, filters []EventFilter, routing *RoutingKey) (uint64, error) {
	resp, err := c.raw.LatestTimestamp(ctx, &eventov1.LatestTimestampRequest{
		Aggregators: toPbFilters(filters),
		RoutingKey:  toPbRoutingKey(routing),
	})
	if err != nil {
		return 0, mapErr(err)
	}
	return resp.GetTimestamp(), nil
}

// Subscribe streams events to handler from the subscription's durable cursor
// (replay from the saved position, then live tail). After handler succeeds for
// every event in a batch, the batch's last cursor is acked so the server
// persists progress — delivery is at-least-once.
//
// Subscribe blocks until ctx is cancelled, the handler returns an error, or the
// stream ends; it returns that error (nil on a clean end). Cancel ctx to stop.
func (c *Client) Subscribe(ctx context.Context, opts SubscribeOptions, handler func(Event) error) error {
	stream, err := c.raw.Subscribe(ctx)
	if err != nil {
		return err
	}

	start := &eventov1.SubscribeRequest{
		Message: &eventov1.SubscribeRequest_Start{
			Start: &eventov1.SubscribeStart{
				Key:         opts.Key,
				Aggregators: toPbFilters(opts.Filters),
				RoutingKey:  toPbRoutingKey(opts.RoutingKey),
				ChunkSize:   uint32(opts.ChunkSize),
			},
		},
	}
	if err := stream.Send(start); err != nil {
		return err
	}

	for {
		batch, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		var lastCursor string
		for _, edge := range batch.GetEdges() {
			if err := handler(fromPbEvent(edge.GetNode())); err != nil {
				return err
			}
			lastCursor = edge.GetCursor()
		}

		if lastCursor != "" {
			ack := &eventov1.SubscribeRequest{
				Message: &eventov1.SubscribeRequest_Ack{
					Ack: &eventov1.SubscribeAck{Cursor: lastCursor},
				},
			}
			if err := stream.Send(ack); err != nil {
				return err
			}
		}
	}
}

// ---- snapshots ----

// Snapshot is a cached projection state plus the cursor it is valid up to.
// Both fields are opaque to the server.
type Snapshot struct {
	Data   []byte
	Cursor string
}

// GetSnapshot returns the snapshot for (aggregateType, revision, id). ok is
// false when none exists — including when the stored revision differs, which
// signals the caller to rebuild from scratch.
func (c *Client) GetSnapshot(ctx context.Context, aggregateType, revision, id string) (Snapshot, bool, error) {
	resp, err := c.raw.GetSnapshot(ctx, &eventov1.GetSnapshotRequest{
		AggregateType:     aggregateType,
		AggregateRevision: revision,
		Id:                id,
	})
	if err != nil {
		return Snapshot{}, false, mapErr(err)
	}
	s := resp.GetSnapshot()
	if s == nil {
		return Snapshot{}, false, nil
	}
	return Snapshot{Data: s.GetData(), Cursor: s.GetCursor()}, true, nil
}

// SaveSnapshot upserts the snapshot for (aggregateType, id). `cursor` is the
// position the snapshot reflects (e.g. the last folded event's cursor).
func (c *Client) SaveSnapshot(ctx context.Context, aggregateType, revision, id string, data []byte, cursor string) error {
	_, err := c.raw.SaveSnapshot(ctx, &eventov1.SaveSnapshotRequest{
		AggregateType:     aggregateType,
		AggregateRevision: revision,
		Id:                id,
		Data:              data,
		Cursor:            cursor,
	})
	return mapErr(err)
}

// DeleteSnapshot removes the snapshot for (aggregateType, id). Idempotent.
func (c *Client) DeleteSnapshot(ctx context.Context, aggregateType, id string) error {
	_, err := c.raw.DeleteSnapshot(ctx, &eventov1.DeleteSnapshotRequest{
		AggregateType: aggregateType,
		Id:            id,
	})
	return mapErr(err)
}

// ---- conversions ----

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	switch st.Code() {
	case codes.FailedPrecondition:
		return fmt.Errorf("%w: %s", ErrInvalidOriginalVersion, st.Message())
	case codes.InvalidArgument:
		return fmt.Errorf("%w: %s", ErrInvalidArgument, st.Message())
	case codes.Unavailable:
		return fmt.Errorf("%w: %s", ErrUnavailable, st.Message())
	default:
		return err
	}
}

func toPbFilters(filters []EventFilter) []*eventov1.EventFilter {
	if len(filters) == 0 {
		return nil
	}
	out := make([]*eventov1.EventFilter, len(filters))
	for i, f := range filters {
		out[i] = &eventov1.EventFilter{
			AggregateType: f.AggregateType,
			AggregateId:   f.AggregateID,
			Name:          f.Name,
		}
	}
	return out
}

func toPbRoutingKey(rk *RoutingKey) *eventov1.RoutingKey {
	if rk == nil {
		return nil
	}
	return &eventov1.RoutingKey{All: rk.All, Value: rk.Value}
}

func toPbArgs(a Args) *eventov1.Args {
	return &eventov1.Args{First: a.First, After: a.After, Last: a.Last, Before: a.Before}
}

func fromPbEvent(e *eventov1.Event) Event {
	if e == nil {
		return Event{}
	}
	md := Metadata{}
	if m := e.GetMetadata(); m != nil {
		md.ID = m.GetId()
		md.Meta = m.GetMeta()
	}
	return Event{
		ID:              e.GetId(),
		AggregateID:     e.GetAggregateId(),
		AggregateType:   e.GetAggregateType(),
		Version:         e.GetVersion(),
		Name:            e.GetName(),
		RoutingKey:      e.RoutingKey,
		Data:            e.GetData(),
		Metadata:        md,
		Timestamp:       e.GetTimestamp(),
		TimestampSubsec: e.GetTimestampSubsec(),
	}
}
