package evento_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	evento "github.com/timayz/evento/clients/go"
)

// startServer returns the address of an evento server to test against.
//
// If EVENTO_GRPC_ADDR is set it is used directly. Otherwise it launches the
// `evento-server` binary (built by `cargo build -p evento-server`) on a temp
// Fjall store and a free port, and tears it down at test end. If the binary is
// not found, the test is skipped.
func startServer(t *testing.T) string {
	t.Helper()
	if addr := os.Getenv("EVENTO_GRPC_ADDR"); addr != "" {
		return addr
	}

	bin := serverBinary()
	if bin == "" {
		t.Skip("evento-server not found; run `cargo build -p evento-server` or set EVENTO_GRPC_ADDR")
	}

	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	dir := t.TempDir()

	cmd := exec.Command(bin)
	cmd.Env = append(os.Environ(),
		"EVENTO_GRPC_ADDR="+addr,
		"EVENTO_MODE=single",
		"EVENTO_BACKEND=fjall",
		"EVENTO_STORE_PATH="+filepath.Join(dir, "store"),
	)
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	waitListening(t, addr)
	return addr
}

func serverBinary() string {
	for _, p := range []string{
		"../../target/debug/evento-server",
		"../../target/release/evento-server",
	} {
		if abs, err := filepath.Abs(p); err == nil {
			if _, statErr := os.Stat(abs); statErr == nil {
				return abs
			}
		}
	}
	return ""
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitListening(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server at %s did not start listening", addr)
}

func TestRoundTrip(t *testing.T) {
	addr := startServer(t)
	c, err := evento.Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	ctx := context.Background()

	const agg = "go/Account"

	// Create two events.
	created, err := c.Create(ctx, agg, []evento.NewEvent{
		{Name: "Opened", Data: []byte("a")},
		{Name: "Renamed", Data: []byte("b")},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.LastVersion != 2 {
		t.Fatalf("last version = %d, want 2", created.LastVersion)
	}
	id := created.AggregateID

	// Read them back; opaque data must match byte-for-byte.
	rr, err := c.Read(ctx, evento.ReadQuery{
		Filters: []evento.EventFilter{evento.FilterByID(agg, id)},
		Args:    evento.Forward(10, ""),
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(rr.Edges) != 2 {
		t.Fatalf("read %d edges, want 2", len(rr.Edges))
	}
	if rr.Edges[0].Node.Name != "Opened" || !bytes.Equal(rr.Edges[0].Node.Data, []byte("a")) {
		t.Fatalf("unexpected first event: %+v", rr.Edges[0].Node)
	}
	if rr.Edges[1].Node.Name != "Renamed" || !bytes.Equal(rr.Edges[1].Node.Data, []byte("b")) {
		t.Fatalf("unexpected second event: %+v", rr.Edges[1].Node)
	}

	// Append with the correct version.
	appended, err := c.Append(ctx, agg, id, 2, []evento.NewEvent{{Name: "Closed", Data: []byte("c")}})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if appended.LastVersion != 3 {
		t.Fatalf("append last version = %d, want 3", appended.LastVersion)
	}

	// Append with a stale version -> ErrInvalidOriginalVersion.
	_, err = c.Append(ctx, agg, id, 2, []evento.NewEvent{{Name: "Closed", Data: []byte("x")}})
	if !errors.Is(err, evento.ErrInvalidOriginalVersion) {
		t.Fatalf("stale append error = %v, want ErrInvalidOriginalVersion", err)
	}
}

func TestSubscribe(t *testing.T) {
	addr := startServer(t)
	c, err := evento.Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const agg = "go/Sub"
	created, err := c.Create(ctx, agg, []evento.NewEvent{
		{Name: "A", Data: []byte("1")},
		{Name: "B", Data: []byte("2")},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	got := make(chan string, 16)
	go func() {
		_ = c.Subscribe(ctx, evento.SubscribeOptions{
			Key:       "go-test-" + created.AggregateID,
			Filters:   []evento.EventFilter{evento.FilterByID(agg, created.AggregateID)},
			ChunkSize: 10,
		}, func(ev evento.Event) error {
			got <- ev.Name
			return nil
		})
	}()

	// The two seeded events replay first.
	assertNext(t, got, "A")
	assertNext(t, got, "B")

	// A live append is delivered without re-subscribing.
	if _, err := c.Append(ctx, agg, created.AggregateID, 2, []evento.NewEvent{{Name: "C", Data: []byte("3")}}); err != nil {
		t.Fatalf("append: %v", err)
	}
	assertNext(t, got, "C")
}

func assertNext(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("subscribe delivered %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %q", want)
	}
}

func TestErrorMapping(t *testing.T) {
	addr := startServer(t)
	c, err := evento.Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	// A write with no events is INVALID_ARGUMENT -> ErrInvalidArgument.
	_, err = c.Create(context.Background(), "go/Err", nil)
	if !errors.Is(err, evento.ErrInvalidArgument) {
		t.Fatalf("empty create error = %v, want ErrInvalidArgument", err)
	}
}

func TestCommitRetry(t *testing.T) {
	addr := startServer(t)
	c, err := evento.Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	ctx := context.Background()

	const agg = "go/Commit"
	created, err := c.Create(ctx, agg, []evento.NewEvent{{Name: "Opened", Data: []byte("1")}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	id := created.AggregateID

	// First attempt deliberately uses a stale (too-high) version to force one
	// conflict; the second uses the real version and succeeds.
	attempts := 0
	res, err := c.Commit(ctx, 3, func() (evento.CommitIntent, error) {
		attempts++
		page, err := c.Read(ctx, evento.ReadQuery{
			Filters: []evento.EventFilter{evento.FilterByID(agg, id)},
			Args:    evento.Backward(1, ""),
		})
		if err != nil {
			return evento.CommitIntent{}, err
		}
		version := uint16(page.Edges[0].Node.Version)
		if attempts == 1 {
			version += 5 // wrong -> conflict
		}
		return evento.CommitIntent{
			AggregateType:   agg,
			AggregateID:     id,
			OriginalVersion: version,
			Events:          []evento.NewEvent{{Name: "Tagged", Data: []byte("x")}},
		}, nil
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("expected 2 attempts (one conflict, one success), got %d", attempts)
	}
	if res.LastVersion != 2 {
		t.Fatalf("last version = %d, want 2", res.LastVersion)
	}
}

func TestSnapshot(t *testing.T) {
	addr := startServer(t)
	c, err := evento.Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	ctx := context.Background()

	const agg = "go/Snap"

	// Absent initially.
	if _, ok, err := c.GetSnapshot(ctx, agg, "v1", "id-1"); err != nil || ok {
		t.Fatalf("expected no snapshot, got ok=%v err=%v", ok, err)
	}

	// Save then read back opaque data + cursor.
	if err := c.SaveSnapshot(ctx, agg, "v1", "id-1", []byte("state"), "cur-1"); err != nil {
		t.Fatalf("save: %v", err)
	}
	snap, ok, err := c.GetSnapshot(ctx, agg, "v1", "id-1")
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(snap.Data, []byte("state")) || snap.Cursor != "cur-1" {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}

	// Revision mismatch -> not found.
	if _, ok, _ := c.GetSnapshot(ctx, agg, "v2", "id-1"); ok {
		t.Fatalf("revision mismatch must not match")
	}

	// Delete -> gone (and idempotent).
	if err := c.DeleteSnapshot(ctx, agg, "id-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok, _ := c.GetSnapshot(ctx, agg, "v1", "id-1"); ok {
		t.Fatalf("snapshot should be gone after delete")
	}
	if err := c.DeleteSnapshot(ctx, agg, "id-1"); err != nil {
		t.Fatalf("delete idempotent: %v", err)
	}
}

func TestPagination(t *testing.T) {
	addr := startServer(t)
	c, err := evento.Dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	ctx := context.Background()

	const agg = "go/Paged"
	created, err := c.Create(ctx, agg, []evento.NewEvent{
		{Name: "E1", Data: []byte("1")},
		{Name: "E2", Data: []byte("2")},
		{Name: "E3", Data: []byte("3")},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	filter := []evento.EventFilter{evento.FilterByID(agg, created.AggregateID)}

	page1, err := c.Read(ctx, evento.ReadQuery{Filters: filter, Args: evento.Forward(2, "")})
	if err != nil {
		t.Fatalf("read page1: %v", err)
	}
	if len(page1.Edges) != 2 || !page1.PageInfo.HasNextPage || page1.PageInfo.EndCursor == nil {
		t.Fatalf("page1 unexpected: edges=%d info=%+v", len(page1.Edges), page1.PageInfo)
	}

	page2, err := c.Read(ctx, evento.ReadQuery{Filters: filter, Args: evento.Forward(2, *page1.PageInfo.EndCursor)})
	if err != nil {
		t.Fatalf("read page2: %v", err)
	}
	if len(page2.Edges) != 1 || page2.PageInfo.HasNextPage {
		t.Fatalf("page2 unexpected: edges=%d info=%+v", len(page2.Edges), page2.PageInfo)
	}
}
