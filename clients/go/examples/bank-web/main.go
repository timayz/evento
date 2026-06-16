// Command bank-web is a small web app demonstrating the evento Go SDK.
//
// It mirrors the Rust `examples/bank-axum-sqlite` example, but talks to a
// running `evento-server` over gRPC instead of embedding the event store. The
// aggregate fold and the account projection are reimplemented client-side (see
// bank.go) because the gRPC surface is the raw event store — events in, events
// out — with payloads as opaque (here JSON) bytes.
//
// Run an event store first, then this app:
//
//	cargo run -p evento-server                      # listens on 127.0.0.1:50051
//	cd clients/go/examples/bank-web && go run .      # serves http://127.0.0.1:3000
//
// Configure with EVENTO_GRPC_ADDR (default 127.0.0.1:50051) and ADDR (default
// 127.0.0.1:3000).
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"html/template"
	"log"
	"net/http"
	"os"
	"strconv"

	evento "github.com/timayz/evento/clients/go"
)

type server struct {
	client *evento.Client
	tmpl   *template.Template
	proj   *projections
}

func main() {
	grpcAddr := envOr("EVENTO_GRPC_ADDR", "127.0.0.1:50051")
	httpAddr := envOr("ADDR", "127.0.0.1:3000")

	// WithWaitForReady so requests block briefly while the server starts up
	// (e.g. under `make bank-go`) instead of failing the first call.
	client, err := evento.Dial(grpcAddr, evento.WithWaitForReady())
	if err != nil {
		log.Fatalf("connect to evento at %s: %v", grpcAddr, err)
	}
	defer client.Close()

	// Build the live account projection and keep it current in the background.
	proj := newProjections()
	go proj.run(context.Background(), client)

	s := &server{
		client: client,
		tmpl:   template.Must(template.New("").Parse(templates)),
		proj:   proj,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /", s.index)
	mux.HandleFunc("GET /accounts", s.listAccounts)
	mux.HandleFunc("GET /accounts/new", s.newAccountForm)
	mux.HandleFunc("POST /accounts", s.createAccount)
	mux.HandleFunc("GET /accounts/{id}", s.viewAccount)
	mux.HandleFunc("POST /accounts/{id}/deposit", s.deposit)
	mux.HandleFunc("POST /accounts/{id}/withdraw", s.withdraw)
	mux.HandleFunc("POST /accounts/{id}/transfer", s.transfer)

	log.Printf("bank-web listening on http://%s (evento at %s)", httpAddr, grpcAddr)
	if err := http.ListenAndServe(httpAddr, mux); err != nil {
		log.Fatal(err)
	}
}

// ---- handlers ----

func (s *server) index(w http.ResponseWriter, r *http.Request) {
	s.render(w, "index", nil)
}

func (s *server) listAccounts(w http.ResponseWriter, r *http.Request) {
	// Served from the live projection (no replay on each request).
	s.render(w, "list", map[string]any{"Accounts": s.proj.list()})
}

func (s *server) newAccountForm(w http.ResponseWriter, r *http.Request) {
	s.render(w, "new", nil)
}

func (s *server) createAccount(w http.ResponseWriter, r *http.Request) {
	id, err := openAccount(
		r.Context(), s.client,
		r.FormValue("owner_name"),
		r.FormValue("currency"),
		formInt(r, "initial_balance"),
	)
	if err != nil {
		s.fail(w, err)
		return
	}
	http.Redirect(w, r, "/accounts/"+id, http.StatusSeeOther)
}

func (s *server) viewAccount(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	acc, err := loadAccount(r.Context(), s.client, id)
	if err != nil {
		s.fail(w, err)
		return
	}
	if !acc.Exists {
		http.Error(w, "account not found", http.StatusNotFound)
		return
	}
	// The single account is loaded by replay (strongly consistent, e.g. right
	// after a write); the transfer dropdown uses the live projection.
	s.render(w, "view", map[string]any{"Account": acc, "Accounts": s.proj.list()})
}

func (s *server) deposit(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := deposit(r.Context(), s.client, id, formInt(r, "amount")); err != nil {
		s.fail(w, err)
		return
	}
	http.Redirect(w, r, "/accounts/"+id, http.StatusSeeOther)
}

func (s *server) withdraw(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := withdraw(r.Context(), s.client, id, formInt(r, "amount")); err != nil {
		s.fail(w, err)
		return
	}
	http.Redirect(w, r, "/accounts/"+id, http.StatusSeeOther)
}

func (s *server) transfer(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	err := transfer(r.Context(), s.client, id, r.FormValue("to_account_id"), formInt(r, "amount"))
	if err != nil {
		s.fail(w, err)
		return
	}
	http.Redirect(w, r, "/accounts/"+id, http.StatusSeeOther)
}

// ---- helpers ----

func (s *server) render(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *server) fail(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusBadRequest)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func formInt(r *http.Request, key string) int64 {
	n, _ := strconv.ParseInt(r.FormValue(key), 10, 64)
	return n
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
