package main

import (
	"context"
	"log"
	"sync"
	"time"

	evento "github.com/timayz/evento/clients/go"
)

// projections is an in-memory read model of all accounts, kept current by a live
// subscription. It demonstrates building a projection over the evento Go SDK:
// events stream in, get folded per aggregate, and the accounts list is served
// straight from memory instead of replaying on every request.
type projections struct {
	mu    sync.RWMutex
	byID  map[string]*Account
	order []string
}

func newProjections() *projections {
	return &projections{byID: map[string]*Account{}}
}

func (p *projections) apply(ev evento.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	acc, ok := p.byID[ev.AggregateID]
	if !ok {
		acc = &Account{ID: ev.AggregateID}
		p.byID[ev.AggregateID] = acc
		p.order = append(p.order, ev.AggregateID)
	}
	_ = acc.apply(ev.Name, ev.Data)
	acc.Version = uint16(ev.Version)
	acc.Exists = true
}

// list returns a snapshot copy of the accounts in first-seen order.
func (p *projections) list() []Account {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]Account, 0, len(p.order))
	for _, id := range p.order {
		out = append(out, *p.byID[id])
	}
	return out
}

// run keeps the projection current. It uses a fresh per-boot subscription key so
// the cursor starts at zero and the whole log replays into the (non-persistent)
// in-memory cache, then tails live events. It retries until ctx is cancelled.
func (p *projections) run(ctx context.Context, c *evento.Client) {
	key := "bank-web/accounts-" + newID()
	for ctx.Err() == nil {
		err := c.Subscribe(ctx, evento.SubscribeOptions{
			Key:       key,
			Filters:   []evento.EventFilter{evento.FilterByType(AggregateType)},
			ChunkSize: 200,
		}, func(ev evento.Event) error {
			p.apply(ev)
			return nil
		})
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("projection subscription error: %v (retrying)", err)
			time.Sleep(time.Second)
		}
	}
}
