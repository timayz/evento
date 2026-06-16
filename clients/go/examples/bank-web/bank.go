package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	evento "github.com/timayz/evento/clients/go"
)

// AggregateType is the evento aggregate type for bank accounts. Event payloads
// are encoded as JSON — the evento server stores them as opaque bytes, so the
// encoding is entirely this client's choice.
const AggregateType = "bank/Account"

// Event names.
const (
	EvAccountOpened    = "AccountOpened"
	EvMoneyDeposited   = "MoneyDeposited"
	EvMoneyWithdrawn   = "MoneyWithdrawn"
	EvMoneyTransferred = "MoneyTransferred"
	EvMoneyReceived    = "MoneyReceived"
)

// ---- event payloads ----

type AccountOpened struct {
	OwnerID        string `json:"owner_id"`
	OwnerName      string `json:"owner_name"`
	AccountType    string `json:"account_type"`
	Currency       string `json:"currency"`
	InitialBalance int64  `json:"initial_balance"`
}

type MoneyDeposited struct {
	Amount        int64  `json:"amount"`
	TransactionID string `json:"transaction_id"`
	Description   string `json:"description"`
}

type MoneyWithdrawn struct {
	Amount        int64  `json:"amount"`
	TransactionID string `json:"transaction_id"`
	Description   string `json:"description"`
}

type MoneyTransferred struct {
	Amount        int64  `json:"amount"`
	ToAccountID   string `json:"to_account_id"`
	TransactionID string `json:"transaction_id"`
	Description   string `json:"description"`
}

type MoneyReceived struct {
	Amount        int64  `json:"amount"`
	FromAccountID string `json:"from_account_id"`
	TransactionID string `json:"transaction_id"`
	Description   string `json:"description"`
}

// ---- aggregate (read side) ----

type Status string

const (
	Active Status = "Active"
	Frozen Status = "Frozen"
	Closed Status = "Closed"
)

// Account is the folded state of a bank account aggregate.
type Account struct {
	ID        string
	OwnerName string
	Currency  string
	Balance   int64
	Status    Status
	Version   uint16 // current version, for optimistic concurrency on append
	Exists    bool
}

func (a *Account) apply(name string, data []byte) error {
	switch name {
	case EvAccountOpened:
		var e AccountOpened
		if err := json.Unmarshal(data, &e); err != nil {
			return err
		}
		a.OwnerName = e.OwnerName
		a.Currency = e.Currency
		a.Balance = e.InitialBalance
		a.Status = Active
	case EvMoneyDeposited:
		var e MoneyDeposited
		if err := json.Unmarshal(data, &e); err != nil {
			return err
		}
		a.Balance += e.Amount
	case EvMoneyWithdrawn:
		var e MoneyWithdrawn
		if err := json.Unmarshal(data, &e); err != nil {
			return err
		}
		a.Balance -= e.Amount
	case EvMoneyTransferred:
		var e MoneyTransferred
		if err := json.Unmarshal(data, &e); err != nil {
			return err
		}
		a.Balance -= e.Amount
	case EvMoneyReceived:
		var e MoneyReceived
		if err := json.Unmarshal(data, &e); err != nil {
			return err
		}
		a.Balance += e.Amount
	}
	return nil
}

func newEvent(name string, payload any) (evento.NewEvent, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return evento.NewEvent{}, err
	}
	return evento.NewEvent{Name: name, Data: data}, nil
}

// accountRevision is the snapshot schema version. Bump it whenever the snapshot
// shape or fold logic changes, so stale snapshots are ignored and rebuilt.
const accountRevision = "v1"

// accountSnapshot is the serialized form of an account's folded state.
type accountSnapshot struct {
	OwnerName string `json:"owner_name"`
	Currency  string `json:"currency"`
	Balance   int64  `json:"balance"`
	Status    Status `json:"status"`
	Version   uint16 `json:"version"`
}

// loadAccount rebuilds an aggregate's state. It restores from a snapshot when
// one exists and replays only the events after the snapshot's cursor; otherwise
// it replays from the start. If new events were applied, it saves a refreshed
// snapshot so the next load is cheaper.
func loadAccount(ctx context.Context, c *evento.Client, id string) (*Account, error) {
	acc := &Account{ID: id}
	after := ""

	if snap, ok, err := c.GetSnapshot(ctx, AggregateType, accountRevision, id); err != nil {
		return nil, err
	} else if ok {
		var s accountSnapshot
		if err := json.Unmarshal(snap.Data, &s); err != nil {
			return nil, err
		}
		acc.OwnerName = s.OwnerName
		acc.Currency = s.Currency
		acc.Balance = s.Balance
		acc.Status = s.Status
		acc.Version = s.Version
		acc.Exists = true
		after = snap.Cursor
	}

	lastCursor := ""
	for {
		page, err := c.Read(ctx, evento.ReadQuery{
			Filters: []evento.EventFilter{evento.FilterByID(AggregateType, id)},
			Args:    evento.Forward(100, after),
		})
		if err != nil {
			return nil, err
		}
		for _, edge := range page.Edges {
			if err := acc.apply(edge.Node.Name, edge.Node.Data); err != nil {
				return nil, err
			}
			acc.Version = uint16(edge.Node.Version)
			acc.Exists = true
			lastCursor = edge.Cursor
		}
		if !page.PageInfo.HasNextPage || page.PageInfo.EndCursor == nil {
			break
		}
		after = *page.PageInfo.EndCursor
	}

	// Persist a fresh snapshot only when we folded new events past the snapshot.
	if acc.Exists && lastCursor != "" {
		data, err := json.Marshal(accountSnapshot{
			OwnerName: acc.OwnerName,
			Currency:  acc.Currency,
			Balance:   acc.Balance,
			Status:    acc.Status,
			Version:   acc.Version,
		})
		if err != nil {
			return nil, err
		}
		if err := c.SaveSnapshot(ctx, AggregateType, accountRevision, id, data, lastCursor); err != nil {
			return nil, err
		}
	}

	return acc, nil
}

// ---- commands (write side) ----

var (
	errNotFound      = errors.New("account not found")
	errNotActive     = errors.New("account is not active")
	errInvalidAmount = errors.New("amount must be positive")
	errInsufficient  = errors.New("insufficient funds")
)

func openAccount(ctx context.Context, c *evento.Client, ownerName, currency string, initialBalance int64) (string, error) {
	if ownerName == "" {
		return "", errors.New("owner name is required")
	}
	if currency == "" {
		return "", errors.New("currency is required")
	}
	if initialBalance < 0 {
		return "", errInvalidAmount
	}
	ev, err := newEvent(EvAccountOpened, AccountOpened{
		OwnerID:        newID(),
		OwnerName:      ownerName,
		AccountType:    "Checking",
		Currency:       currency,
		InitialBalance: initialBalance,
	})
	if err != nil {
		return "", err
	}
	res, err := c.Create(ctx, AggregateType, []evento.NewEvent{ev})
	if err != nil {
		return "", err
	}
	return res.AggregateID, nil
}

// commitAttempts bounds optimistic-concurrency retries for a command.
const commitAttempts = 5

func deposit(ctx context.Context, c *evento.Client, id string, amount int64) error {
	// Commit reloads + revalidates on each attempt, retrying if a concurrent
	// write bumped the version between load and append.
	_, err := c.Commit(ctx, commitAttempts, func() (evento.CommitIntent, error) {
		acc, err := loadAccount(ctx, c, id)
		if err != nil {
			return evento.CommitIntent{}, err
		}
		if !acc.Exists {
			return evento.CommitIntent{}, errNotFound
		}
		if acc.Status != Active {
			return evento.CommitIntent{}, errNotActive
		}
		if amount <= 0 {
			return evento.CommitIntent{}, errInvalidAmount
		}
		ev, err := newEvent(EvMoneyDeposited, MoneyDeposited{
			Amount:        amount,
			TransactionID: newID(),
			Description:   "Web deposit",
		})
		if err != nil {
			return evento.CommitIntent{}, err
		}
		return evento.CommitIntent{
			AggregateType:   AggregateType,
			AggregateID:     id,
			OriginalVersion: acc.Version,
			Events:          []evento.NewEvent{ev},
		}, nil
	})
	return err
}

func withdraw(ctx context.Context, c *evento.Client, id string, amount int64) error {
	_, err := c.Commit(ctx, commitAttempts, func() (evento.CommitIntent, error) {
		acc, err := loadAccount(ctx, c, id)
		if err != nil {
			return evento.CommitIntent{}, err
		}
		if !acc.Exists {
			return evento.CommitIntent{}, errNotFound
		}
		if acc.Status != Active {
			return evento.CommitIntent{}, errNotActive
		}
		if amount <= 0 {
			return evento.CommitIntent{}, errInvalidAmount
		}
		if acc.Balance < amount {
			return evento.CommitIntent{}, errInsufficient
		}
		ev, err := newEvent(EvMoneyWithdrawn, MoneyWithdrawn{
			Amount:        amount,
			TransactionID: newID(),
			Description:   "Web withdrawal",
		})
		if err != nil {
			return evento.CommitIntent{}, err
		}
		return evento.CommitIntent{
			AggregateType:   AggregateType,
			AggregateID:     id,
			OriginalVersion: acc.Version,
			Events:          []evento.NewEvent{ev},
		}, nil
	})
	return err
}

// transfer debits the source and credits the destination, mirroring the
// transfer with a matching receive so both balances stay consistent. Each leg
// is its own optimistic-concurrency commit.
func transfer(ctx context.Context, c *evento.Client, fromID, toID string, amount int64) error {
	// Pre-check the destination exists before moving any money.
	if to, err := loadAccount(ctx, c, toID); err != nil {
		return err
	} else if !to.Exists {
		return fmt.Errorf("destination %s: %w", toID, errNotFound)
	}

	txID := newID()

	// Debit the source.
	_, err := c.Commit(ctx, commitAttempts, func() (evento.CommitIntent, error) {
		from, err := loadAccount(ctx, c, fromID)
		if err != nil {
			return evento.CommitIntent{}, err
		}
		if !from.Exists {
			return evento.CommitIntent{}, errNotFound
		}
		if from.Status != Active {
			return evento.CommitIntent{}, errNotActive
		}
		if amount <= 0 {
			return evento.CommitIntent{}, errInvalidAmount
		}
		if from.Balance < amount {
			return evento.CommitIntent{}, errInsufficient
		}
		sent, err := newEvent(EvMoneyTransferred, MoneyTransferred{
			Amount:        amount,
			ToAccountID:   toID,
			TransactionID: txID,
			Description:   "Web transfer",
		})
		if err != nil {
			return evento.CommitIntent{}, err
		}
		return evento.CommitIntent{
			AggregateType:   AggregateType,
			AggregateID:     fromID,
			OriginalVersion: from.Version,
			Events:          []evento.NewEvent{sent},
		}, nil
	})
	if err != nil {
		return err
	}

	// Credit the destination.
	_, err = c.Commit(ctx, commitAttempts, func() (evento.CommitIntent, error) {
		to, err := loadAccount(ctx, c, toID)
		if err != nil {
			return evento.CommitIntent{}, err
		}
		if !to.Exists {
			return evento.CommitIntent{}, fmt.Errorf("destination %s: %w", toID, errNotFound)
		}
		received, err := newEvent(EvMoneyReceived, MoneyReceived{
			Amount:        amount,
			FromAccountID: fromID,
			TransactionID: txID,
			Description:   "Web transfer",
		})
		if err != nil {
			return evento.CommitIntent{}, err
		}
		return evento.CommitIntent{
			AggregateType:   AggregateType,
			AggregateID:     toID,
			OriginalVersion: to.Version,
			Events:          []evento.NewEvent{received},
		}, nil
	})
	return err
}
