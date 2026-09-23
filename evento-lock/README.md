# evento-lock

Keeps the shapes an [evento](https://github.com/timayz/evento) application persists
append-only.

Events are stored as positional, packed bitcode: no field names on disk, no tolerance
for an extra or missing field, and an enum's discriminant is packed to the number of
variants it had when it was written. Add a field to an event — or to a type nested in
one — and every stored occurrence stops decoding. No ordinary test notices.

`evento-lock` scans your workspace with `syn`, writes the persisted shapes to
`events.lock`, and fails a test when a line that is already there changes.

## Usage

```toml
[dev-dependencies]
evento-lock = "2.0.0-alpha.28"
```

```rust,no_run
// tests/events_lock.rs
#[test]
fn persisted_shapes_only_grow() {
    evento_lock::check(env!("CARGO_MANIFEST_DIR")).unwrap();
}
```

```text
cargo test                      # verify — what CI runs
EVENTO_LOCK=update cargo test   # record new events, types and views; refuses breaking changes
EVENTO_LOCK=force  cargo test   # record anything — only for shapes no deployed database has stored
```

By default every package of the workspace is scanned and the lock lives at
`<workspace root>/events.lock`. `Config` narrows it:

```rust,no_run
evento_lock::Config::new(env!("CARGO_MANIFEST_DIR"))
    .packages(["billing", "shipping"])
    .lock_path("domain/events.lock")
    .require_pinned_names(true)
    .mode_from_env()
    .run()
    .unwrap();
```

## What is in the lock

```text
event bank/BankAccount::AccountOpened { owner_id: String, owner_name: String, account_type: AccountType, currency: String, initial_balance: i64 }
type bank::value_object::AccountType enum { Checking, Savings, Business }
view bank::query::account_balance::AccountBalanceView rev=0 { balance: i64, currency: String, available_balance: i64, cursor: String, aggregate_version: u16 }
```

| Line | What | Rule |
|---|---|---|
| `event <aggregate>::<name>` | a variant of an `#[evento::aggregate]` enum, keyed by the aggregate name (`name = ".."` or `<package>/<Enum>`) and the stored event name (`#[evento(name = "..")]` or the variant) | **frozen**: never edited, renamed or removed |
| `type <module>::<Name>` | a `bitcode::Encode` struct or enum an event reaches, transitively | **frozen**, enums included |
| `view <projection name> rev=<n>` | a projection snapshotted through the executor (`#[evento::projection(bitcode::Encode, bitcode::Decode)]`), including the `cursor` and `aggregate_version` fields the macro adds, plus the `Encode` types only it uses | **may change** when `.revision(n)` grows |

`#[evento::snapshot(none | memory)]` state and SQL read models are not persisted as
bitcode, so they are not in the lock.

## When the test fails

- **An event or type changed.** Restore it. Record the new information as a companion
  event committed in the same batch, or add a new variant and point the old one at it
  with `#[evento(upcast_to = NewVariant)]`. Renaming a variant is fine when its stored
  name is pinned with `#[evento(name = "..")]`.
- **An event or type was removed.** Restore it: an event that is no longer written
  still has to be read for as long as a database holds one.
- **A view changed and kept its revision.** Bump the projection's `.revision(n)` in the
  same commit, so snapshots taken with the old shape are rebuilt rather than mis-decoded.
- **The lock is out of date.** You added something: run `EVENTO_LOCK=update cargo test`
  and commit `events.lock`.

## How revisions are found

For each `.revision(<literal>)`, the view is taken from the enclosing function's return
type (`fn create_projection<E: Executor>() -> Projection<E, MyView>`), else from a
`Projection::<E, MyView>` turbofish, else from the only snapshotted view in the file.
Anything else is refused as ambiguous.

## Limits

The scanner reads sources, it does not compile them. Not tracked:

- metadata values (`WriteBuilder::metadata`) and `#[derive(Cursor)]` tokens;
- types from other crates, type aliases and macro-generated types (a name with no
  `Encode` definition in the workspace is simply not frozen);
- `#[cfg(test)]` items (never stored), and revisions that are not integer literals.

A name mentioned by an event is looked up in the same module, then the same package,
then anywhere: a wrong guess freezes one type too many, never too few. Two definitions
that land on the same lock key (e.g. `cfg`-gated duplicates) are refused.
