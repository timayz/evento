//! The smallest end-to-end evento run: define events, execute a command
//! through the `write()` gateway, load a read model, and drain a subscription —
//! all on the embedded Fjall store (no database, no migrations).
//!
//! ```text
//! cargo run -p quickstart
//! ```

use evento::{
    metadata::Event,
    projection::Projection,
    subscription::{Context, SubscriptionBuilder},
    Executor, ProjectionAggregate,
};

// 1. Events: each variant becomes an event struct with all required traits.
#[evento::aggregate]
pub enum BankAccount {
    AccountOpened { owner: String, initial_balance: i64 },
    MoneyDeposited { amount: i64 },
}

// 2. A read model. `id = id` enables the `write()` gateway; `snapshot(memory)`
//    keeps a process-local materialized row per aggregate.
#[evento::projection(id = id)]
#[evento::snapshot(memory)]
pub struct Account {
    pub id: String,
    pub owner: String,
    pub balance: i64,
}

// 3. Projection handlers are pure: (event, &mut view).
#[evento::handler]
async fn on_opened(event: Event<AccountOpened>, row: &mut Account) -> anyhow::Result<()> {
    row.id = event.aggregate_id.to_owned();
    row.owner = event.data.owner.clone();
    row.balance = event.data.initial_balance;
    Ok(())
}

#[evento::handler]
async fn on_deposited(event: Event<MoneyDeposited>, row: &mut Account) -> anyhow::Result<()> {
    row.balance += event.data.amount;
    Ok(())
}

fn account_projection<E: Executor>() -> Projection<E, Account> {
    Projection::new::<BankAccount>()
        .handler(on_opened())
        .handler(on_deposited())
        .strict()
}

// 4. A command: load current state, guard invariants, emit events through the
//    loaded projection's `write()` gateway (optimistic concurrency built in).
//    The trailing `routing_key` parameter makes `#[evento::command]` generate
//    `deposit`, `deposit_with_routing`, and `deposit_opt` wrappers.
pub struct Command<E: Executor>(pub E);

#[evento::command]
impl<E: Executor> Command<E> {
    pub async fn deposit(
        &self,
        id: impl Into<String>,
        amount: i64,
        routing_key: Option<String>,
    ) -> anyhow::Result<()> {
        let Some(account) = account_projection().load(id).execute(&self.0).await? else {
            anyhow::bail!("account not found");
        };
        if amount <= 0 {
            anyhow::bail!("deposit amount must be positive");
        }

        account
            .write()?
            .routing_key_opt(routing_key)
            .event(&MoneyDeposited { amount })
            .commit(&self.0)
            .await?;
        Ok(())
    }
}

// 5. A subscription handler: side effects allowed, runs as events arrive.
#[evento::subscription]
async fn deposit_logger<E: Executor>(
    _ctx: &Context<'_, E>,
    event: Event<MoneyDeposited>,
) -> anyhow::Result<()> {
    println!(
        "[subscription] {} deposited on {}",
        event.data.amount, event.aggregate_id
    );
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // An embedded Fjall store in a temp directory — swap for a persistent path
    // (or a SQL pool, or a remote client) without touching the code above.
    let dir = tempfile::tempdir()?;
    let executor = evento::Fjall::open(dir.path())?;

    // Write: start a new aggregate…
    let id = evento::create()
        .event(&AccountOpened {
            owner: "Alice".into(),
            initial_balance: 100,
        })
        .commit(&executor)
        .await?;
    println!("opened account {id} with balance 100");

    // …then run a command against it (load → guard → write()).
    let cmd = Command(executor.clone());
    cmd.deposit(&id, 50).await?;
    println!("deposited 50");

    // A bad command is rejected by the guard, not written to the log.
    assert!(cmd.deposit(&id, -5).await.is_err());

    // Read: fold the events into the view.
    let account = account_projection()
        .load(&id)
        .execute(&executor)
        .await?
        .expect("account exists");
    println!(
        "loaded {}: owner={} balance={}",
        account.id, account.owner, account.balance
    );
    assert_eq!(account.balance, 150);

    // Subscribe: drain everything pending once (`start` instead of `run_once`
    // would keep processing in the background until `shutdown()`).
    SubscriptionBuilder::new("deposit-logger")
        .handler(deposit_logger())
        .run_once(&executor)
        .await?;

    println!("done");
    Ok(())
}
