mod change_daily_withdrawal_limit;
mod change_overdraft_limit;
mod close_account;
mod deposit_money;
mod freeze_account;
mod open_account;
mod receive_money;
mod transfer_money;
mod unfreeze_account;
mod withdraw_money;

use std::{collections::HashMap, sync::RwLock};

pub use change_daily_withdrawal_limit::*;
pub use change_overdraft_limit::*;
pub use close_account::*;
pub use deposit_money::*;
pub use freeze_account::*;
pub use open_account::*;
pub use receive_money::*;
pub use transfer_money::*;
pub use unfreeze_account::*;
pub use withdraw_money::*;

use evento::{Action, Executor, LoadResult, Projection, SubscriptionBuilder, metadata::Event};

use crate::{
    AccountClosed, AccountFrozen, AccountOpened, AccountUnfrozen, BankAccount, MoneyDeposited,
    MoneyReceived, MoneyTransferred, MoneyWithdrawn, OverdraftLimitChanged,
    value_object::AccountStatus,
};

use once_cell::sync::Lazy;

pub type LazyCommandrow = (CommandData, u16, Option<String>);

pub static COMMAND_ROWS: Lazy<RwLock<HashMap<String, LazyCommandrow>>> =
    Lazy::new(Default::default);

#[evento::command]
pub struct Command {
    pub balance: i64,
    pub status: AccountStatus,
    pub overdraft_limit: i64,
}

impl<'a, E: Executor> Command<'a, E> {
    pub fn can_withdraw(&self, amount: i64) -> bool {
        matches!(self.status, AccountStatus::Active)
            && amount > 0
            && (self.balance + self.overdraft_limit) >= amount
    }

    pub fn can_transfer(&self, amount: i64) -> bool {
        self.can_withdraw(amount)
    }

    pub fn can_deposit(&self, amount: i64) -> bool {
        matches!(self.status, AccountStatus::Active) && amount > 0
    }

    pub fn can_close(&self) -> bool {
        matches!(self.status, AccountStatus::Active) && self.balance >= 0
    }

    pub fn is_active(&self) -> bool {
        matches!(self.status, AccountStatus::Active)
    }

    pub fn is_frozen(&self) -> bool {
        matches!(self.status, AccountStatus::Frozen)
    }

    pub fn is_closed(&self) -> bool {
        matches!(self.status, AccountStatus::Closed)
    }
}

fn create_projection<E: Executor>() -> Projection<CommandData, E> {
    Projection::new("command")
        .handler(handle_money_deposit())
        .handler(handle_account_opened())
        .handler(handle_money_received())
        .handler(handle_money_withdrawn())
        .handler(handle_money_transferred())
        .handler(handle_overdraf_limit_changed())
        .handler(handle_account_closed())
        .handler(handle_account_frozen())
        .handler(handle_account_unfrozen())
}

pub async fn load<E: Executor>(
    executor: &E,
    id: impl Into<String>,
) -> Result<Option<Command<'_, E>>, anyhow::Error> {
    let id = id.into();

    Ok(create_projection()
        .load::<BankAccount>(&id)
        .filter_events_by_name(false)
        .execute(executor)
        .await?
        .map(|loaded| Command::new(id, loaded, executor)))
}

pub fn subscription<E: Executor>() -> SubscriptionBuilder<CommandData, E> {
    create_projection().subscription()
}

#[evento::snapshot]
async fn restore(
    _context: &evento::context::RwContext,
    id: String,
) -> anyhow::Result<Option<LoadResult<CommandData>>> {
    let rows = COMMAND_ROWS.read().unwrap();

    Ok(rows
        .get(&id)
        .cloned()
        .map(|(item, version, routing_key)| LoadResult {
            item,
            version,
            routing_key,
        }))
}

#[evento::handler]
async fn handle_account_opened<E: Executor>(
    event: Event<AccountOpened>,
    action: Action<'_, CommandData, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.balance = event.data.initial_balance;
            row.status = AccountStatus::Active;
            row.overdraft_limit = 0;
        }
        Action::Handle(_context) => {
            let mut rows = COMMAND_ROWS.write().unwrap();
            rows.insert(
                event.aggregator_id.to_owned(),
                (
                    CommandData {
                        balance: event.data.initial_balance,
                        status: AccountStatus::Active,
                        overdraft_limit: 0,
                    },
                    event.version,
                    event.routing_key.to_owned(),
                ),
            );
        }
    };

    Ok(())
}

#[evento::handler]
async fn handle_money_deposit<E: Executor>(
    event: Event<MoneyDeposited>,
    action: Action<'_, CommandData, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.balance += event.data.amount;
        }
        Action::Handle(_context) => {
            let mut rows = COMMAND_ROWS.write().unwrap();
            if let Some(row) = rows.get_mut(&event.aggregator_id) {
                row.0.balance += event.data.amount;
                row.1 = event.version;
                row.2 = event.routing_key.to_owned();
            }
        }
    };

    Ok(())
}

#[evento::handler]
async fn handle_money_withdrawn<E: Executor>(
    event: Event<MoneyWithdrawn>,
    action: Action<'_, CommandData, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.balance -= event.data.amount;
        }
        Action::Handle(_context) => {
            let mut rows = COMMAND_ROWS.write().unwrap();
            if let Some(row) = rows.get_mut(&event.aggregator_id) {
                row.0.balance -= event.data.amount;
                row.1 = event.version;
                row.2 = event.routing_key.to_owned();
            }
        }
    };

    Ok(())
}

#[evento::handler]
async fn handle_money_transferred<E: Executor>(
    event: Event<MoneyTransferred>,
    action: Action<'_, CommandData, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.balance -= event.data.amount;
        }
        Action::Handle(_context) => {
            let mut rows = COMMAND_ROWS.write().unwrap();
            if let Some(row) = rows.get_mut(&event.aggregator_id) {
                row.0.balance -= event.data.amount;
                row.1 = event.version;
                row.2 = event.routing_key.to_owned();
            }
        }
    };

    Ok(())
}

#[evento::handler]
async fn handle_money_received<E: Executor>(
    event: Event<MoneyReceived>,
    action: Action<'_, CommandData, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.balance += event.data.amount;
        }
        Action::Handle(_context) => {
            let mut rows = COMMAND_ROWS.write().unwrap();
            if let Some(row) = rows.get_mut(&event.aggregator_id) {
                row.0.balance += event.data.amount;
                row.1 = event.version;
                row.2 = event.routing_key.to_owned();
            }
        }
    };

    Ok(())
}

#[evento::handler]
async fn handle_overdraf_limit_changed<E: Executor>(
    event: Event<OverdraftLimitChanged>,
    action: Action<'_, CommandData, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.overdraft_limit = event.data.new_limit;
        }
        Action::Handle(_context) => {
            let mut rows = COMMAND_ROWS.write().unwrap();
            if let Some(row) = rows.get_mut(&event.aggregator_id) {
                row.0.overdraft_limit += event.data.new_limit;
                row.1 = event.version;
                row.2 = event.routing_key.to_owned();
            }
        }
    };

    Ok(())
}

#[evento::handler]
async fn handle_account_frozen<E: Executor>(
    event: Event<AccountFrozen>,
    action: Action<'_, CommandData, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.status = AccountStatus::Frozen;
        }
        Action::Handle(_context) => {
            let mut rows = COMMAND_ROWS.write().unwrap();
            if let Some(row) = rows.get_mut(&event.aggregator_id) {
                row.0.status = AccountStatus::Frozen;
                row.1 = event.version;
                row.2 = event.routing_key.to_owned();
            }
        }
    };

    Ok(())
}

#[evento::handler]
async fn handle_account_unfrozen<E: Executor>(
    event: Event<AccountUnfrozen>,
    action: Action<'_, CommandData, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.status = AccountStatus::Active;
        }
        Action::Handle(_context) => {
            let mut rows = COMMAND_ROWS.write().unwrap();
            if let Some(row) = rows.get_mut(&event.aggregator_id) {
                row.0.status = AccountStatus::Active;
                row.1 = event.version;
                row.2 = event.routing_key.to_owned();
            }
        }
    };

    Ok(())
}

#[evento::handler]
async fn handle_account_closed<E: Executor>(
    event: Event<AccountClosed>,
    action: Action<'_, CommandData, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.status = AccountStatus::Closed;
        }
        Action::Handle(_context) => {
            let mut rows = COMMAND_ROWS.write().unwrap();
            if let Some(row) = rows.get_mut(&event.aggregator_id) {
                row.0.status = AccountStatus::Closed;
                row.1 = event.version;
                row.2 = event.routing_key.to_owned();
            }
        }
    };

    Ok(())
}
