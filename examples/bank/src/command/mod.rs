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

use evento::{Executor, Projection, Snapshot, cursor, metadata::Event};

use crate::{
    AccountClosed, AccountFrozen, AccountOpened, AccountUnfrozen, BankAccount, MoneyDeposited,
    MoneyReceived, MoneyTransferred, MoneyWithdrawn, OverdraftLimitChanged,
    value_object::AccountStatus,
};

use once_cell::sync::Lazy;

pub static COMMAND_ROWS: Lazy<RwLock<HashMap<String, CommandData>>> = Lazy::new(Default::default);

#[evento::command]
pub struct Command {
    pub id: String,
    pub balance: i64,
    pub status: AccountStatus,
    pub overdraft_limit: i64,
    pub cursor: cursor::Value,
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

fn create_projection(id: impl Into<String>) -> Projection<CommandData> {
    Projection::new::<BankAccount>(id)
        .handler(handle_money_deposit())
        .handler(handle_account_opened())
        .handler(handle_money_received())
        .handler(handle_money_withdrawn())
        .handler(handle_money_transferred())
        .handler(handle_overdraf_limit_changed())
        .handler(handle_account_closed())
        .handler(handle_account_frozen())
        .handler(handle_account_unfrozen())
        .safety_check()
}

pub async fn load<E: Executor>(
    executor: &E,
    id: impl Into<String>,
) -> Result<Option<Command<'_, E>>, anyhow::Error> {
    let id = id.into();

    let result = create_projection(&id).execute(executor).await?;

    match result {
        Some(data) => Ok(Some(Command::new(
            id,
            data.get_cursor_version()?,
            data,
            executor,
        ))),
        _ => Ok(None),
    }
}

impl Snapshot for CommandData {
    fn get_cursor(&self) -> cursor::Value {
        self.cursor.clone()
    }

    fn set_cursor(&mut self, v: &cursor::Value) {
        self.cursor = v.clone();
    }

    async fn restore(
        _context: &evento::context::RwContext,
        id: String,
    ) -> anyhow::Result<Option<Self>> {
        let rows = COMMAND_ROWS.read().unwrap();

        Ok(rows.get(&id).cloned())
    }

    async fn take_snapshot(&self, _context: &evento::context::RwContext) -> anyhow::Result<()> {
        let mut rows = COMMAND_ROWS.write().unwrap();
        rows.insert(self.id.to_owned(), self.clone());

        Ok(())
    }
}

#[evento::debug_handler]
async fn handle_account_opened(
    event: Event<AccountOpened>,
    row: &mut CommandData,
) -> anyhow::Result<()> {
    row.balance = event.data.initial_balance;
    row.status = AccountStatus::Active;
    row.overdraft_limit = 0;

    Ok(())
}

#[evento::handler]
async fn handle_money_deposit(
    event: Event<MoneyDeposited>,
    row: &mut CommandData,
) -> anyhow::Result<()> {
    row.balance += event.data.amount;

    Ok(())
}

#[evento::handler]
async fn handle_money_withdrawn(
    event: Event<MoneyWithdrawn>,
    row: &mut CommandData,
) -> anyhow::Result<()> {
    row.balance -= event.data.amount;

    Ok(())
}

#[evento::handler]
async fn handle_money_transferred(
    event: Event<MoneyTransferred>,
    row: &mut CommandData,
) -> anyhow::Result<()> {
    row.balance -= event.data.amount;

    Ok(())
}

#[evento::handler]
async fn handle_money_received(
    event: Event<MoneyReceived>,
    row: &mut CommandData,
) -> anyhow::Result<()> {
    row.balance += event.data.amount;

    Ok(())
}

#[evento::handler]
async fn handle_overdraf_limit_changed(
    event: Event<OverdraftLimitChanged>,
    row: &mut CommandData,
) -> anyhow::Result<()> {
    row.overdraft_limit = event.data.new_limit;

    Ok(())
}

#[evento::handler]
async fn handle_account_frozen(
    _event: Event<AccountFrozen>,
    row: &mut CommandData,
) -> anyhow::Result<()> {
    row.status = AccountStatus::Frozen;

    Ok(())
}

#[evento::handler]
async fn handle_account_unfrozen(
    _event: Event<AccountUnfrozen>,
    row: &mut CommandData,
) -> anyhow::Result<()> {
    row.status = AccountStatus::Active;

    Ok(())
}

#[evento::handler]
async fn handle_account_closed(
    _event: Event<AccountClosed>,
    row: &mut CommandData,
) -> anyhow::Result<()> {
    row.status = AccountStatus::Closed;

    Ok(())
}
