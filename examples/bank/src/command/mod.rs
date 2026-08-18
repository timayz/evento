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

use std::ops::Deref;

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

use evento::{Executor, Projection, metadata::Event};

use crate::aggregator::{
    AccountClosed, AccountFrozen, AccountOpened, AccountUnfrozen, MoneyDeposited, MoneyReceived,
    MoneyTransferred, MoneyWithdrawn, OverdraftLimitChanged,
};
use crate::value_object::AccountStatus;

pub struct Command<E: Executor>(pub E);

impl<E: Executor> Deref for Command<E> {
    type Target = E;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<E: Executor> Command<E> {
    pub async fn load(&self, id: impl Into<String>) -> anyhow::Result<Option<BankAccount>> {
        create_projection().load(id).execute(&self.0).await
    }
}

#[evento::projection(id = id)]
#[evento::snapshot(memory)]
pub struct BankAccount {
    pub id: String,
    pub balance: i64,
    pub status: AccountStatus,
    pub overdraft_limit: i64,
}

impl BankAccount {
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

fn create_projection<E: Executor>() -> Projection<E, BankAccount> {
    Projection::new::<crate::aggregator::BankAccount>()
        .handler(handle_money_deposit())
        .handler(handle_account_opened())
        .handler(handle_money_received())
        .handler(handle_money_withdrawn())
        .handler(handle_money_transferred())
        .handler(handle_overdraft_limit_changed())
        .handler(handle_account_closed())
        .handler(handle_account_frozen())
        .handler(handle_account_unfrozen())
        .strict()
}

#[evento::handler]
async fn handle_account_opened(
    event: Event<AccountOpened>,
    row: &mut BankAccount,
) -> anyhow::Result<()> {
    row.id = event.aggregate_id.to_owned();
    row.balance = event.data.initial_balance;
    row.status = AccountStatus::Active;
    row.overdraft_limit = 0;

    Ok(())
}

#[evento::handler]
async fn handle_money_deposit(
    event: Event<MoneyDeposited>,
    row: &mut BankAccount,
) -> anyhow::Result<()> {
    row.balance += event.data.amount;

    Ok(())
}

#[evento::handler]
async fn handle_money_withdrawn(
    event: Event<MoneyWithdrawn>,
    row: &mut BankAccount,
) -> anyhow::Result<()> {
    row.balance -= event.data.amount;

    Ok(())
}

#[evento::handler]
async fn handle_money_transferred(
    event: Event<MoneyTransferred>,
    row: &mut BankAccount,
) -> anyhow::Result<()> {
    row.balance -= event.data.amount;

    Ok(())
}

#[evento::handler]
async fn handle_money_received(
    event: Event<MoneyReceived>,
    row: &mut BankAccount,
) -> anyhow::Result<()> {
    row.balance += event.data.amount;

    Ok(())
}

#[evento::handler]
async fn handle_overdraft_limit_changed(
    event: Event<OverdraftLimitChanged>,
    row: &mut BankAccount,
) -> anyhow::Result<()> {
    row.overdraft_limit = event.data.new_limit;

    Ok(())
}

#[evento::handler]
async fn handle_account_frozen(
    _event: Event<AccountFrozen>,
    row: &mut BankAccount,
) -> anyhow::Result<()> {
    row.status = AccountStatus::Frozen;

    Ok(())
}

#[evento::handler]
async fn handle_account_unfrozen(
    _event: Event<AccountUnfrozen>,
    row: &mut BankAccount,
) -> anyhow::Result<()> {
    row.status = AccountStatus::Active;

    Ok(())
}

#[evento::handler]
async fn handle_account_closed(
    _event: Event<AccountClosed>,
    row: &mut BankAccount,
) -> anyhow::Result<()> {
    row.status = AccountStatus::Closed;

    Ok(())
}
