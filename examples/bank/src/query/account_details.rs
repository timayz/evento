use evento::{Executor, metadata::Event, projection::Projection};

use crate::{
    aggregator::{
        AccountClosed, AccountFrozen, AccountOpened, AccountUnfrozen, BankAccount,
        DailyWithdrawalLimitChanged, MoneyDeposited, MoneyReceived, MoneyTransferred,
        MoneyWithdrawn, NameChanged, OverdraftLimitChanged, Owner,
    },
    value_object::{AccountStatus, AccountType},
};

pub fn create_projection<E: Executor>() -> Projection<E, AccountDetailsView> {
    Projection::new::<BankAccount>()
        .handler(handle_money_deposit())
        .handler(handle_account_opened())
        .handler(handle_money_received())
        .handler(handle_money_withdrawn())
        .handler(handle_money_transferred())
        .handler(handle_overdraft_limit_changed())
        .handler(handle_daily_withdrawal_limit_changed())
        .handler(handle_account_closed())
        .handler(handle_account_frozen())
        .handler(handle_account_unfrozen())
        .handler(handle_owned_name_chaged())
}

pub async fn load<E: Executor>(
    executor: &E,
    account_id: impl Into<String>,
    owner_id: impl Into<String>,
) -> Result<Option<AccountDetailsView>, anyhow::Error> {
    create_projection()
        .load(account_id)
        .aggregate::<Owner>(owner_id)
        .execute(executor)
        .await
}

#[evento::projection(cursor = evento::cursor::Value)]
#[evento::snapshot(memory)]
pub struct AccountDetailsView {
    pub id: String,
    pub owner_id: String,
    pub owner_name: String,
    pub account_type: AccountType,
    pub currency: String,
    pub balance: i64,
    pub available_balance: i64,
    pub status: AccountStatus,
    pub daily_withdrawal_limit: i64,
    pub overdraft_limit: i64,
}

#[evento::handler]
async fn handle_account_opened(
    event: Event<AccountOpened>,
    row: &mut AccountDetailsView,
) -> anyhow::Result<()> {
    row.id = event.aggregate_id.to_owned();
    row.owner_id = event.data.owner_id;
    row.owner_name = event.data.owner_name;
    row.account_type = event.data.account_type;
    row.currency = event.data.currency;
    row.balance = event.data.initial_balance;
    row.available_balance = event.data.initial_balance;
    row.status = AccountStatus::Active;
    row.daily_withdrawal_limit = 1000;
    row.overdraft_limit = 0;

    Ok(())
}
#[evento::handler]
async fn handle_money_deposit(
    event: Event<MoneyDeposited>,
    row: &mut AccountDetailsView,
) -> anyhow::Result<()> {
    row.balance += event.data.amount;
    row.available_balance += event.data.amount;

    Ok(())
}

#[evento::handler]
async fn handle_money_withdrawn(
    event: Event<MoneyWithdrawn>,
    row: &mut AccountDetailsView,
) -> anyhow::Result<()> {
    row.balance -= event.data.amount;
    row.available_balance -= event.data.amount;

    Ok(())
}

#[evento::handler]
async fn handle_money_transferred(
    event: Event<MoneyTransferred>,
    row: &mut AccountDetailsView,
) -> anyhow::Result<()> {
    row.balance -= event.data.amount;
    row.available_balance -= event.data.amount;

    Ok(())
}

#[evento::handler]
async fn handle_money_received(
    event: Event<MoneyReceived>,
    row: &mut AccountDetailsView,
) -> anyhow::Result<()> {
    row.balance += event.data.amount;
    row.available_balance += event.data.amount;

    Ok(())
}

#[evento::handler]
async fn handle_daily_withdrawal_limit_changed(
    event: Event<DailyWithdrawalLimitChanged>,
    row: &mut AccountDetailsView,
) -> anyhow::Result<()> {
    row.daily_withdrawal_limit = event.data.new_limit;

    Ok(())
}

#[evento::handler]
async fn handle_overdraft_limit_changed(
    event: Event<OverdraftLimitChanged>,
    row: &mut AccountDetailsView,
) -> anyhow::Result<()> {
    row.available_balance = row.balance + event.data.new_limit;
    row.overdraft_limit = event.data.new_limit;

    Ok(())
}

#[evento::handler]
async fn handle_account_frozen(
    _event: Event<AccountFrozen>,
    row: &mut AccountDetailsView,
) -> anyhow::Result<()> {
    row.status = AccountStatus::Frozen;

    Ok(())
}

#[evento::handler]
async fn handle_account_unfrozen(
    _event: Event<AccountUnfrozen>,
    row: &mut AccountDetailsView,
) -> anyhow::Result<()> {
    row.status = AccountStatus::Active;

    Ok(())
}

#[evento::handler]
async fn handle_account_closed(
    _event: Event<AccountClosed>,
    row: &mut AccountDetailsView,
) -> anyhow::Result<()> {
    row.status = AccountStatus::Closed;

    Ok(())
}

#[evento::handler]
async fn handle_owned_name_chaged(
    event: Event<NameChanged>,
    row: &mut AccountDetailsView,
) -> anyhow::Result<()> {
    row.owner_name = event.data.value;

    Ok(())
}
