use std::{collections::HashMap, sync::RwLock};

use evento::{Executor, cursor, metadata::Event, projection::Projection};
use once_cell::sync::Lazy;

use crate::{
    BankAccount, NameChanged, Owner,
    aggregator::{
        AccountClosed, AccountFrozen, AccountOpened, AccountUnfrozen, DailyWithdrawalLimitChanged,
        MoneyDeposited, MoneyReceived, MoneyTransferred, MoneyWithdrawn, OverdraftLimitChanged,
    },
    value_object::{AccountStatus, AccountType},
};

pub static ACCOUNT_DETAILS_ROWS: Lazy<RwLock<HashMap<String, AccountDetailsView>>> =
    Lazy::new(Default::default);

pub fn create_projection(id: impl Into<String>) -> Projection<AccountDetailsView> {
    Projection::new::<BankAccount>(id)
        .handler(handle_money_deposit())
        .handler(handle_account_opened())
        .handler(handle_money_received())
        .handler(handle_money_withdrawn())
        .handler(handle_money_transferred())
        .handler(handle_overdraf_limit_changed())
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
    create_projection(account_id)
        .aggregator::<Owner>(owner_id)
        .execute(executor)
        .await
}

#[derive(Default, Clone)]
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
    pub cursor: cursor::Value,
}

impl evento::Snapshot for AccountDetailsView {
    fn set_cursor(&mut self, v: &cursor::Value) {
        self.cursor = v.clone();
    }

    fn get_cursor(&self) -> cursor::Value {
        self.cursor.clone()
    }

    async fn restore(
        _context: &evento::context::RwContext,
        id: String,
        _aggregators: &HashMap<String, String>,
    ) -> anyhow::Result<Option<Self>> {
        let rows = ACCOUNT_DETAILS_ROWS.read().unwrap();

        Ok(rows.get(&id).cloned())
    }

    async fn take_snapshot(&self, _context: &evento::context::RwContext) -> anyhow::Result<()> {
        let mut rows = ACCOUNT_DETAILS_ROWS.write().unwrap();
        rows.insert(self.id.to_owned(), self.clone());

        Ok(())
    }
}

#[evento::handler]
async fn handle_account_opened(
    event: Event<AccountOpened>,
    row: &mut AccountDetailsView,
) -> anyhow::Result<()> {
    row.id = event.aggregator_id.to_owned();
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
async fn handle_overdraf_limit_changed(
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
