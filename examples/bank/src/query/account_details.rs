use std::{collections::HashMap, sync::RwLock};

use evento::{
    Executor, LoadResult,
    metadata::Event,
    projection::{Action, Projection},
};
use once_cell::sync::Lazy;

use crate::{
    NameChanged, Owner,
    aggregator::{
        AccountClosed, AccountFrozen, AccountOpened, AccountUnfrozen, DailyWithdrawalLimitChanged,
        MoneyDeposited, MoneyReceived, MoneyTransferred, MoneyWithdrawn, OverdraftLimitChanged,
    },
    value_object::{AccountStatus, AccountType},
};

pub type LazyAccountDetailsRow = (AccountDetailsView, u16, Option<String>);

pub static ACCOUNT_DETAILS_ROWS: Lazy<RwLock<HashMap<String, LazyAccountDetailsRow>>> =
    Lazy::new(Default::default);

pub fn create_projection<E: Executor>() -> Projection<AccountDetailsView, E> {
    Projection::new("account-details-view")
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

pub fn subscription<E: Executor>() -> evento::SubscriptionBuilder<AccountDetailsView, E> {
    create_projection().subscription()
}

pub async fn load<E: Executor>(
    executor: &E,
    account_id: impl Into<String>,
    owner_id: impl Into<String>,
) -> Result<Option<LoadResult<AccountDetailsView>>, anyhow::Error> {
    use crate::BankAccount;
    create_projection()
        .load::<BankAccount>(account_id)
        .aggregator::<Owner>(owner_id)
        .execute(executor)
        .await
}

#[derive(Default)]
pub struct AccountDetailsView {
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

#[evento::snapshot]
async fn restore(
    _context: &evento::context::RwContext,
    _id: String,
) -> anyhow::Result<Option<AccountDetailsView>> {
    Ok(None)
}

#[evento::handler]
async fn handle_account_opened<E: Executor>(
    event: Event<AccountOpened>,
    action: Action<'_, AccountDetailsView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.owner_id = event.data.owner_id;
            row.owner_name = event.data.owner_name;
            row.account_type = event.data.account_type;
            row.currency = event.data.currency;
            row.balance = event.data.initial_balance;
            row.available_balance = event.data.initial_balance;
            row.status = AccountStatus::Active;
            row.daily_withdrawal_limit = 1000;
            row.overdraft_limit = 0;
        }
        Action::Handle(_context) => {
            let mut rows = ACCOUNT_DETAILS_ROWS.write().unwrap();
            rows.insert(
                event.aggregator_id.to_owned(),
                (
                    AccountDetailsView {
                        owner_id: event.data.owner_id.to_owned(),
                        owner_name: event.data.owner_name.to_owned(),
                        account_type: event.data.account_type.to_owned(),
                        currency: event.data.currency.to_owned(),
                        balance: event.data.initial_balance,
                        available_balance: event.data.initial_balance,
                        status: AccountStatus::Active,
                        daily_withdrawal_limit: 1000,
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
    action: Action<'_, AccountDetailsView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.balance += event.data.amount;
            row.available_balance += event.data.amount;
        }
        Action::Handle(_context) => {
            let mut rows = ACCOUNT_DETAILS_ROWS.write().unwrap();
            if let Some(row) = rows.get_mut(&event.aggregator_id) {
                row.0.balance += event.data.amount;
                row.0.available_balance += event.data.amount;
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
    action: Action<'_, AccountDetailsView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.balance -= event.data.amount;
            row.available_balance -= event.data.amount;
        }
        Action::Handle(_context) => {
            let mut rows = ACCOUNT_DETAILS_ROWS.write().unwrap();
            if let Some(row) = rows.get_mut(&event.aggregator_id) {
                row.0.balance -= event.data.amount;
                row.0.available_balance -= event.data.amount;
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
    action: Action<'_, AccountDetailsView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.balance -= event.data.amount;
            row.available_balance -= event.data.amount;
        }
        Action::Handle(_context) => {
            let mut rows = ACCOUNT_DETAILS_ROWS.write().unwrap();
            if let Some(row) = rows.get_mut(&event.aggregator_id) {
                row.0.balance -= event.data.amount;
                row.0.available_balance -= event.data.amount;
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
    action: Action<'_, AccountDetailsView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.balance += event.data.amount;
            row.available_balance += event.data.amount;
        }
        Action::Handle(_context) => {
            let mut rows = ACCOUNT_DETAILS_ROWS.write().unwrap();
            if let Some(row) = rows.get_mut(&event.aggregator_id) {
                row.0.balance += event.data.amount;
                row.0.available_balance += event.data.amount;
                row.1 = event.version;
                row.2 = event.routing_key.to_owned();
            }
        }
    };

    Ok(())
}

#[evento::handler]
async fn handle_daily_withdrawal_limit_changed<E: Executor>(
    event: Event<DailyWithdrawalLimitChanged>,
    action: Action<'_, AccountDetailsView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.daily_withdrawal_limit = event.data.new_limit;
        }
        Action::Handle(_context) => {
            let mut rows = ACCOUNT_DETAILS_ROWS.write().unwrap();
            if let Some(row) = rows.get_mut(&event.aggregator_id) {
                row.0.daily_withdrawal_limit = event.data.new_limit;
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
    action: Action<'_, AccountDetailsView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.available_balance = row.balance + event.data.new_limit;
            row.overdraft_limit = event.data.new_limit;
        }
        Action::Handle(_context) => {
            let mut rows = ACCOUNT_DETAILS_ROWS.write().unwrap();
            if let Some(row) = rows.get_mut(&event.aggregator_id) {
                row.0.available_balance = row.0.balance + event.data.new_limit;
                row.0.overdraft_limit = event.data.new_limit;
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
    action: Action<'_, AccountDetailsView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.status = AccountStatus::Frozen;
        }
        Action::Handle(_context) => {
            let mut rows = ACCOUNT_DETAILS_ROWS.write().unwrap();
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
    action: Action<'_, AccountDetailsView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.status = AccountStatus::Active;
        }
        Action::Handle(_context) => {
            let mut rows = ACCOUNT_DETAILS_ROWS.write().unwrap();
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
    action: Action<'_, AccountDetailsView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.status = AccountStatus::Closed;
        }
        Action::Handle(_context) => {
            let mut rows = ACCOUNT_DETAILS_ROWS.write().unwrap();
            if let Some(row) = rows.get_mut(&event.aggregator_id) {
                row.0.status = AccountStatus::Closed;
                row.1 = event.version;
                row.2 = event.routing_key.to_owned();
            }
        }
    };

    Ok(())
}

#[evento::handler]
async fn handle_owned_name_chaged<E: Executor>(
    event: Event<NameChanged>,
    action: Action<'_, AccountDetailsView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.owner_name = event.data.value;
        }
        Action::Handle(_context) => {
            // Update all accounts that have this owner_id
            let mut rows = ACCOUNT_DETAILS_ROWS.write().unwrap();
            for row in rows.values_mut() {
                if row.0.owner_id == event.aggregator_id {
                    row.0.owner_name = event.data.value.clone();
                }
            }
        }
    };

    Ok(())
}
