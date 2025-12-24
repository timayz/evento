use evento::{
    Executor,
    metadata::Event,
    projection::{Action, Projection},
};

use crate::{
    aggregator::{
        AccountClosed, AccountFrozen, AccountOpened, AccountUnfrozen, DailyWithdrawalLimitChanged,
        MoneyDeposited, MoneyReceived, MoneyTransferred, MoneyWithdrawn, OverdraftLimitChanged,
    },
    value_object::{AccountStatus, AccountType},
};

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
    context: &evento::context::RwContext,
    id: String,
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
            row.daily_withdrawal_limit = 1000; // Default: $1000.00 in cents
            row.overdraft_limit = 0;
        }
        Action::Handle(context) => {}
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
        Action::Handle(context) => {}
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
        Action::Handle(context) => {}
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
        Action::Handle(context) => {}
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
        Action::Handle(context) => {}
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
        Action::Handle(context) => {}
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
        Action::Handle(context) => {}
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
        Action::Handle(context) => {}
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
        Action::Handle(context) => {}
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
        Action::Handle(context) => {}
    };

    Ok(())
}
