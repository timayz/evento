use evento::{
    Executor,
    metadata::Event,
    projection::{Action, Projection},
};

use crate::aggregator::{
    AccountOpened, MoneyDeposited, MoneyReceived, MoneyTransferred, MoneyWithdrawn,
    OverdraftLimitChanged,
};

pub fn create_projection<E: Executor>() -> Projection<AccountBalanceView, E> {
    Projection::new("account-balance-view")
        .handler(handle_money_deposit())
        .handler(handle_account_opened())
        .handler(handle_money_received())
        .handler(handle_money_withdrawn())
        .handler(handle_money_transferred())
        .handler(handle_overdraf_limit_changed())
}

#[derive(Default)]
pub struct AccountBalanceView {
    pub balance: i64,
    pub currency: String,
    pub available_balance: i64,
}

#[evento::snapshot]
async fn restore(
    _context: &evento::context::RwContext,
    _id: String,
) -> anyhow::Result<Option<AccountBalanceView>> {
    Ok(None)
}

#[evento::handler]
async fn handle_account_opened<E: Executor>(
    event: Event<AccountOpened>,
    action: Action<'_, AccountBalanceView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.balance = event.data.initial_balance;
            row.currency = event.data.currency;
            row.available_balance = event.data.initial_balance;
        }
        Action::Handle(_context) => {}
    };

    Ok(())
}

#[evento::handler]
async fn handle_money_deposit<E: Executor>(
    event: Event<MoneyDeposited>,
    action: Action<'_, AccountBalanceView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.balance += event.data.amount;
            row.available_balance += event.data.amount;
        }
        Action::Handle(_context) => {}
    };

    Ok(())
}

#[evento::handler]
async fn handle_money_withdrawn<E: Executor>(
    event: Event<MoneyWithdrawn>,
    action: Action<'_, AccountBalanceView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.balance -= event.data.amount;
            row.available_balance -= event.data.amount;
        }
        Action::Handle(_context) => {}
    };

    Ok(())
}

#[evento::handler]
async fn handle_money_transferred<E: Executor>(
    event: Event<MoneyTransferred>,
    action: Action<'_, AccountBalanceView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.balance -= event.data.amount;
            row.available_balance -= event.data.amount;
        }
        Action::Handle(_context) => {}
    };

    Ok(())
}

#[evento::handler]
async fn handle_money_received<E: Executor>(
    event: Event<MoneyReceived>,
    action: Action<'_, AccountBalanceView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.balance += event.data.amount;
            row.available_balance += event.data.amount;
        }
        Action::Handle(_context) => {}
    };

    Ok(())
}

#[evento::handler]
async fn handle_overdraf_limit_changed<E: Executor>(
    event: Event<OverdraftLimitChanged>,
    action: Action<'_, AccountBalanceView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.available_balance = row.balance + event.data.new_limit;
        }
        Action::Handle(_context) => {}
    };

    Ok(())
}
