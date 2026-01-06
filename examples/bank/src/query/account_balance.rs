use evento::{metadata::Event, projection::Projection};

use crate::{
    BankAccount,
    aggregator::{
        AccountOpened, MoneyDeposited, MoneyReceived, MoneyTransferred, MoneyWithdrawn,
        OverdraftLimitChanged,
    },
};

pub fn create_projection(id: impl Into<String>) -> Projection<AccountBalanceView> {
    Projection::new::<BankAccount>(id)
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

impl evento::Snapshot for AccountBalanceView {}

#[evento::handler]
async fn handle_account_opened(
    event: Event<AccountOpened>,
    row: &mut AccountBalanceView,
) -> anyhow::Result<()> {
    row.balance = event.data.initial_balance;
    row.currency = event.data.currency;
    row.available_balance = event.data.initial_balance;

    Ok(())
}

#[evento::handler]
async fn handle_money_deposit(
    event: Event<MoneyDeposited>,
    row: &mut AccountBalanceView,
) -> anyhow::Result<()> {
    row.balance += event.data.amount;
    row.available_balance += event.data.amount;

    Ok(())
}

#[evento::handler]
async fn handle_money_withdrawn(
    event: Event<MoneyWithdrawn>,
    row: &mut AccountBalanceView,
) -> anyhow::Result<()> {
    row.balance -= event.data.amount;
    row.available_balance -= event.data.amount;

    Ok(())
}

#[evento::handler]
async fn handle_money_transferred(
    event: Event<MoneyTransferred>,
    row: &mut AccountBalanceView,
) -> anyhow::Result<()> {
    row.balance -= event.data.amount;
    row.available_balance -= event.data.amount;

    Ok(())
}

#[evento::handler]
async fn handle_money_received(
    event: Event<MoneyReceived>,
    row: &mut AccountBalanceView,
) -> anyhow::Result<()> {
    row.balance += event.data.amount;
    row.available_balance += event.data.amount;

    Ok(())
}

#[evento::handler]
async fn handle_overdraf_limit_changed(
    event: Event<OverdraftLimitChanged>,
    row: &mut AccountBalanceView,
) -> anyhow::Result<()> {
    row.available_balance = row.balance + event.data.new_limit;

    Ok(())
}
