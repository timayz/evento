use evento::{cursor, metadata::Event, projection::Projection};

use crate::{
    aggregator::{AccountClosed, AccountFrozen, AccountOpened, AccountUnfrozen, BankAccount},
    value_object::AccountStatus,
};

pub fn create_projection(id: impl Into<String>) -> Projection<AccountStatusView> {
    Projection::new::<BankAccount>(id)
        .handler(handle_account_opened())
        .handler(handle_account_closed())
        .handler(handle_account_frozen())
        .handler(handle_account_unfrozen())
}

#[derive(Default)]
pub struct AccountStatusView {
    pub status: AccountStatus,
    pub is_active: bool,
    pub is_frozen: bool,
    pub is_closed: bool,
    pub cursor: cursor::Value,
}

impl evento::ProjectionCursor for AccountStatusView {
    fn get_cursor(&self) -> evento::cursor::Value {
        self.cursor.to_owned()
    }

    fn set_cursor(&mut self, v: &cursor::Value) {
        self.cursor = v.to_owned();
    }
}
impl evento::Snapshot for AccountStatusView {}

#[evento::handler]
async fn handle_account_opened(
    _event: Event<AccountOpened>,
    row: &mut AccountStatusView,
) -> anyhow::Result<()> {
    row.status = AccountStatus::Active;
    row.is_closed = false;
    row.is_active = true;
    row.is_frozen = false;

    Ok(())
}

#[evento::handler]
async fn handle_account_frozen(
    _event: Event<AccountFrozen>,
    row: &mut AccountStatusView,
) -> anyhow::Result<()> {
    row.status = AccountStatus::Frozen;
    row.is_closed = false;
    row.is_active = false;
    row.is_frozen = true;

    Ok(())
}

#[evento::handler]
async fn handle_account_unfrozen(
    _event: Event<AccountUnfrozen>,
    row: &mut AccountStatusView,
) -> anyhow::Result<()> {
    row.status = AccountStatus::Active;
    row.is_closed = false;
    row.is_active = true;
    row.is_frozen = false;

    Ok(())
}

#[evento::handler]
async fn handle_account_closed(
    _event: Event<AccountClosed>,
    row: &mut AccountStatusView,
) -> anyhow::Result<()> {
    row.status = AccountStatus::Closed;
    row.is_closed = true;
    row.is_active = false;
    row.is_frozen = false;

    Ok(())
}
