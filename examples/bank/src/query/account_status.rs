use evento::{
    Executor,
    metadata::Event,
    projection::{Action, Projection},
};

use crate::{
    aggregator::{AccountClosed, AccountFrozen, AccountOpened, AccountUnfrozen},
    value_object::AccountStatus,
};

pub fn create_projection<E: Executor>() -> Projection<AccountStatusView, E> {
    Projection::new("account-status-view")
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
}

#[evento::snapshot]
async fn restore(
    context: &evento::context::RwContext,
    id: String,
) -> anyhow::Result<Option<AccountStatusView>> {
    Ok(None)
}

#[evento::handler]
async fn handle_account_opened<E: Executor>(
    event: Event<AccountOpened>,
    action: Action<'_, AccountStatusView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.status = AccountStatus::Active;
            row.is_closed = false;
            row.is_active = true;
            row.is_frozen = false;
        }
        Action::Handle(context) => {}
    };

    Ok(())
}

#[evento::handler]
async fn handle_account_frozen<E: Executor>(
    event: Event<AccountFrozen>,
    action: Action<'_, AccountStatusView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.status = AccountStatus::Frozen;
            row.is_closed = false;
            row.is_active = false;
            row.is_frozen = true;
        }
        Action::Handle(context) => {}
    };

    Ok(())
}

#[evento::handler]
async fn handle_account_unfrozen<E: Executor>(
    event: Event<AccountUnfrozen>,
    action: Action<'_, AccountStatusView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.status = AccountStatus::Active;
            row.is_closed = false;
            row.is_active = true;
            row.is_frozen = false;
        }
        Action::Handle(context) => {}
    };

    Ok(())
}

#[evento::handler]
async fn handle_account_closed<E: Executor>(
    event: Event<AccountClosed>,
    action: Action<'_, AccountStatusView, E>,
) -> anyhow::Result<()> {
    match action {
        Action::Apply(row) => {
            row.status = AccountStatus::Closed;
            row.is_closed = true;
            row.is_active = false;
            row.is_frozen = false;
        }
        Action::Handle(context) => {}
    };

    Ok(())
}
