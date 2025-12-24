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

use evento::AggregatorBuilder;

use crate::value_object::{AccountStatus, AccountType};

pub struct Command {
    pub account_id: String,
    pub owner_id: String,
    pub owner_name: String,
    pub account_type: AccountType,
    pub currency: String,
    pub balance: i64,
    pub status: AccountStatus,
    pub daily_withdrawal_limit: i64,
    pub overdraft_limit: i64,
    pub version: u16,
    pub routin_key: Option<String>,
}

impl Command {
    fn aggregator(&self) -> AggregatorBuilder {
        evento::aggregator(&self.account_id)
            .original_version(self.version)
            .routing_key_opt(self.routin_key.to_owned())
    }

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
