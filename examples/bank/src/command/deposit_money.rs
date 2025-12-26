use evento::{Executor, metadata::Metadata};

use crate::{aggregator::MoneyDeposited, error::BankAccountError, value_object::AccountStatus};

/// Command to deposit money into an account
#[derive(Debug, Clone)]
pub struct DepositMoney {
    pub amount: i64,
    pub transaction_id: String,
    pub description: String,
}

impl<'a, E: Executor> super::Command<'a, E> {
    /// Handle DepositMoney command
    pub async fn deposit_money(&self, cmd: DepositMoney) -> Result<(), BankAccountError> {
        if matches!(self.status, AccountStatus::Closed) {
            return Err(BankAccountError::AccountClosed);
        }
        if matches!(self.status, AccountStatus::Frozen) {
            return Err(BankAccountError::AccountFrozen);
        }
        if cmd.amount <= 0 {
            return Err(BankAccountError::InvalidAmount);
        }

        self.aggregator()
            .event(&MoneyDeposited {
                amount: cmd.amount,
                transaction_id: cmd.transaction_id,
                description: cmd.description,
            })
            .metadata(&Metadata::default())
            .commit(self.executor)
            .await?;

        Ok(())
    }
}
