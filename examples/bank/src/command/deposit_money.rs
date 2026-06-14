use evento::{Executor, projection::ProjectionAggregate};

use crate::{aggregator::MoneyDeposited, error::BankAccountError, value_object::AccountStatus};

/// Command to deposit money into an account
#[derive(Debug, Clone)]
pub struct DepositMoney {
    pub amount: i64,
    pub transaction_id: String,
    pub description: String,
}

impl<E: Executor> super::Command<E> {
    /// Handle DepositMoney command
    pub async fn deposit_money(
        &self,
        id: impl Into<String>,
        cmd: DepositMoney,
    ) -> Result<(), BankAccountError> {
        let Some(account) = self.load(id).await? else {
            return Err(BankAccountError::AccountNotFound);
        };

        if matches!(account.status, AccountStatus::Closed) {
            return Err(BankAccountError::AccountClosed);
        }
        if matches!(account.status, AccountStatus::Frozen) {
            return Err(BankAccountError::AccountFrozen);
        }
        if cmd.amount <= 0 {
            return Err(BankAccountError::InvalidAmount);
        }

        account
            .write()?
            .event(&MoneyDeposited {
                amount: cmd.amount,
                transaction_id: cmd.transaction_id,
                description: cmd.description,
            })
            .commit(&self.0)
            .await?;

        Ok(())
    }
}
