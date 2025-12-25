use evento::{Executor, metadata::Metadata};

use crate::{aggregator::MoneyWithdrawn, error::BankAccountError, value_object::AccountStatus};

/// Command to withdraw money from an account
#[derive(Debug, Clone)]
pub struct WithdrawMoney {
    pub amount: i64,
    pub transaction_id: String,
    pub description: String,
}

impl super::Command {
    /// Handle WithdrawMoney command
    pub async fn withdraw_money<E: Executor>(
        &self,
        cmd: WithdrawMoney,
        executor: &E,
    ) -> Result<(), BankAccountError> {
        if matches!(self.status, AccountStatus::Closed) {
            return Err(BankAccountError::AccountClosed);
        }
        if matches!(self.status, AccountStatus::Frozen) {
            return Err(BankAccountError::AccountFrozen);
        }
        if cmd.amount <= 0 {
            return Err(BankAccountError::InvalidAmount);
        }

        let available = self.balance + self.overdraft_limit;
        if cmd.amount > available {
            return Err(BankAccountError::InsufficientFunds {
                available,
                requested: cmd.amount,
            });
        }

        self.aggregator()
            .event(&MoneyWithdrawn {
                amount: cmd.amount,
                transaction_id: cmd.transaction_id,
                description: cmd.description,
            })
            .metadata(&Metadata::default())
            .commit(executor)
            .await?;

        Ok(())
    }
}
