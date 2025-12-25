use evento::{Executor, metadata::Metadata};

use crate::{aggregator::MoneyTransferred, error::BankAccountError, value_object::AccountStatus};

/// Command to transfer money to another account
#[derive(Debug, Clone)]
pub struct TransferMoney {
    pub amount: i64,
    pub to_account_id: String,
    pub transaction_id: String,
    pub description: String,
}

impl super::Command {
    /// Handle TransferMoney command
    pub async fn transfer_money<E: Executor>(
        &self,
        cmd: TransferMoney,
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
            .event(&MoneyTransferred {
                amount: cmd.amount,
                to_account_id: cmd.to_account_id,
                transaction_id: cmd.transaction_id,
                description: cmd.description,
            })
            .metadata(&Metadata::default())
            .commit(executor)
            .await?;

        Ok(())
    }

    /// Handle TransferMoney command
    pub async fn transfer_money_with_routing<E: Executor>(
        &self,
        cmd: TransferMoney,
        executor: &E,
        key: impl Into<String>,
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
            .routing_key(key)
            .event(&MoneyTransferred {
                amount: cmd.amount,
                to_account_id: cmd.to_account_id,
                transaction_id: cmd.transaction_id,
                description: cmd.description,
            })
            .metadata(&Metadata::default())
            .commit(executor)
            .await?;

        Ok(())
    }
}
