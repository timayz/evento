use evento::{Executor, metadata::Metadata, projection::ProjectionAggregator};

use crate::{aggregator::MoneyTransferred, error::BankAccountError, value_object::AccountStatus};

/// Command to transfer money to another account
#[derive(Debug, Clone)]
pub struct TransferMoney {
    pub amount: i64,
    pub to_account_id: String,
    pub transaction_id: String,
    pub description: String,
}

impl<E: Executor> super::Command<E> {
    /// Handle TransferMoney command
    pub async fn transfer_money(
        &self,
        id: impl Into<String>,
        cmd: TransferMoney,
    ) -> Result<(), BankAccountError> {
        let Some(account) = self.load(id).await.unwrap() else {
            return Err(BankAccountError::Server("not found".to_owned()));
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

        let available = account.balance + account.overdraft_limit;
        if cmd.amount > available {
            return Err(BankAccountError::InsufficientFunds {
                available,
                requested: cmd.amount,
            });
        }

        account
            .aggregator()
            .unwrap()
            .event(&MoneyTransferred {
                amount: cmd.amount,
                to_account_id: cmd.to_account_id,
                transaction_id: cmd.transaction_id,
                description: cmd.description,
            })
            .metadata(&Metadata::default())
            .commit(&self.0)
            .await?;

        Ok(())
    }

    /// Handle TransferMoney command
    pub async fn transfer_money_with_routing(
        &self,
        id: impl Into<String>,
        cmd: TransferMoney,
        key: impl Into<String>,
    ) -> Result<(), BankAccountError> {
        let Some(account) = self.load(id).await.unwrap() else {
            return Err(BankAccountError::Server("not found".to_owned()));
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

        let available = account.balance + account.overdraft_limit;
        if cmd.amount > available {
            return Err(BankAccountError::InsufficientFunds {
                available,
                requested: cmd.amount,
            });
        }

        account
            .aggregator()
            .unwrap()
            .routing_key(key)
            .event(&MoneyTransferred {
                amount: cmd.amount,
                to_account_id: cmd.to_account_id,
                transaction_id: cmd.transaction_id,
                description: cmd.description,
            })
            .metadata(&Metadata::default())
            .commit(&self.0)
            .await?;

        Ok(())
    }
}
