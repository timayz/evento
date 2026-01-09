use evento::{Executor, metadata::Metadata, projection::ProjectionAggregator};

use crate::{aggregator::MoneyWithdrawn, error::BankAccountError, value_object::AccountStatus};

/// Command to withdraw money from an account
#[derive(Debug, Clone)]
pub struct WithdrawMoney {
    pub amount: i64,
    pub transaction_id: String,
    pub description: String,
}

impl<E: Executor> super::Command<E> {
    /// Handle WithdrawMoney command
    pub async fn withdraw_money(
        &self,
        id: impl Into<String>,
        cmd: WithdrawMoney,
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
            .event(&MoneyWithdrawn {
                amount: cmd.amount,
                transaction_id: cmd.transaction_id,
                description: cmd.description,
            })
            .metadata(&Metadata::default())
            .commit(&self.0)
            .await?;

        Ok(())
    }
}
