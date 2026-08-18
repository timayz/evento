use evento::{Executor, projection::ProjectionAggregate};

use crate::{aggregator::MoneyTransferred, error::BankAccountError, value_object::AccountStatus};

/// Command to transfer money to another account
#[derive(Debug, Clone)]
pub struct TransferMoney {
    pub amount: i64,
    pub to_account_id: String,
    pub transaction_id: String,
    pub description: String,
}

#[evento::command]
impl<E: Executor> super::Command<E> {
    /// Handle TransferMoney command
    pub async fn transfer_money(
        &self,
        id: impl Into<String>,
        cmd: TransferMoney,
        routing_key: Option<String>,
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

        let available = account.balance + account.overdraft_limit;
        if cmd.amount > available {
            return Err(BankAccountError::InsufficientFunds {
                available,
                requested: cmd.amount,
            });
        }

        account
            .write()?
            .routing_key_opt(routing_key)
            .event(&MoneyTransferred {
                amount: cmd.amount,
                to_account_id: cmd.to_account_id,
                transaction_id: cmd.transaction_id,
                description: cmd.description,
            })
            .commit(&self.0)
            .await?;

        Ok(())
    }
}
