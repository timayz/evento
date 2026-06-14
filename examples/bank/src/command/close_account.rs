use evento::{Executor, projection::ProjectionAggregate};

use crate::{aggregator::AccountClosed, error::BankAccountError, value_object::AccountStatus};

/// Command to close an account
#[derive(Debug, Clone)]
pub struct CloseAccount {
    pub reason: String,
}

impl<E: Executor> super::Command<E> {
    /// Handle CloseAccount command
    pub async fn close_account(
        &self,
        id: impl Into<String>,
        cmd: CloseAccount,
    ) -> Result<(), BankAccountError> {
        let Some(account) = self.load(id).await? else {
            return Err(BankAccountError::AccountNotFound);
        };
        if matches!(account.status, AccountStatus::Closed) {
            return Err(BankAccountError::AccountClosed);
        }
        if account.balance < 0 {
            return Err(BankAccountError::NegativeBalance);
        }

        account
            .write()?
            .event(&AccountClosed {
                reason: cmd.reason,
                final_balance: account.balance,
            })
            .commit(&self.0)
            .await?;

        Ok(())
    }
}
