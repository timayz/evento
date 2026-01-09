use evento::{Executor, metadata::Metadata, projection::ProjectionAggregator};

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
        let Some(account) = self.load(id).await.unwrap() else {
            return Err(BankAccountError::Server("not found".to_owned()));
        };
        if matches!(account.status, AccountStatus::Closed) {
            return Err(BankAccountError::AccountClosed);
        }
        if account.balance < 0 {
            return Err(BankAccountError::NegativeBalance);
        }

        account
            .aggregator()
            .unwrap()
            .event(&AccountClosed {
                reason: cmd.reason,
                final_balance: account.balance,
            })
            .metadata(&Metadata::default())
            .commit(&self.0)
            .await?;

        Ok(())
    }
}
