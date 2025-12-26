use evento::{Executor, metadata::Metadata};

use crate::{aggregator::AccountClosed, error::BankAccountError, value_object::AccountStatus};

/// Command to close an account
#[derive(Debug, Clone)]
pub struct CloseAccount {
    pub reason: String,
}

impl<'a, E: Executor> super::Command<'a, E> {
    /// Handle CloseAccount command
    pub async fn close_account(&self, cmd: CloseAccount) -> Result<(), BankAccountError> {
        if matches!(self.status, AccountStatus::Closed) {
            return Err(BankAccountError::AccountClosed);
        }
        if self.balance < 0 {
            return Err(BankAccountError::NegativeBalance);
        }

        self.aggregator()
            .event(&AccountClosed {
                reason: cmd.reason,
                final_balance: self.balance,
            })
            .metadata(&Metadata::default())
            .commit(self.executor)
            .await?;

        Ok(())
    }
}
