use evento::{Executor, metadata::Metadata};

use crate::{aggregator::AccountClosed, error::BankAccountError, value_object::AccountStatus};

/// Command to close an account
#[derive(Debug, Clone)]
pub struct CloseAccount {
    pub reason: String,
}

impl super::Command {
    /// Handle CloseAccount command
    pub async fn close_account<E: Executor>(
        &self,
        cmd: CloseAccount,
        executor: &E,
    ) -> Result<(), BankAccountError> {
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
            })?
            .metadata(&Metadata::default())?
            .commit(executor)
            .await?;

        Ok(())
    }
}
