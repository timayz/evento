use evento::{Executor, metadata::Metadata};

use crate::{aggregator::AccountFrozen, error::BankAccountError, value_object::AccountStatus};

/// Command to freeze an account
#[derive(Debug, Clone)]
pub struct FreezeAccount {
    pub reason: String,
}

impl<'a, E: Executor> super::Command<'a, E> {
    /// Handle FreezeAccount command
    pub async fn freeze_account(&self, cmd: FreezeAccount) -> Result<(), BankAccountError> {
        if matches!(self.status, AccountStatus::Closed) {
            return Err(BankAccountError::AccountClosed);
        }
        if matches!(self.status, AccountStatus::Frozen) {
            return Err(BankAccountError::AccountAlreadyFrozen);
        }

        self.aggregator()
            .event(&AccountFrozen { reason: cmd.reason })
            .metadata(&Metadata::default())
            .commit(self.executor)
            .await?;

        Ok(())
    }
}
