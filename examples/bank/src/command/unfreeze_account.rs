use evento::{Executor, metadata::Metadata};

use crate::{aggregator::AccountUnfrozen, error::BankAccountError, value_object::AccountStatus};

/// Command to unfreeze an account
#[derive(Debug, Clone)]
pub struct UnfreezeAccount {
    pub reason: String,
}

impl<'a, E: Executor> super::Command<'a, E> {
    /// Handle UnfreezeAccount command
    pub async fn unfreeze_account(&self, cmd: UnfreezeAccount) -> Result<(), BankAccountError> {
        if matches!(self.status, AccountStatus::Closed) {
            return Err(BankAccountError::AccountClosed);
        }
        if !matches!(self.status, AccountStatus::Frozen) {
            return Err(BankAccountError::AccountNotFrozen);
        }

        self.aggregator()
            .event(&AccountUnfrozen { reason: cmd.reason })
            .metadata(&Metadata::default())
            .commit(self.executor)
            .await?;

        Ok(())
    }
}
