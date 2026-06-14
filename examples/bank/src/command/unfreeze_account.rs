use evento::{Executor, projection::ProjectionAggregate};

use crate::{aggregator::AccountUnfrozen, error::BankAccountError, value_object::AccountStatus};

/// Command to unfreeze an account
#[derive(Debug, Clone)]
pub struct UnfreezeAccount {
    pub reason: String,
}

impl<E: Executor> super::Command<E> {
    /// Handle UnfreezeAccount command
    pub async fn unfreeze_account(
        &self,
        id: impl Into<String>,
        cmd: UnfreezeAccount,
    ) -> Result<(), BankAccountError> {
        let Some(account) = self.load(id).await? else {
            return Err(BankAccountError::AccountNotFound);
        };
        if matches!(account.status, AccountStatus::Closed) {
            return Err(BankAccountError::AccountClosed);
        }
        if !matches!(account.status, AccountStatus::Frozen) {
            return Err(BankAccountError::AccountNotFrozen);
        }

        account
            .write()?
            .event(&AccountUnfrozen { reason: cmd.reason })
            .commit(&self.0)
            .await?;

        Ok(())
    }
}
