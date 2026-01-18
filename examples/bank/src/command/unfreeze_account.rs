use evento::{Executor, projection::ProjectionAggregator};

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
        let Some(account) = self.load(id).await.unwrap() else {
            return Err(BankAccountError::Server("not found".to_owned()));
        };
        if matches!(account.status, AccountStatus::Closed) {
            return Err(BankAccountError::AccountClosed);
        }
        if !matches!(account.status, AccountStatus::Frozen) {
            return Err(BankAccountError::AccountNotFrozen);
        }

        account
            .aggregator()
            .unwrap()
            .event(&AccountUnfrozen { reason: cmd.reason })
            .commit(&self.0)
            .await?;

        Ok(())
    }
}
