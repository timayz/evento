use evento::{Executor, metadata::Metadata, projection::ProjectionAggregator};

use crate::{aggregator::AccountFrozen, error::BankAccountError, value_object::AccountStatus};

/// Command to freeze an account
#[derive(Debug, Clone)]
pub struct FreezeAccount {
    pub reason: String,
}

impl<E: Executor> super::Command<E> {
    /// Handle FreezeAccount command
    pub async fn freeze_account(
        &self,
        id: impl Into<String>,
        cmd: FreezeAccount,
    ) -> Result<(), BankAccountError> {
        let Some(account) = self.load(id).await.unwrap() else {
            return Err(BankAccountError::Server("not found".to_owned()));
        };
        if matches!(account.status, AccountStatus::Closed) {
            return Err(BankAccountError::AccountClosed);
        }
        if matches!(account.status, AccountStatus::Frozen) {
            return Err(BankAccountError::AccountAlreadyFrozen);
        }

        account
            .aggregator()
            .unwrap()
            .event(&AccountFrozen { reason: cmd.reason })
            .metadata(&Metadata::default())
            .commit(&self.0)
            .await?;

        Ok(())
    }
}
