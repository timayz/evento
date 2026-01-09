use evento::{Executor, metadata::Metadata, projection::ProjectionCursor};

use crate::{
    aggregator::OverdraftLimitChanged, error::BankAccountError, value_object::AccountStatus,
};

/// Command to change the overdraft limit
#[derive(Debug, Clone)]
pub struct ChangeOverdraftLimit {
    pub new_limit: i64,
}

impl<E: Executor> super::Command<E> {
    /// Handle ChangeOverdraftLimit command
    pub async fn change_overdraft_limit(
        &self,
        id: impl Into<String>,
        cmd: ChangeOverdraftLimit,
    ) -> Result<(), BankAccountError> {
        let Some(account) = self.load(id).await.unwrap() else {
            return Err(BankAccountError::Server("not found".to_owned()));
        };
        if matches!(account.status, AccountStatus::Closed) {
            return Err(BankAccountError::AccountClosed);
        }
        if cmd.new_limit < 0 {
            return Err(BankAccountError::InvalidLimit);
        }

        account
            .aggregator()
            .unwrap()
            .event(&OverdraftLimitChanged {
                new_limit: cmd.new_limit,
            })
            .metadata(&Metadata::default())
            .commit(&self.0)
            .await?;

        Ok(())
    }
}
