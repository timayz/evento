use evento::{Executor, projection::ProjectionAggregate};

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
        let Some(account) = self.load(id).await? else {
            return Err(BankAccountError::AccountNotFound);
        };
        if matches!(account.status, AccountStatus::Closed) {
            return Err(BankAccountError::AccountClosed);
        }
        if cmd.new_limit < 0 {
            return Err(BankAccountError::InvalidLimit);
        }

        account
            .write()?
            .event(&OverdraftLimitChanged {
                new_limit: cmd.new_limit,
            })
            .commit(&self.0)
            .await?;

        Ok(())
    }
}
