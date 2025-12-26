use evento::{Executor, metadata::Metadata};

use crate::{
    aggregator::OverdraftLimitChanged, error::BankAccountError, value_object::AccountStatus,
};

/// Command to change the overdraft limit
#[derive(Debug, Clone)]
pub struct ChangeOverdraftLimit {
    pub new_limit: i64,
}

impl<'a, E: Executor> super::Command<'a, E> {
    /// Handle ChangeOverdraftLimit command
    pub async fn change_overdraft_limit(
        &self,
        cmd: ChangeOverdraftLimit,
    ) -> Result<(), BankAccountError> {
        if matches!(self.status, AccountStatus::Closed) {
            return Err(BankAccountError::AccountClosed);
        }
        if cmd.new_limit < 0 {
            return Err(BankAccountError::InvalidLimit);
        }

        self.aggregator()
            .event(&OverdraftLimitChanged {
                new_limit: cmd.new_limit,
            })
            .metadata(&Metadata::default())
            .commit(self.executor)
            .await?;

        Ok(())
    }
}
