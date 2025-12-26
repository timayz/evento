use evento::{Executor, metadata::Metadata};

use crate::{
    aggregator::DailyWithdrawalLimitChanged, error::BankAccountError, value_object::AccountStatus,
};

/// Command to change the daily withdrawal limit
#[derive(Debug, Clone)]
pub struct ChangeDailyWithdrawalLimit {
    pub new_limit: i64,
}

impl<'a, E: Executor> super::Command<'a, E> {
    /// Handle ChangeDailyWithdrawalLimit command
    pub async fn change_daily_withdrawal_limit(
        &self,
        cmd: ChangeDailyWithdrawalLimit,
    ) -> Result<(), BankAccountError> {
        if matches!(self.status, AccountStatus::Closed) {
            return Err(BankAccountError::AccountClosed);
        }
        if cmd.new_limit < 0 {
            return Err(BankAccountError::InvalidLimit);
        }

        self.aggregator()
            .event(&DailyWithdrawalLimitChanged {
                new_limit: cmd.new_limit,
            })
            .metadata(&Metadata::default())
            .commit(self.executor)
            .await?;

        Ok(())
    }
}
