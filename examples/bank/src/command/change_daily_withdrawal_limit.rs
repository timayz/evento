use evento::{Executor, projection::ProjectionAggregator};

use crate::{
    aggregator::DailyWithdrawalLimitChanged, error::BankAccountError, value_object::AccountStatus,
};

/// Command to change the daily withdrawal limit
#[derive(Debug, Clone)]
pub struct ChangeDailyWithdrawalLimit {
    pub new_limit: i64,
}

impl<E: Executor> super::Command<E> {
    /// Handle ChangeDailyWithdrawalLimit command
    pub async fn change_daily_withdrawal_limit(
        &self,
        id: impl Into<String>,
        cmd: ChangeDailyWithdrawalLimit,
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
            .event(&DailyWithdrawalLimitChanged {
                new_limit: cmd.new_limit,
            })
            .commit(&self.0)
            .await?;

        Ok(())
    }
}
