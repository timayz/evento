use evento::{Executor, metadata::Metadata, projection::ProjectionAggregator};

use crate::{aggregator::MoneyDeposited, error::BankAccountError, value_object::AccountStatus};

/// Command to deposit money into an account
#[derive(Debug, Clone)]
pub struct DepositMoney {
    pub amount: i64,
    pub transaction_id: String,
    pub description: String,
}

impl<E: Executor> super::Command<E> {
    /// Handle DepositMoney command
    pub async fn deposit_money(
        &self,
        id: impl Into<String>,
        cmd: DepositMoney,
    ) -> Result<(), BankAccountError> {
        let Some(account) = self.load(id).await.unwrap() else {
            return Err(BankAccountError::Server("not found".to_owned()));
        };

        if matches!(account.status, AccountStatus::Closed) {
            return Err(BankAccountError::AccountClosed);
        }
        if matches!(account.status, AccountStatus::Frozen) {
            return Err(BankAccountError::AccountFrozen);
        }
        if cmd.amount <= 0 {
            return Err(BankAccountError::InvalidAmount);
        }

        account
            .aggregator()
            .unwrap()
            .event(&MoneyDeposited {
                amount: cmd.amount,
                transaction_id: cmd.transaction_id,
                description: cmd.description,
            })
            .metadata(&Metadata::default())
            .commit(&self.0)
            .await?;

        Ok(())
    }
}
