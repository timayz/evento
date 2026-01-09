use evento::{Executor, metadata::Metadata, projection::ProjectionCursor};

use crate::{aggregator::MoneyReceived, error::BankAccountError, value_object::AccountStatus};

/// Command to receive money from another account
#[derive(Debug, Clone)]
pub struct ReceiveMoney {
    pub amount: i64,
    pub from_account_id: String,
    pub transaction_id: String,
    pub description: String,
}

impl<E: Executor> super::Command<E> {
    /// Handle ReceiveMoney command
    pub async fn receive_money(
        &self,
        id: impl Into<String>,
        cmd: ReceiveMoney,
    ) -> Result<(), BankAccountError> {
        let Some(account) = self.load(id).await.unwrap() else {
            return Err(BankAccountError::Server("not found".to_owned()));
        };
        if matches!(account.status, AccountStatus::Closed) {
            return Err(BankAccountError::AccountClosed);
        }
        // Note: Frozen accounts can still receive money
        if cmd.amount <= 0 {
            return Err(BankAccountError::InvalidAmount);
        }

        account
            .aggregator()
            .unwrap()
            .event(&MoneyReceived {
                amount: cmd.amount,
                from_account_id: cmd.from_account_id,
                transaction_id: cmd.transaction_id,
                description: cmd.description,
            })
            .metadata(&Metadata::default())
            .commit(&self.0)
            .await?;

        Ok(())
    }
}
