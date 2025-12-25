use evento::{Executor, metadata::Metadata};

use crate::{aggregator::MoneyReceived, error::BankAccountError, value_object::AccountStatus};

/// Command to receive money from another account
#[derive(Debug, Clone)]
pub struct ReceiveMoney {
    pub amount: i64,
    pub from_account_id: String,
    pub transaction_id: String,
    pub description: String,
}

impl super::Command {
    /// Handle ReceiveMoney command
    pub async fn receive_money<E: Executor>(
        &self,
        cmd: ReceiveMoney,
        executor: &E,
    ) -> Result<(), BankAccountError> {
        if matches!(self.status, AccountStatus::Closed) {
            return Err(BankAccountError::AccountClosed);
        }
        // Note: Frozen accounts can still receive money
        if cmd.amount <= 0 {
            return Err(BankAccountError::InvalidAmount);
        }

        self.aggregator()
            .event(&MoneyReceived {
                amount: cmd.amount,
                from_account_id: cmd.from_account_id,
                transaction_id: cmd.transaction_id,
                description: cmd.description,
            })
            .metadata(&Metadata::default())
            .commit(executor)
            .await?;

        Ok(())
    }
}
