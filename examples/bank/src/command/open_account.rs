use evento::{Executor, metadata::Metadata};

use crate::{aggregator::AccountOpened, error::BankAccountError, value_object::AccountType};

/// Command to open a new bank account
#[derive(Debug, Clone)]
pub struct OpenAccount {
    pub owner_id: String,
    pub owner_name: String,
    pub account_type: AccountType,
    pub currency: String,
    pub initial_balance: i64,
}

impl super::Command {
    /// Handle OpenAccount command - creates a new account
    pub async fn open_account<E: Executor>(
        cmd: OpenAccount,
        executor: &E,
    ) -> Result<String, BankAccountError> {
        if cmd.owner_id.is_empty() {
            return Err(BankAccountError::OwnerIdRequired);
        }
        if cmd.owner_name.is_empty() {
            return Err(BankAccountError::OwnerNameRequired);
        }
        if cmd.currency.is_empty() {
            return Err(BankAccountError::CurrencyRequired);
        }
        if cmd.initial_balance < 0 {
            return Err(BankAccountError::InvalidAmount);
        }

        Ok(evento::create()
            .event(&AccountOpened {
                owner_id: cmd.owner_id,
                owner_name: cmd.owner_name,
                account_type: cmd.account_type,
                currency: cmd.currency,
                initial_balance: cmd.initial_balance,
            })?
            .metadata(&Metadata::default())?
            .commit(executor)
            .await?)
    }

    pub async fn open_account_with_routing<E: Executor>(
        cmd: OpenAccount,
        executor: &E,
        key: impl Into<String>,
    ) -> Result<String, BankAccountError> {
        if cmd.owner_id.is_empty() {
            return Err(BankAccountError::OwnerIdRequired);
        }
        if cmd.owner_name.is_empty() {
            return Err(BankAccountError::OwnerNameRequired);
        }
        if cmd.currency.is_empty() {
            return Err(BankAccountError::CurrencyRequired);
        }
        if cmd.initial_balance < 0 {
            return Err(BankAccountError::InvalidAmount);
        }

        Ok(evento::create()
            .routing_key(key)
            .event(&AccountOpened {
                owner_id: cmd.owner_id,
                owner_name: cmd.owner_name,
                account_type: cmd.account_type,
                currency: cmd.currency,
                initial_balance: cmd.initial_balance,
            })?
            .metadata(&Metadata::default())?
            .commit(executor)
            .await?)
    }
}
