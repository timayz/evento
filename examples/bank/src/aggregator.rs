use crate::value_object::AccountType;

// =============================================================================
// EVENTS
// =============================================================================

#[evento::aggregator]
pub enum BankAccount {
    /// Event raised when a new bank account is opened
    AccountOpened {
        owner_id: String,
        owner_name: String,
        account_type: AccountType,
        currency: String,
        initial_balance: i64,
    },

    /// Event raised when money is deposited into the account
    MoneyDeposited {
        amount: i64,
        transaction_id: String,
        description: String,
    },

    /// Event raised when money is withdrawn from the account
    MoneyWithdrawn {
        amount: i64,
        transaction_id: String,
        description: String,
    },

    /// Event raised when money is transferred to another account
    MoneyTransferred {
        amount: i64,
        to_account_id: String,
        transaction_id: String,
        description: String,
    },

    MoneyReceived {
        amount: i64,
        from_account_id: String,
        transaction_id: String,
        description: String,
    },

    /// Event raised when the account is frozen (e.g., due to suspicious activity)
    AccountFrozen { reason: String },

    /// Event raised when the account is unfrozen
    AccountUnfrozen { reason: String },

    /// Event raised when the daily withdrawal limit is changed
    DailyWithdrawalLimitChanged { new_limit: i64 },

    /// Event raised when the overdraft limit is changed
    OverdraftLimitChanged { new_limit: i64 },

    /// Event raised when the account is closed
    AccountClosed { reason: String, final_balance: i64 },
}
