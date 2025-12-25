use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum BankAccountError {
    AccountNotActive,
    AccountFrozen,
    AccountClosed,
    AccountAlreadyFrozen,
    AccountNotFrozen,
    InsufficientFunds { available: i64, requested: i64 },
    InvalidAmount,
    InvalidLimit,
    NegativeBalance,
    OwnerIdRequired,
    OwnerNameRequired,
    CurrencyRequired,
    Server(String),
}

impl fmt::Display for BankAccountError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AccountNotActive => write!(f, "Account is not active"),
            Self::AccountFrozen => write!(f, "Account is frozen"),
            Self::AccountClosed => write!(f, "Account is closed"),
            Self::AccountAlreadyFrozen => write!(f, "Account is already frozen"),
            Self::AccountNotFrozen => write!(f, "Account is not frozen"),
            Self::InsufficientFunds {
                available,
                requested,
            } => {
                write!(
                    f,
                    "Insufficient funds: available {available}, requested {requested}"
                )
            }
            Self::InvalidAmount => write!(f, "Amount must be greater than zero"),
            Self::InvalidLimit => write!(f, "Limit must be non-negative"),
            Self::NegativeBalance => write!(f, "Cannot close account with negative balance"),
            Self::OwnerIdRequired => write!(f, "Owner ID is required"),
            Self::OwnerNameRequired => write!(f, "Owner name is required"),
            Self::CurrencyRequired => write!(f, "Currency is required"),
            Self::Server(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for BankAccountError {}

impl From<evento::WriteError> for BankAccountError {
    fn from(value: evento::WriteError) -> Self {
        Self::Server(value.to_string())
    }
}
