use bincode::{Decode, Encode};

#[derive(Debug, Clone, PartialEq, Encode, Decode, Default)]
pub enum AccountType {
    #[default]
    Checking,
    Savings,
    Business,
}

#[derive(Debug, Clone, PartialEq, Encode, Decode, Default)]
pub enum AccountStatus {
    #[default]
    Active,
    Frozen,
    Closed,
}
