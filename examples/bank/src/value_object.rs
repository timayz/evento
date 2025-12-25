use bitcode::{Decode, Encode};

#[derive(Debug, Clone, PartialEq, Default, Encode, Decode)]
pub enum AccountType {
    #[default]
    Checking,
    Savings,
    Business,
}

#[derive(Debug, Clone, PartialEq, Default, Encode, Decode)]
pub enum AccountStatus {
    #[default]
    Active,
    Frozen,
    Closed,
}
