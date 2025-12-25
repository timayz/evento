use rkyv::{Archive, Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Default, Archive, Serialize, Deserialize)]
pub enum AccountType {
    #[default]
    Checking,
    Savings,
    Business,
}

#[derive(Debug, Clone, PartialEq, Default, Archive, Serialize, Deserialize)]
pub enum AccountStatus {
    #[default]
    Active,
    Frozen,
    Closed,
}
