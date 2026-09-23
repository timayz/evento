#[evento::aggregate]
pub enum Ledger {
    EntryAdded { amount: i64 },
    LedgerEvent { amount: i64 },
}

fn main() {}
