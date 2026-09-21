#[evento::aggregate]
pub enum Payment {
    PaymentRefunded { amount: i64 },
    #[evento(name = "PaymentRefunded")]
    PaymentRefundedV2 { amount: i64, reason: String },
}

fn main() {}
