#[evento::aggregate]
pub enum Payment {
    #[evento(upcast_to = PaymentRefundedV9)]
    PaymentRefunded { amount: i64 },
    PaymentRefundedV2 { amount: i64, reason: String },
}

fn main() {}
