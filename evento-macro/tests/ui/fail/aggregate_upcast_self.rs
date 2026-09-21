#[evento::aggregate]
pub enum Payment {
    #[evento(upcast_to = PaymentRefunded)]
    PaymentRefunded { amount: i64 },
}

fn main() {}
