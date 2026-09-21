#[evento::aggregate]
pub enum Payment {
    #[evento(upcast_to = B)]
    A { amount: i64 },
    #[evento(upcast_to = C)]
    B { amount: i64 },
    #[evento(upcast_to = A)]
    C { amount: i64 },
}

fn main() {}
