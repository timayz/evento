use evento::AggregateEvent;

#[evento::aggregate(name = "myapp/Payment")]
pub enum Payment {
    #[evento(upcast_to = PaymentRefundedV2)]
    PaymentRefunded { amount: i64 },
    #[evento(upcast_to = PaymentRefundedV3)]
    PaymentRefundedV2 { amount: i64, reason: String },
    PaymentRefundedV3 {
        amount: i64,
        reason: String,
        reference: Option<String>,
    },

    #[evento(name = "legacy.captured", upcast_to = PaymentCapturedV2)]
    PaymentCaptured(i64),
    PaymentCapturedV2(i64, String),
}

impl From<PaymentRefunded> for PaymentRefundedV2 {
    fn from(old: PaymentRefunded) -> Self {
        Self {
            amount: old.amount,
            reason: "unknown".into(),
        }
    }
}

impl From<PaymentRefundedV2> for PaymentRefundedV3 {
    fn from(old: PaymentRefundedV2) -> Self {
        Self {
            amount: old.amount,
            reason: old.reason,
            reference: None,
        }
    }
}

impl From<PaymentCaptured> for PaymentCapturedV2 {
    fn from(old: PaymentCaptured) -> Self {
        Self(old.0, "EUR".into())
    }
}

fn main() {
    // The oldest event upcasts to nothing.
    assert!(PaymentRefunded::upcasters().is_empty());

    let v2 = PaymentRefundedV2::upcasters();
    assert_eq!(v2.len(), 1);
    assert_eq!((v2[0].from, v2[0].hops), ("PaymentRefunded", 1));

    // The whole chain is folded: V3 lists V1 (two hops) and V2 (one hop).
    let mut v3: Vec<_> = PaymentRefundedV3::upcasters()
        .iter()
        .map(|u| (u.from, u.hops))
        .collect();
    v3.sort();
    assert_eq!(v3, [("PaymentRefunded", 2), ("PaymentRefundedV2", 1)]);

    let v1_to_v3 = PaymentRefundedV3::upcasters()
        .iter()
        .find(|u| u.from == "PaymentRefunded")
        .unwrap();
    let bytes = (v1_to_v3.upcast)(&bitcode::encode(&PaymentRefunded { amount: 12 })).unwrap();
    assert_eq!(
        bitcode::decode::<PaymentRefundedV3>(&bytes).unwrap(),
        PaymentRefundedV3 {
            amount: 12,
            reason: "unknown".into(),
            reference: None,
        }
    );
    assert!((v1_to_v3.upcast)(&[0xFF]).is_err());

    // `from` is the stored name, which `#[evento(name = ...)]` may pin.
    let captured = PaymentCapturedV2::upcasters();
    assert_eq!(captured[0].from, "legacy.captured");
}
