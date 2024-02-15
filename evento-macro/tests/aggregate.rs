use evento_macro::Aggregate;
use evento_store::Aggregate;

#[test]
fn aggregate() {
    assert_eq!(UserLogin::aggregate_type(), "user-login");
    assert_eq!(UserLogin::to_aggregate_id("1"), "user-login#1");
    assert_eq!(UserLogin::from_aggregate_id("user-login#1"), "1");
    assert_eq!(
        UserLogin::aggregate_version(),
        "63e539a0d47ec336e652c9403e118e3ddc26ef1e3f890195e1176d92f25b12e7"
    );
}

#[derive(Aggregate)]
struct UserLogin {}
