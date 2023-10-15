use evento_mq::Data;
use std::{ops::Deref, sync::Arc};

#[test]
fn test_data_from_arc() {
    let data_new = Data::new(String::from("test-123"));
    let data_from_arc = Data::from(Arc::new(String::from("test-123")));
    assert_eq!(data_new.deref(), data_from_arc.deref());
}

#[test]
fn test_data_from_dyn_arc() {
    trait TestTrait {
        fn get_num(&self) -> i32;
    }
    struct A {}
    impl TestTrait for A {
        fn get_num(&self) -> i32 {
            42
        }
    }
    // This works when Sized is required
    let dyn_arc_box: Arc<Box<dyn TestTrait>> = Arc::new(Box::new(A {}));
    let data_arc_box = Data::from(dyn_arc_box);
    // This works when Data Sized Bound is removed
    let dyn_arc: Arc<dyn TestTrait> = Arc::new(A {});
    let data_arc = Data::from(dyn_arc);
    assert_eq!(data_arc_box.get_num(), data_arc.get_num())
}

#[test]
fn test_dyn_data_into_arc() {
    trait TestTrait {
        fn get_num(&self) -> i32;
    }
    struct A {}
    impl TestTrait for A {
        fn get_num(&self) -> i32 {
            42
        }
    }
    let dyn_arc: Arc<dyn TestTrait> = Arc::new(A {});
    let data_arc = Data::from(dyn_arc);
    let arc_from_data = data_arc.clone().into_inner();
    assert_eq!(data_arc.get_num(), arc_from_data.get_num())
}

#[test]
fn test_get_ref_from_dyn_data() {
    trait TestTrait {
        fn get_num(&self) -> i32;
    }
    struct A {}
    impl TestTrait for A {
        fn get_num(&self) -> i32 {
            42
        }
    }
    let dyn_arc: Arc<dyn TestTrait> = Arc::new(A {});
    let data_arc = Data::from(dyn_arc);
    let ref_data = data_arc.get_ref();
    assert_eq!(data_arc.get_num(), ref_data.get_num())
}
