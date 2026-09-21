use evento::ProjectionCursor;

#[evento::projection]
pub struct Default {
    pub balance: i64,
}

#[evento::projection(name = "myapp/Pinned")]
pub struct Pinned {
    pub balance: i64,
}

#[evento::projection]
pub struct Generic<T: ::core::default::Default + Clone> {
    pub value: T,
}

mod nested {
    #[evento::projection]
    pub struct Default {
        pub balance: i64,
    }
}

fn main() {
    // `<module path>::<Struct>`: same-named views in different modules differ.
    assert_eq!(
        Default::projection_name(),
        concat!(module_path!(), "::Default")
    );
    assert_eq!(
        nested::Default::projection_name(),
        concat!(module_path!(), "::nested::Default")
    );

    assert_eq!(Pinned::projection_name(), "myapp/Pinned");

    // Generic structs keep the type-name default, distinct per instantiation.
    assert_ne!(
        Generic::<u8>::projection_name(),
        Generic::<u16>::projection_name()
    );
}
