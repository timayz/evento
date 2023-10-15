use evento_mq::Context;

#[test]
fn test_remove() {
    let mut map = Context::new();

    map.insert::<i8>(123);
    assert!(map.get::<i8>().is_some());

    map.remove::<i8>();
    assert!(map.get::<i8>().is_none());
}

#[test]
fn test_clear() {
    let mut map = Context::new();

    map.insert::<i8>(8);
    map.insert::<i16>(16);
    map.insert::<i32>(32);

    assert!(map.contains::<i8>());
    assert!(map.contains::<i16>());
    assert!(map.contains::<i32>());

    map.clear();

    assert!(!map.contains::<i8>());
    assert!(!map.contains::<i16>());
    assert!(!map.contains::<i32>());

    map.insert::<i8>(10);
    assert_eq!(*map.get::<i8>().unwrap(), 10);
}

#[test]
fn test_integers() {
    static A: u32 = 8;

    let mut map = Context::new();

    map.insert::<i8>(8);
    map.insert::<i16>(16);
    map.insert::<i32>(32);
    map.insert::<i64>(64);
    map.insert::<i128>(128);
    map.insert::<u8>(8);
    map.insert::<u16>(16);
    map.insert::<u32>(32);
    map.insert::<u64>(64);
    map.insert::<u128>(128);
    map.insert::<&'static u32>(&A);
    assert!(map.get::<i8>().is_some());
    assert!(map.get::<i16>().is_some());
    assert!(map.get::<i32>().is_some());
    assert!(map.get::<i64>().is_some());
    assert!(map.get::<i128>().is_some());
    assert!(map.get::<u8>().is_some());
    assert!(map.get::<u16>().is_some());
    assert!(map.get::<u32>().is_some());
    assert!(map.get::<u64>().is_some());
    assert!(map.get::<u128>().is_some());
    assert!(map.get::<&'static u32>().is_some());
}

#[test]
fn test_composition() {
    struct Magi<T>(pub T);

    struct Madoka {
        pub god: bool,
    }

    struct Homura {
        pub attempts: usize,
    }

    struct Mami {
        pub guns: usize,
    }

    let mut map = Context::new();

    map.insert(Magi(Madoka { god: false }));
    map.insert(Magi(Homura { attempts: 0 }));
    map.insert(Magi(Mami { guns: 999 }));

    assert!(!map.get::<Magi<Madoka>>().unwrap().0.god);
    assert_eq!(0, map.get::<Magi<Homura>>().unwrap().0.attempts);
    assert_eq!(999, map.get::<Magi<Mami>>().unwrap().0.guns);
}

#[test]
fn test_extensions() {
    #[derive(Debug, PartialEq)]
    struct MyType(i32);

    let mut extensions = Context::new();

    extensions.insert(5i32);
    extensions.insert(MyType(10));

    assert_eq!(extensions.get(), Some(&5i32));
    assert_eq!(extensions.get_mut(), Some(&mut 5i32));

    assert_eq!(extensions.remove::<i32>(), Some(5i32));
    assert!(extensions.get::<i32>().is_none());

    assert_eq!(extensions.get::<bool>(), None);
    assert_eq!(extensions.get(), Some(&MyType(10)));
}

#[test]
fn test_extend() {
    #[derive(Debug, PartialEq)]
    struct MyType(i32);

    let mut extensions = Context::new();

    extensions.insert(5i32);
    extensions.insert(MyType(10));

    let mut other = Context::new();

    other.insert(15i32);
    other.insert(20u8);

    extensions.extend(other);

    assert_eq!(extensions.get(), Some(&15i32));
    assert_eq!(extensions.get_mut(), Some(&mut 15i32));

    assert_eq!(extensions.remove::<i32>(), Some(15i32));
    assert!(extensions.get::<i32>().is_none());

    assert_eq!(extensions.get::<bool>(), None);
    assert_eq!(extensions.get(), Some(&MyType(10)));

    assert_eq!(extensions.get(), Some(&20u8));
    assert_eq!(extensions.get_mut(), Some(&mut 20u8));
}
