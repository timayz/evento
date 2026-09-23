//! `Mode::{Verify, Update, Force}` against a throwaway workspace.

use std::path::{Path, PathBuf};

use evento_lock::{Config, Error, Mode, Problem};

const EVENTS: &str = r#"
use bitcode::{Decode, Encode};

#[derive(Encode, Decode)]
pub struct Money { pub minor: i64, pub currency: String }

#[evento::aggregate(name = "shop/Order")]
pub enum Order {
    Placed { total: Money },
}
"#;

fn workspace(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("src/domain")).unwrap();
    // Its own `[workspace]`: it lives under evento's target directory.
    std::fs::write(
        dir.join("Cargo.toml"),
        "[package]\nname = \"shop\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[workspace]\n",
    )
    .unwrap();
    std::fs::write(dir.join("src/lib.rs"), "pub mod domain;\n").unwrap();
    std::fs::write(dir.join("src/domain/mod.rs"), "mod events;\n").unwrap();
    std::fs::write(dir.join("src/domain/events.rs"), EVENTS).unwrap();
    dir
}

fn run(dir: &Path, mode: Mode) -> Result<evento_lock::Report, Error> {
    Config::new(dir).mode(mode).run()
}

#[test]
fn verify_update_force() {
    let dir = workspace("modes");

    assert!(matches!(run(&dir, Mode::Verify), Err(Error::Missing(_))));

    let report = run(&dir, Mode::Update).unwrap();
    assert!(report.written);
    let lock = std::fs::read_to_string(dir.join("events.lock")).unwrap();
    assert!(
        lock.contains("event shop/Order::Placed { total: Money }"),
        "{lock}"
    );
    assert!(
        lock.contains("type shop::domain::events::Money { minor: i64, currency: String }"),
        "{lock}"
    );
    assert!(!run(&dir, Mode::Verify).unwrap().written);

    // A new variant: verify fails, update records it.
    let events = dir.join("src/domain/events.rs");
    std::fs::write(
        &events,
        EVENTS.replace(
            "Placed { total: Money },",
            "Placed { total: Money },\n    Paid,",
        ),
    )
    .unwrap();
    assert!(matches!(
        run(&dir, Mode::Verify),
        Err(Error::Problems(problems)) if matches!(problems.as_slice(), [Problem::OutOfDate { .. }])
    ));
    assert!(run(&dir, Mode::Update).unwrap().written);
    assert!(run(&dir, Mode::Verify).is_ok());

    // A field added to a frozen type: update refuses, force records it.
    std::fs::write(
        &events,
        std::fs::read_to_string(&events).unwrap().replace(
            "pub currency: String",
            "pub currency: String, pub scale: u8",
        ),
    )
    .unwrap();
    let refused = run(&dir, Mode::Update).unwrap_err();
    assert!(matches!(refused, Error::Refused(_)));
    assert!(refused.to_string().contains("changed shape"), "{refused}");
    assert!(run(&dir, Mode::Force).unwrap().written);
    assert!(run(&dir, Mode::Verify).is_ok());
}
