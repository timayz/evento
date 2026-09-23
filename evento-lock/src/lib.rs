//! Keeps the shapes an evento application persists append-only.
//!
//! Events are stored as positional, packed bitcode: there are no field names
//! on disk and no tolerance for an extra or missing field, and even an enum's
//! discriminant is packed to the number of variants it had when it was
//! written. Adding a field to an event, or to a type nested in one, makes every
//! stored occurrence undecodable — and no ordinary test notices.
//!
//! `evento-lock` reads your sources with `syn`, writes the persisted shapes
//! down in `events.lock`, and fails a test when a line that is already there
//! changes. Three kinds of lines:
//!
//! - `event <aggregate>::<name> { .. }` — a variant of an `#[evento::aggregate]`
//!   enum, keyed by its stored name. **Frozen**: never edited, renamed or
//!   removed.
//! - `type <module>::<Name> ..` — a `bitcode::Encode` type reachable from an
//!   event. **Frozen** too, enums included.
//! - `view <projection name> rev=<n> { .. }` — a projection snapshotted through
//!   the executor (`#[evento::projection(bitcode::Encode, bitcode::Decode)]`),
//!   with the types only it uses. **May change** when its `.revision(n)` grows.
//!
//! Add a test to one crate of the workspace (typically as a dev-dependency):
//!
//! ```no_run
//! // tests/events_lock.rs
//! #[test]
//! fn persisted_shapes_only_grow() {
//!     evento_lock::check(env!("CARGO_MANIFEST_DIR")).unwrap();
//! }
//! ```
//!
//! ```text
//! cargo test                      # verify — what CI runs
//! EVENTO_LOCK=update cargo test   # record new events, types and views
//! EVENTO_LOCK=force  cargo test   # also record changes: only for shapes no
//!                                 # deployed database has ever stored
//! ```
//!
//! Use [`Config`] to scan only some packages or to move the lock file.

#![forbid(unsafe_code)]

mod check;
mod discover;
mod lock;
mod revision;
mod scan;

use std::{
    fmt,
    path::{Path, PathBuf},
};

pub use check::{diff, Problem};
pub use lock::{Entry, Kind, Lock};

/// Name of the environment variable [`check`] and [`Config::mode_from_env`] read.
pub const ENV: &str = "EVENTO_LOCK";

/// Verifies the workspace containing `manifest_dir` against `events.lock` at
/// its root, in the [`Mode`] given by `EVENTO_LOCK`.
pub fn check(manifest_dir: impl AsRef<Path>) -> Result<Report, Error> {
    Config::new(manifest_dir).mode_from_env().run()
}

/// What to do with the lock file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Mode {
    /// Fail on any difference. `EVENTO_LOCK` unset.
    #[default]
    Verify,
    /// Write new shapes and bumped views; refuse changes to frozen shapes.
    /// `EVENTO_LOCK=update`.
    Update,
    /// Write whatever the sources say. `EVENTO_LOCK=force`.
    Force,
}

/// Which crates to scan, where the lock lives, and what to do with it.
#[derive(Debug, Clone)]
pub struct Config {
    manifest_dir: PathBuf,
    lock_path: Option<PathBuf>,
    packages: Vec<String>,
    exclude: Vec<String>,
    require_pinned_names: bool,
    mode: Result<Mode, String>,
}

impl Config {
    /// Every package of the workspace containing `manifest_dir`, with the
    /// lock at `<workspace root>/events.lock`, in [`Mode::Verify`].
    pub fn new(manifest_dir: impl AsRef<Path>) -> Self {
        Self {
            manifest_dir: manifest_dir.as_ref().to_path_buf(),
            lock_path: None,
            packages: Vec::new(),
            exclude: Vec::new(),
            require_pinned_names: false,
            mode: Ok(Mode::Verify),
        }
    }

    /// Where the lock is; a relative path is relative to the workspace root.
    pub fn lock_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.lock_path = Some(path.into());
        self
    }

    /// Scans only these packages.
    pub fn packages<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.packages = names.into_iter().map(Into::into).collect();
        self
    }

    /// Does not scan these packages.
    pub fn exclude<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.exclude = names.into_iter().map(Into::into).collect();
        self
    }

    /// Refuses `#[evento::aggregate]` without `name = ".."`. The default name
    /// is `<package>/<Enum>`: renaming the package or the enum would orphan
    /// every stored event.
    pub fn require_pinned_names(mut self, yes: bool) -> Self {
        self.require_pinned_names = yes;
        self
    }

    /// Sets the mode.
    pub fn mode(mut self, mode: Mode) -> Self {
        self.mode = Ok(mode);
        self
    }

    /// Reads the mode from `EVENTO_LOCK`: unset or `verify`, `update`, `force`.
    pub fn mode_from_env(mut self) -> Self {
        self.mode = match std::env::var(ENV).as_deref() {
            Err(_) | Ok("") | Ok("verify") => Ok(Mode::Verify),
            Ok("update") => Ok(Mode::Update),
            Ok("force") => Ok(Mode::Force),
            Ok(other) => Err(other.to_owned()),
        };
        self
    }

    /// Scans, compares and, depending on the mode, writes the lock.
    pub fn run(self) -> Result<Report, Error> {
        let mode = self.mode.clone().map_err(|value| {
            Error::Other(anyhow::anyhow!(
                "{ENV}={value:?}: expected `verify`, `update` or `force`"
            ))
        })?;
        let workspace = discover::workspace(&self.manifest_dir)?;
        let path = match &self.lock_path {
            Some(path) => workspace.root.join(path),
            None => workspace.root.join("events.lock"),
        };
        for name in self.packages.iter().chain(&self.exclude) {
            if !workspace.packages.iter().any(|p| p.name == *name) {
                return Err(Error::Other(anyhow::anyhow!(
                    "no package `{name}` in the workspace at {}",
                    workspace.root.display()
                )));
            }
        }

        let mut scanner = scan::Scanner::new(self.require_pinned_names);
        for package in &workspace.packages {
            let included = self.packages.is_empty() || self.packages.contains(&package.name);
            if !included || self.exclude.contains(&package.name) {
                continue;
            }
            for target in &package.targets {
                scanner.crate_root(&package.name, &target.crate_ident, &target.src_path)?;
            }
        }
        let current = scanner.finish()?;

        let locked = match std::fs::read_to_string(&path) {
            Ok(text) => Some(Lock::parse(&text)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(Error::Other(error.into())),
        };
        let problems = match &locked {
            Some(locked) => diff(locked, &current),
            None => Vec::new(),
        };
        let write = match (mode, &locked) {
            (Mode::Verify, None) => return Err(Error::Missing(path)),
            (Mode::Verify, Some(_)) if !problems.is_empty() => {
                return Err(Error::Problems(problems))
            }
            (Mode::Verify, Some(_)) => false,
            (Mode::Update, _) if problems.iter().any(Problem::is_breaking) => {
                return Err(Error::Refused(
                    problems.into_iter().filter(Problem::is_breaking).collect(),
                ))
            }
            (Mode::Update | Mode::Force, None) => true,
            (Mode::Update | Mode::Force, Some(locked)) => *locked != current,
        };
        if write {
            std::fs::write(&path, current.render()).map_err(|e| Error::Other(e.into()))?;
        }
        Ok(Report {
            path,
            shapes: current.len(),
            written: write,
        })
    }
}

/// What [`Config::run`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    /// The lock file.
    pub path: PathBuf,
    /// Number of shapes in the sources.
    pub shapes: usize,
    /// Whether the lock file was (re)written.
    pub written: bool,
}

/// Why [`Config::run`] failed.
///
/// `Debug` prints the same as `Display`, so `check(..).unwrap()` reads well.
pub enum Error {
    /// `events.lock` does not exist yet.
    Missing(PathBuf),
    /// The sources differ from the lock ([`Mode::Verify`]).
    Problems(Vec<Problem>),
    /// `EVENTO_LOCK=update` refused to record breaking changes.
    Refused(Vec<Problem>),
    /// The sources or the workspace could not be read.
    Other(anyhow::Error),
}

impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        Error::Other(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let list = |f: &mut fmt::Formatter<'_>, problems: &[Problem]| -> fmt::Result {
            for problem in problems {
                writeln!(f, "\n- {problem}")?;
            }
            Ok(())
        };
        match self {
            Error::Missing(path) => write!(
                f,
                "{} does not exist — run `{ENV}=update cargo test` and commit it",
                path.display()
            ),
            Error::Problems(problems) => {
                writeln!(f, "the persisted shapes do not match events.lock:")?;
                list(f, problems)
            }
            Error::Refused(problems) => {
                writeln!(f, "refusing to update events.lock:")?;
                list(f, problems)?;
                write!(
                    f,
                    "\n`{ENV}=force` records them anyway: only for shapes no deployed \
                     database has ever stored."
                )
            }
            Error::Other(error) => write!(f, "{error:#}"),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl std::error::Error for Error {}
