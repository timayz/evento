//! Comparing the committed lock with the sources.

use std::fmt;

use crate::lock::{Kind, Lock};

/// What is wrong between the committed lock and the sources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Problem {
    /// A frozen shape was edited.
    Changed {
        /// `<kind> <name>`.
        key: String,
        /// The shape in the lock.
        locked: String,
        /// The shape in the sources.
        current: String,
    },
    /// A frozen shape disappeared (removed or renamed).
    Removed {
        /// `<kind> <name>`.
        key: String,
    },
    /// A view changed shape and kept its revision.
    ViewNeedsRevision {
        /// `view <name>`.
        key: String,
        /// The unchanged revision.
        revision: u16,
    },
    /// A view's revision went backwards.
    ViewRevisionDecreased {
        /// `view <name>`.
        key: String,
        /// The revision in the lock.
        locked: u16,
        /// The revision in the sources.
        current: u16,
    },
    /// The lock is merely behind: new shapes, a view with a bumped revision,
    /// or a view that is gone.
    OutOfDate {
        /// `<kind> <name>`.
        key: String,
    },
}

impl Problem {
    /// Whether `EVENTO_LOCK=update` must refuse to record it.
    pub fn is_breaking(&self) -> bool {
        !matches!(self, Problem::OutOfDate { .. })
    }
}

impl fmt::Display for Problem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Problem::Changed {
                key,
                locked,
                current,
            } => write!(
                f,
                "`{key}` is persisted and changed shape — stored data would no longer decode.\n    \
                 locked:  {locked}\n    current: {current}\n    \
                 Restore it and add a new variant (or a companion event) for the new shape."
            ),
            Problem::Removed { key } => write!(
                f,
                "`{key}` is persisted and was removed or renamed — stored data would be orphaned. \
                 Restore it; an event that is no longer written still has to be read."
            ),
            Problem::ViewNeedsRevision { key, revision } => write!(
                f,
                "`{key}` changed shape but its projection is still at `.revision({revision})` — \
                 old snapshots would be mis-decoded. Bump the revision."
            ),
            Problem::ViewRevisionDecreased {
                key,
                locked,
                current,
            } => write!(
                f,
                "`{key}` went from revision {locked} back to {current}; revisions only grow."
            ),
            Problem::OutOfDate { key } => write!(
                f,
                "`{key}` is not up to date in events.lock — run \
                 `EVENTO_LOCK=update cargo test` and commit the result."
            ),
        }
    }
}

/// Compares the committed lock with what the sources say now.
pub fn diff(locked: &Lock, current: &Lock) -> Vec<Problem> {
    let mut problems = Vec::new();
    for (key, was) in &locked.entries {
        let Some(now) = current.entries.get(key) else {
            problems.push(match was.kind {
                Kind::View => Problem::OutOfDate { key: key.clone() },
                _ => Problem::Removed { key: key.clone() },
            });
            continue;
        };
        match was.kind {
            Kind::Event | Kind::Type if was.shape != now.shape => problems.push(Problem::Changed {
                key: key.clone(),
                locked: was.shape.clone(),
                current: now.shape.clone(),
            }),
            Kind::View => {
                let (locked_rev, current_rev) =
                    (was.revision.unwrap_or(0), now.revision.unwrap_or(0));
                if current_rev < locked_rev {
                    problems.push(Problem::ViewRevisionDecreased {
                        key: key.clone(),
                        locked: locked_rev,
                        current: current_rev,
                    });
                } else if was.shape != now.shape && current_rev == locked_rev {
                    problems.push(Problem::ViewNeedsRevision {
                        key: key.clone(),
                        revision: current_rev,
                    });
                } else if was != now {
                    problems.push(Problem::OutOfDate { key: key.clone() });
                }
            }
            _ => {}
        }
    }
    for key in current.entries.keys() {
        if !locked.entries.contains_key(key) {
            problems.push(Problem::OutOfDate { key: key.clone() });
        }
    }
    problems
}
