//! The `events.lock` file: one line per persisted shape.

use std::collections::BTreeMap;

const HEADER: &str = "\
# Shapes persisted by evento. Generated: `EVENTO_LOCK=update cargo test`.
# `event` and `type` lines are frozen — add a new variant instead of editing
# one. A `view` line may change when its projection's `.revision(n)` grows.
";

/// What a line of the lock describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Kind {
    /// A variant of an `#[evento::aggregate]` enum. Frozen.
    Event,
    /// A `bitcode::Encode` type reachable from an event. Frozen.
    Type,
    /// A snapshotted projection, with the types only it uses. May change when
    /// its revision grows.
    View,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Kind::Event => "event",
            Kind::Type => "type",
            Kind::View => "view",
        }
    }
}

/// One line of the lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// What the line describes.
    pub kind: Kind,
    /// `.revision(n)` of a view's projection; `None` for events and types.
    pub revision: Option<u16>,
    /// The layout, as written in the sources: `{ amount: i64, reason: String }`.
    pub shape: String,
}

/// The persisted shapes, keyed by `<kind> <name>`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Lock {
    pub(crate) entries: BTreeMap<String, Entry>,
}

impl Lock {
    /// Number of shapes.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the lock has no shape at all.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The entry for `key`, e.g. `"event bank/BankAccount::AccountOpened"`.
    pub fn get(&self, key: &str) -> Option<&Entry> {
        self.entries.get(key)
    }

    /// Every `(key, entry)`, sorted by key.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Entry)> {
        self.entries
            .iter()
            .map(|(key, entry)| (key.as_str(), entry))
    }

    pub(crate) fn insert(&mut self, kind: Kind, name: &str, revision: Option<u16>, shape: String) {
        self.entries.insert(
            format!("{} {name}", kind.as_str()),
            Entry {
                kind,
                revision,
                shape,
            },
        );
    }

    /// The file contents.
    pub fn render(&self) -> String {
        let mut out = String::from(HEADER);
        let mut previous = None;
        // Events first, then the types they freeze, then the views.
        let mut lines: Vec<(&String, &Entry)> = self.entries.iter().collect();
        lines.sort_by_key(|(key, entry)| (entry.kind, (*key).clone()));
        for (key, entry) in lines {
            if previous != Some(entry.kind) {
                out.push('\n');
                previous = Some(entry.kind);
            }
            match entry.revision {
                Some(revision) => out.push_str(&format!("{key} rev={revision} {}\n", entry.shape)),
                None => out.push_str(&format!("{key} {}\n", entry.shape)),
            }
        }
        out
    }

    /// Reads a lock written by [`Lock::render`].
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let mut lock = Lock::default();
        for line in text.lines().map(str::trim) {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let malformed = || anyhow::anyhow!("malformed events.lock line: `{line}`");
            let (kind, rest) = line.split_once(' ').ok_or_else(malformed)?;
            let kind = match kind {
                "event" => Kind::Event,
                "type" => Kind::Type,
                "view" => Kind::View,
                _ => return Err(malformed()),
            };
            let (name, rest) = rest.split_once(' ').ok_or_else(malformed)?;
            let (revision, shape) = match kind {
                Kind::View => {
                    let (revision, shape) = rest.split_once(' ').ok_or_else(malformed)?;
                    let revision = revision
                        .strip_prefix("rev=")
                        .and_then(|r| r.parse().ok())
                        .ok_or_else(malformed)?;
                    (Some(revision), shape)
                }
                _ => (None, rest),
            };
            lock.insert(kind, name, revision, shape.to_owned());
        }
        Ok(lock)
    }
}
