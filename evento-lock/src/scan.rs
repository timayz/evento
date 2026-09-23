//! Reading the sources: aggregates, the `Encode` types they reach, and the
//! snapshotted projections.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use anyhow::Context;
use quote::ToTokens;
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    visit::Visit,
    Token,
};

use crate::{
    lock::{Kind, Lock},
    revision,
};

/// Where an item was declared.
#[derive(Clone)]
struct Origin {
    package: String,
    /// `module_path!()` of the item: `bank::query::account_balance`.
    module: String,
    file: PathBuf,
}

/// An `Encode` struct or enum.
struct TypeDef {
    origin: Origin,
    name: String,
    shape: String,
    /// Identifiers its fields mention: candidates for nested types.
    mentions: BTreeSet<String>,
}

struct EventDef {
    origin: Origin,
    /// `<aggregate>::<stored event name>`.
    name: String,
    shape: String,
    mentions: BTreeSet<String>,
}

struct ViewDef {
    origin: Origin,
    ident: String,
    /// The projection name snapshots are keyed by.
    name: String,
    shape: String,
    mentions: BTreeSet<String>,
}

struct RevisionCall {
    origin: Origin,
    call: revision::Call,
}

/// Collects shapes file by file, then [`Scanner::finish`] assembles the lock.
pub(crate) struct Scanner {
    require_pinned_names: bool,
    events: Vec<EventDef>,
    types: Vec<TypeDef>,
    views: Vec<ViewDef>,
    revisions: Vec<RevisionCall>,
}

impl Scanner {
    pub(crate) fn new(require_pinned_names: bool) -> Self {
        Self {
            require_pinned_names,
            events: Vec::new(),
            types: Vec::new(),
            views: Vec::new(),
            revisions: Vec::new(),
        }
    }

    /// Scans a crate from its root file (`src/lib.rs`, `src/main.rs`, ..),
    /// following `mod` declarations.
    pub(crate) fn crate_root(
        &mut self,
        package: &str,
        crate_ident: &str,
        src_path: &Path,
    ) -> anyhow::Result<()> {
        let dir = src_path.parent().unwrap_or(Path::new("")).to_path_buf();
        self.file(package, &[crate_ident.to_owned()], src_path, &dir)
    }

    fn file(
        &mut self,
        package: &str,
        module: &[String],
        path: &Path,
        children: &Path,
    ) -> anyhow::Result<()> {
        let source = std::fs::read_to_string(path)
            .with_context(|| format!("cannot read {}", path.display()))?;
        let parsed =
            syn::parse_file(&source).with_context(|| format!("cannot parse {}", path.display()))?;
        self.source(package, module, path, children, &parsed)
            .with_context(|| format!("in {}", path.display()))
    }

    /// Scans already parsed items; `children` is where `mod foo;` looks for
    /// `foo.rs` and `foo/mod.rs`.
    fn source(
        &mut self,
        package: &str,
        module: &[String],
        path: &Path,
        children: &Path,
        parsed: &syn::File,
    ) -> anyhow::Result<()> {
        let base = path.parent().unwrap_or(Path::new("")).to_path_buf();
        self.items(package, module, path, children, &base, &parsed.items)
    }

    #[allow(clippy::too_many_arguments)]
    fn items(
        &mut self,
        package: &str,
        module: &[String],
        file: &Path,
        children: &Path,
        path_base: &Path,
        items: &[syn::Item],
    ) -> anyhow::Result<()> {
        let origin = Origin {
            package: package.to_owned(),
            module: module.join("::"),
            file: file.to_path_buf(),
        };
        for call in revision::calls(items)? {
            self.revisions.push(RevisionCall {
                origin: origin.clone(),
                call,
            });
        }
        for item in items {
            match item {
                syn::Item::Mod(item) if !is_test_only(&item.attrs) => {
                    let name = item.ident.to_string();
                    let mut nested = module.to_vec();
                    nested.push(name.clone());
                    match &item.content {
                        Some((_, items)) => {
                            let dir = children.join(&name);
                            self.items(package, &nested, file, &dir, &dir, items)?;
                        }
                        None => {
                            let (path, dir) = module_file(item, &name, children, path_base)?;
                            self.file(package, &nested, &path, &dir)?;
                        }
                    }
                }
                syn::Item::Enum(item) if !is_test_only(&item.attrs) => {
                    if let Some(attr) = evento_attr(&item.attrs, "aggregate") {
                        self.aggregate(&origin, attr, item)?;
                    } else if derives_encode(&item.attrs) {
                        let variants: Vec<String> = item
                            .variants
                            .iter()
                            .map(|v| match &v.fields {
                                syn::Fields::Unit => v.ident.to_string(),
                                fields => format!("{} {}", v.ident, fields_shape(fields)),
                            })
                            .collect();
                        let mut mentions = BTreeSet::new();
                        for variant in &item.variants {
                            mentions.extend(mentions_of(&variant.fields));
                        }
                        self.types.push(TypeDef {
                            origin: origin.clone(),
                            name: item.ident.to_string(),
                            shape: format!("enum {{ {} }}", variants.join(", ")),
                            mentions,
                        });
                    }
                }
                syn::Item::Struct(item) if !is_test_only(&item.attrs) => {
                    if let Some(attr) = evento_attr(&item.attrs, "projection") {
                        self.projection(&origin, attr, item)?;
                    } else if derives_encode(&item.attrs) {
                        self.types.push(TypeDef {
                            origin: origin.clone(),
                            name: item.ident.to_string(),
                            shape: fields_shape(&item.fields),
                            mentions: mentions_of(&item.fields),
                        });
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn aggregate(
        &mut self,
        origin: &Origin,
        attr: &syn::Attribute,
        item: &syn::ItemEnum,
    ) -> anyhow::Result<()> {
        let aggregate = match pinned_name(attr)? {
            Some(name) => name,
            None if self.require_pinned_names => anyhow::bail!(
                "`{}` has no `name = \"..\"`: pin it, the aggregate name is persisted",
                item.ident
            ),
            // What `#[evento::aggregate]` computes: `CARGO_PKG_NAME/Enum`.
            None => format!("{}/{}", origin.package, item.ident),
        };
        for variant in &item.variants {
            let stored = stored_event_name(variant)?.unwrap_or_else(|| variant.ident.to_string());
            self.events.push(EventDef {
                origin: origin.clone(),
                name: format!("{aggregate}::{stored}"),
                shape: fields_shape(&variant.fields),
                mentions: mentions_of(&variant.fields),
            });
        }
        Ok(())
    }

    fn projection(
        &mut self,
        origin: &Origin,
        attr: &syn::Attribute,
        item: &syn::ItemStruct,
    ) -> anyhow::Result<()> {
        let args = match &attr.meta {
            syn::Meta::Path(_) => ProjectionArgs::default(),
            _ => attr.parse_args::<ProjectionArgs>()?,
        };
        // Snapshotted through the executor iff bitcode-encodable, unless
        // `#[evento::snapshot(..)]` takes over.
        let encodes = args.derives.iter().any(is_encode) || derives_encode(&item.attrs);
        if !encodes || evento_attr(&item.attrs, "snapshot").is_some() {
            return Ok(());
        }
        let mut fields: Vec<String> = match &item.fields {
            syn::Fields::Named(named) => named.named.iter().map(field_text).collect(),
            _ => anyhow::bail!("`{}`: a projection has named fields", item.ident),
        };
        // Fields `#[evento::projection]` appends; they are encoded too.
        let cursor = args.cursor.as_ref().map_or("String".to_owned(), type_text);
        fields.push(format!("cursor: {cursor}"));
        fields.push("aggregate_version: u16".to_owned());
        let mut mentions = mentions_of(&item.fields);
        if let Some(cursor) = &args.cursor {
            mentions.extend(mentions_of_type(cursor));
        }
        let name = match args.name {
            Some(name) => name,
            None => format!("{}::{}", origin.module, item.ident),
        };
        self.views.push(ViewDef {
            origin: origin.clone(),
            ident: item.ident.to_string(),
            name,
            shape: format!("{{ {} }}", fields.join(", ")),
            mentions,
        });
        Ok(())
    }

    /// Resolves revisions, freezes what events reach, and builds the lock.
    pub(crate) fn finish(self) -> anyhow::Result<Lock> {
        let revisions = self.revisions()?;

        // Everything an event mentions, transitively, is frozen with it.
        let mut frozen: BTreeSet<usize> = BTreeSet::new();
        for event in &self.events {
            self.reach(&event.origin, &event.mentions, &mut frozen);
        }

        let mut lock = Lock::default();
        for event in &self.events {
            check_key(&event.name)?;
            let key = format!("event {}", event.name);
            anyhow::ensure!(
                lock.get(&key).is_none(),
                "`{key}` is declared twice (in {})",
                event.origin.file.display()
            );
            lock.insert(Kind::Event, &event.name, None, event.shape.clone());
        }
        for index in &frozen {
            let def = &self.types[*index];
            let name = format!("{}::{}", def.origin.module, def.name);
            anyhow::ensure!(
                lock.get(&format!("type {name}")).is_none(),
                "`{name}` is declared twice: the lock cannot tell them apart"
            );
            lock.insert(Kind::Type, &name, None, def.shape.clone());
        }
        for (index, view) in self.views.iter().enumerate() {
            check_key(&view.name)?;
            // A view's shape includes the types only it uses; the frozen ones
            // already have their own line.
            let mut own: BTreeSet<usize> = BTreeSet::new();
            self.reach(&view.origin, &view.mentions, &mut own);
            let nested: Vec<String> = own
                .difference(&frozen)
                .map(|i| format!("{} {}", self.types[*i].name, self.types[*i].shape))
                .collect();
            let shape = if nested.is_empty() {
                view.shape.clone()
            } else {
                format!("{} with {}", view.shape, nested.join("; "))
            };
            let key = format!("view {}", view.name);
            anyhow::ensure!(
                lock.get(&key).is_none(),
                "`{key}` is declared twice (in {}): pin distinct names with \
                 `#[evento::projection(name = \"..\")]`",
                view.origin.file.display()
            );
            let revision = revisions.get(&index).copied().unwrap_or(0);
            lock.insert(Kind::View, &view.name, Some(revision), shape);
        }
        Ok(lock)
    }

    /// The revision of each view (by index) that has a `.revision(n)`.
    fn revisions(&self) -> anyhow::Result<BTreeMap<usize, u16>> {
        let mut revisions = BTreeMap::new();
        for RevisionCall { origin, call } in &self.revisions {
            let candidates: Vec<usize> = match &call.view {
                Some(ident) => {
                    let views = self
                        .views
                        .iter()
                        .enumerate()
                        .filter(|(_, view)| view.ident == *ident)
                        .map(|(index, view)| (index, &view.origin));
                    let found = closest(origin, views);
                    anyhow::ensure!(
                        found.len() <= 1,
                        "{}: `.revision({})` is for `{ident}`, which several snapshotted views \
                         are called",
                        origin.file.display(),
                        call.value
                    );
                    found
                }
                None => {
                    let found: Vec<usize> = (0..self.views.len())
                        .filter(|i| self.views[*i].origin.file == origin.file)
                        .collect();
                    anyhow::ensure!(
                        found.len() <= 1,
                        "{}: cannot tell which view `.revision({})` belongs to — build the \
                         projection in a function returning `Projection<E, MyView>`",
                        origin.file.display(),
                        call.value
                    );
                    found
                }
            };
            // No candidate: the projection is not snapshotted.
            let Some(index) = candidates.first() else {
                continue;
            };
            if let Some(previous) = revisions.insert(*index, call.value) {
                anyhow::ensure!(
                    previous == call.value,
                    "`{}` has two revisions: {previous} and {}",
                    self.views[*index].name,
                    call.value
                );
            }
        }
        Ok(revisions)
    }

    /// Adds to `into` the types `mentions` leads to, transitively.
    fn reach(&self, from: &Origin, mentions: &BTreeSet<String>, into: &mut BTreeSet<usize>) {
        for mention in mentions {
            let types = self
                .types
                .iter()
                .enumerate()
                .filter(|(_, def)| def.name == *mention)
                .map(|(index, def)| (index, &def.origin));
            for index in closest(from, types) {
                if into.insert(index) {
                    let def = &self.types[index];
                    self.reach(&def.origin, &def.mentions, into);
                }
            }
        }
    }
}

/// The candidates declared closest to `from`: in its module, else in its
/// package, else anywhere. A wrong guess only freezes one type too many.
fn closest<'a>(from: &Origin, candidates: impl Iterator<Item = (usize, &'a Origin)>) -> Vec<usize> {
    let candidates: Vec<(usize, &Origin)> = candidates.collect();
    let same_module: Vec<usize> = candidates
        .iter()
        .filter(|(_, o)| o.package == from.package && o.module == from.module)
        .map(|(i, _)| *i)
        .collect();
    if !same_module.is_empty() {
        return same_module;
    }
    let same_package: Vec<usize> = candidates
        .iter()
        .filter(|(_, o)| o.package == from.package)
        .map(|(i, _)| *i)
        .collect();
    if !same_package.is_empty() {
        return same_package;
    }
    candidates.into_iter().map(|(i, _)| i).collect()
}

fn check_key(name: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        !name.is_empty() && !name.contains(char::is_whitespace),
        "`{name}`: names in events.lock cannot contain whitespace"
    );
    Ok(())
}

/// The file of `mod name;` and the directory its own `mod`s live in.
fn module_file(
    item: &syn::ItemMod,
    name: &str,
    children: &Path,
    path_base: &Path,
) -> anyhow::Result<(PathBuf, PathBuf)> {
    let explicit = item.attrs.iter().find_map(|attr| match &attr.meta {
        syn::Meta::NameValue(nv) if nv.path.is_ident("path") => match &nv.value {
            syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(path),
                ..
            }) => Some(path.value()),
            _ => None,
        },
        _ => None,
    });
    if let Some(explicit) = explicit {
        let path = path_base.join(explicit);
        let dir = path.parent().unwrap_or(Path::new("")).to_path_buf();
        return Ok((path, dir));
    }
    let dir = children.join(name);
    let flat = children.join(format!("{name}.rs"));
    if flat.is_file() {
        return Ok((flat, dir));
    }
    let nested = dir.join("mod.rs");
    anyhow::ensure!(
        nested.is_file(),
        "cannot find the file of `mod {name};` (looked for {} and {})",
        flat.display(),
        nested.display()
    );
    Ok((nested, dir))
}

/// `#[cfg(test)]`: not compiled into the crate, nothing it declares is stored.
fn is_test_only(attrs: &[syn::Attribute]) -> bool {
    attrs.iter().any(|attr| {
        attr.path().is_ident("cfg")
            && attr
                .parse_args::<syn::Ident>()
                .is_ok_and(|ident| ident == "test")
    })
}

/// `#[evento::<name>]`, or `#[<name>]` when imported.
fn evento_attr<'a>(attrs: &'a [syn::Attribute], name: &str) -> Option<&'a syn::Attribute> {
    attrs.iter().find(|attr| {
        let segments: Vec<String> = attr
            .path()
            .segments
            .iter()
            .map(|s| s.ident.to_string())
            .collect();
        match segments.as_slice() {
            [only] => only == name,
            [krate, last] => krate == "evento" && last == name,
            _ => false,
        }
    })
}

/// The `name = ".."` of `#[evento::aggregate(..)]`.
fn pinned_name(attr: &syn::Attribute) -> anyhow::Result<Option<String>> {
    let syn::Meta::List(_) = attr.meta else {
        return Ok(None);
    };
    let args = attr.parse_args_with(Punctuated::<syn::Meta, Token![,]>::parse_terminated)?;
    Ok(args.iter().find_map(|meta| match meta {
        syn::Meta::NameValue(nv) if nv.path.is_ident("name") => match &nv.value {
            syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(name),
                ..
            }) => Some(name.value()),
            _ => None,
        },
        _ => None,
    }))
}

/// The `name = ".."` of `#[evento(..)]` on a variant.
fn stored_event_name(variant: &syn::Variant) -> anyhow::Result<Option<String>> {
    let mut name = None;
    for attr in &variant.attrs {
        if !attr.path().is_ident("evento") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                let value: syn::LitStr = meta.value()?.parse()?;
                name = Some(value.value());
            } else if meta.input.peek(Token![=]) {
                meta.value()?.parse::<syn::Expr>()?;
            }
            Ok(())
        })?;
    }
    Ok(name)
}

/// `#[evento::projection(cursor = <Type>, id = <field>, name = "..", Derive, ..)]`.
#[derive(Default)]
struct ProjectionArgs {
    cursor: Option<syn::Type>,
    name: Option<String>,
    derives: Vec<syn::Path>,
}

impl Parse for ProjectionArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut args = Self::default();
        while !input.is_empty() {
            if input.peek(syn::Ident) && input.peek2(Token![=]) {
                let key: syn::Ident = input.parse()?;
                input.parse::<Token![=]>()?;
                match key.to_string().as_str() {
                    "cursor" => args.cursor = Some(input.parse()?),
                    "name" => args.name = Some(input.parse::<syn::LitStr>()?.value()),
                    _ => {
                        input.parse::<syn::Expr>()?;
                    }
                }
            } else {
                args.derives.push(input.parse()?);
            }
            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }
        Ok(args)
    }
}

fn is_encode(path: &syn::Path) -> bool {
    path.segments
        .last()
        .is_some_and(|segment| segment.ident == "Encode")
}

fn derives_encode(attrs: &[syn::Attribute]) -> bool {
    attrs
        .iter()
        .filter(|attr| attr.path().is_ident("derive"))
        .any(|attr| {
            attr.parse_args_with(Punctuated::<syn::Path, Token![,]>::parse_terminated)
                // A derive list that does not parse is simply not ours.
                .is_ok_and(|paths| paths.iter().any(is_encode))
        })
}

fn field_text(field: &syn::Field) -> String {
    let name = field
        .ident
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();
    format!("{name}: {}", type_text(&field.ty))
}

fn fields_shape(fields: &syn::Fields) -> String {
    match fields {
        syn::Fields::Unit => "unit".to_owned(),
        syn::Fields::Named(named) => {
            let fields: Vec<String> = named.named.iter().map(field_text).collect();
            format!("{{ {} }}", fields.join(", "))
        }
        syn::Fields::Unnamed(unnamed) => {
            let fields: Vec<String> = unnamed.unnamed.iter().map(|f| type_text(&f.ty)).collect();
            format!("({})", fields.join(", "))
        }
    }
}

/// `Vec < (String , u32) >` as `Vec<(String, u32)>`.
fn type_text(ty: &syn::Type) -> String {
    let compact: String = ty
        .to_token_stream()
        .to_string()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    compact.replace(',', ", ")
}

fn mentions_of(fields: &syn::Fields) -> BTreeSet<String> {
    fields
        .iter()
        .flat_map(|f| mentions_of_type(&f.ty))
        .collect()
}

fn mentions_of_type(ty: &syn::Type) -> BTreeSet<String> {
    struct Idents(BTreeSet<String>);
    impl<'ast> Visit<'ast> for Idents {
        fn visit_path_segment(&mut self, segment: &'ast syn::PathSegment) {
            self.0.insert(segment.ident.to_string());
            syn::visit::visit_path_segment(self, segment);
        }
    }
    let mut idents = Idents(BTreeSet::new());
    idents.visit_type(ty);
    idents.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::check::{diff, Problem};

    fn scanner_of(scanner: &mut Scanner, module: &str, file: &str, source: &str) {
        let parsed = syn::parse_file(source).unwrap();
        let module: Vec<String> = module.split("::").map(str::to_owned).collect();
        scanner
            .source(
                "demo",
                &module,
                Path::new(file),
                Path::new("/nonexistent"),
                &parsed,
            )
            .unwrap();
    }

    fn lock_of(source: &str) -> anyhow::Result<Lock> {
        let parsed = syn::parse_file(source)?;
        let mut scanner = Scanner::new(false);
        scanner.source(
            "demo",
            &["demo".to_owned()],
            Path::new("lib.rs"),
            Path::new("/nonexistent"),
            &parsed,
        )?;
        scanner.finish()
    }

    fn shape<'a>(lock: &'a Lock, key: &str) -> Option<&'a str> {
        lock.get(key).map(|e| e.shape.as_str())
    }

    const V1: &str = r#"
        #[derive(Encode, Decode)] pub struct Money { pub minor: i64, pub currency: String }
        #[derive(Encode, Decode)] pub enum Status { Open, Closed }
        #[evento::aggregate(name = "demo/Order")]
        pub enum Order { Placed { total: Money, lines: Vec<(String, u32)> }, Cancelled }
        #[evento::projection(bitcode::Encode, bitcode::Decode)]
        pub struct OrderView { pub id: String, pub status: Status }
    "#;

    #[test]
    fn events_freeze_what_they_mention_and_views_carry_the_rest() -> anyhow::Result<()> {
        let lock = lock_of(V1)?;
        assert_eq!(
            shape(&lock, "event demo/Order::Placed"),
            Some("{ total: Money, lines: Vec<(String, u32)> }")
        );
        assert_eq!(shape(&lock, "event demo/Order::Cancelled"), Some("unit"));
        assert_eq!(
            shape(&lock, "type demo::Money"),
            Some("{ minor: i64, currency: String }")
        );
        // `Status` is only used by the view: part of its shape, not frozen.
        assert_eq!(shape(&lock, "type demo::Status"), None);
        assert_eq!(
            shape(&lock, "view demo::OrderView"),
            Some(
                "{ id: String, status: Status, cursor: String, aggregate_version: u16 } \
                 with Status enum { Open, Closed }"
            )
        );
        assert_eq!(Lock::parse(&lock.render())?, lock);
        assert!(diff(&lock, &lock).is_empty());
        Ok(())
    }

    #[test]
    fn frozen_shapes_cannot_change_but_new_ones_can_appear() -> anyhow::Result<()> {
        let locked = lock_of(V1)?;

        let field_added = lock_of(&V1.replace("total: Money,", "total: Money, note: String,"))?;
        assert!(matches!(
            diff(&locked, &field_added).as_slice(),
            [Problem::Changed { key, .. }] if key == "event demo/Order::Placed"
        ));
        let nested_changed = lock_of(&V1.replace("pub minor: i64", "pub minor: i128"))?;
        assert!(matches!(
            diff(&locked, &nested_changed).as_slice(),
            [Problem::Changed { key, .. }] if key == "type demo::Money"
        ));
        let renamed = lock_of(&V1.replace("Cancelled }", "Canceled }"))?;
        assert!(diff(&locked, &renamed).contains(&Problem::Removed {
            key: "event demo/Order::Cancelled".into()
        }));

        // The way forward: a new variant is only a lock to refresh.
        let grown = lock_of(&V1.replace("Cancelled }", "Cancelled, Noted { note: String } }"))?;
        let problems = diff(&locked, &grown);
        assert_eq!(
            problems,
            [Problem::OutOfDate {
                key: "event demo/Order::Noted".into()
            }]
        );
        assert!(!problems[0].is_breaking());
        Ok(())
    }

    #[test]
    fn stored_name_keys_the_event() -> anyhow::Result<()> {
        let pinned = r#"
            #[evento::aggregate(name = "demo/Payment")]
            pub enum Payment {
                #[evento(name = "legacy.captured", upcast_to = CapturedV2)]
                Captured { psp: String },
                CapturedV2 { psp: String, at: u64 },
            }
        "#;
        let locked = lock_of(pinned)?;
        assert_eq!(
            shape(&locked, "event demo/Payment::legacy.captured"),
            Some("{ psp: String }")
        );
        // The stored name is pinned: renaming the variant changes nothing.
        let renamed =
            lock_of(&pinned.replace("Captured { psp: String },", "CapturedV1 { psp: String },"))?;
        assert!(diff(&locked, &renamed).is_empty());
        Ok(())
    }

    #[test]
    fn unpinned_aggregate_uses_the_package_name() -> anyhow::Result<()> {
        let unpinned = "#[evento::aggregate(serde::Serialize)] pub enum Owner { Created }";
        let lock = lock_of(unpinned)?;
        assert_eq!(shape(&lock, "event demo/Owner::Created"), Some("unit"));

        let parsed = syn::parse_file(unpinned)?;
        let mut strict = Scanner::new(true);
        let result = strict.source(
            "demo",
            &["demo".to_owned()],
            Path::new("lib.rs"),
            Path::new("/nonexistent"),
            &parsed,
        );
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn views_record_the_cursor_and_their_projection_name() -> anyhow::Result<()> {
        let lock = lock_of(
            r#"
            #[evento::projection(name = "demo/Balance", cursor = evento::cursor::Value)]
            #[derive(bitcode::Encode, bitcode::Decode)]
            pub struct BalanceView { pub amount: i64 }
            "#,
        )?;
        assert_eq!(
            shape(&lock, "view demo/Balance"),
            Some("{ amount: i64, cursor: evento::cursor::Value, aggregate_version: u16 }")
        );
        Ok(())
    }

    #[test]
    fn views_not_snapshotted_through_the_executor_are_ignored() -> anyhow::Result<()> {
        let lock = lock_of(
            r#"
            #[evento::projection] pub struct Plain { pub a: i64 }
            #[evento::projection(bitcode::Encode, bitcode::Decode)]
            #[evento::snapshot(memory)]
            pub struct InMemory { pub a: i64 }
            "#,
        )?;
        assert!(lock.is_empty());
        Ok(())
    }

    #[test]
    fn same_view_name_in_sibling_modules() -> anyhow::Result<()> {
        let lock = lock_of(
            r#"
            mod a {
                #[evento::projection(bitcode::Encode, bitcode::Decode)]
                pub struct View { pub a: i64 }
                fn p<E>() -> Projection<E, View> { Projection::new::<A>().revision(2) }
            }
            mod b {
                #[evento::projection(bitcode::Encode, bitcode::Decode)]
                pub struct View { pub b: i64 }
            }
            "#,
        )?;
        assert_eq!(
            lock.get("view demo::a::View").and_then(|e| e.revision),
            Some(2)
        );
        assert_eq!(
            lock.get("view demo::b::View").and_then(|e| e.revision),
            Some(0)
        );
        Ok(())
    }

    #[test]
    fn a_view_may_change_only_with_a_new_revision() -> anyhow::Result<()> {
        let locked = lock_of(V1)?;
        let reshaped = V1.replace("Open, Closed", "Open, Closed, Archived");
        assert_eq!(
            diff(&locked, &lock_of(&reshaped)?),
            [Problem::ViewNeedsRevision {
                key: "view demo::OrderView".into(),
                revision: 0
            }]
        );
        let bumped = format!("{reshaped} fn p() {{ Projection::new().revision(1) }}");
        let bumped = lock_of(&bumped)?;
        assert_eq!(
            diff(&locked, &bumped),
            [Problem::OutOfDate {
                key: "view demo::OrderView".into()
            }]
        );
        assert!(matches!(
            diff(&bumped, &locked).as_slice(),
            [Problem::ViewRevisionDecreased { .. }]
        ));
        Ok(())
    }

    #[test]
    fn revision_follows_the_return_type_across_files() -> anyhow::Result<()> {
        let mut scanner = Scanner::new(false);
        scanner_of(
            &mut scanner,
            "demo::views",
            "views.rs",
            r#"
            #[evento::projection(bitcode::Encode, bitcode::Decode)]
            pub struct First { pub a: i64 }
            #[evento::projection(bitcode::Encode, bitcode::Decode)]
            pub struct Second { pub b: i64 }
            "#,
        );
        scanner_of(
            &mut scanner,
            "demo::wiring",
            "wiring.rs",
            r#"
            pub fn first<E: Executor>() -> Projection<E, First> {
                Projection::new::<A>().handler(h()).revision(3)
            }
            pub fn second<E: Executor>() -> evento::Projection<E, Second> {
                Projection::<E, Second>::new::<A>().revision(1).strict()
            }
            fn unrelated() -> Projection<E, NotSnapshotted> { Projection::new().revision(9) }
            "#,
        );
        let lock = scanner.finish()?;
        assert_eq!(
            lock.get("view demo::views::First").and_then(|e| e.revision),
            Some(3)
        );
        assert_eq!(
            lock.get("view demo::views::Second")
                .and_then(|e| e.revision),
            Some(1)
        );
        Ok(())
    }

    #[test]
    fn turbofish_names_the_view() -> anyhow::Result<()> {
        let lock = lock_of(
            r#"
            #[evento::projection(bitcode::Encode, bitcode::Decode)] pub struct A { pub a: i64 }
            #[evento::projection(bitcode::Encode, bitcode::Decode)] pub struct B { pub b: i64 }
            fn build() { let p = Projection::<E, B>::new::<Agg>().revision(4); }
            "#,
        )?;
        assert_eq!(lock.get("view demo::A").and_then(|e| e.revision), Some(0));
        assert_eq!(lock.get("view demo::B").and_then(|e| e.revision), Some(4));
        Ok(())
    }

    #[test]
    fn ambiguous_revision_is_refused() {
        let ambiguous = r#"
            #[evento::projection(bitcode::Encode, bitcode::Decode)] pub struct A { pub a: i64 }
            #[evento::projection(bitcode::Encode, bitcode::Decode)] pub struct B { pub b: i64 }
            fn build() { let p = Projection::new::<Agg>().revision(1); }
        "#;
        let error = lock_of(ambiguous).unwrap_err().to_string();
        assert!(error.contains("cannot tell which view"), "{error}");
    }

    #[test]
    fn test_only_items_are_not_persisted() -> anyhow::Result<()> {
        let lock = lock_of(
            r#"
            #[cfg(test)]
            mod tests {
                #[evento::aggregate(name = "demo/Fixture")] pub enum Fixture { Happened }
            }
            "#,
        )?;
        assert!(lock.is_empty());
        Ok(())
    }
}
