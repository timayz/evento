//! Finding the workspace and the crates to scan, through `cargo metadata`.

use std::{
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::Context;
use serde_json::Value;

/// A workspace member.
pub(crate) struct Package {
    pub name: String,
    pub targets: Vec<Target>,
}

/// A library or binary target of a package.
pub(crate) struct Target {
    /// The crate root's name, as `module_path!()` spells it.
    pub crate_ident: String,
    pub src_path: PathBuf,
}

pub(crate) struct Workspace {
    pub root: PathBuf,
    pub packages: Vec<Package>,
}

const SCANNED_KINDS: &[&str] = &["lib", "rlib", "dylib", "cdylib", "staticlib", "bin"];

/// Runs `cargo metadata --no-deps` for the package in `manifest_dir`.
pub(crate) fn workspace(manifest_dir: &Path) -> anyhow::Result<Workspace> {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = Command::new(cargo)
        .args([
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--manifest-path",
        ])
        .arg(manifest_dir.join("Cargo.toml"))
        .output()
        .context("cannot run `cargo metadata`")?;
    anyhow::ensure!(
        output.status.success(),
        "`cargo metadata` failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    parse(&serde_json::from_slice(&output.stdout)?)
}

fn parse(metadata: &Value) -> anyhow::Result<Workspace> {
    let text = |value: &Value, what: &str| -> anyhow::Result<String> {
        value
            .as_str()
            .map(str::to_owned)
            .with_context(|| format!("`cargo metadata` output has no {what}"))
    };
    let root = PathBuf::from(text(&metadata["workspace_root"], "workspace_root")?);
    let mut packages = Vec::new();
    for package in metadata["packages"].as_array().into_iter().flatten() {
        let mut targets = Vec::new();
        for target in package["targets"].as_array().into_iter().flatten() {
            let scanned = target["kind"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|kind| kind.as_str().is_some_and(|k| SCANNED_KINDS.contains(&k)));
            if scanned {
                targets.push(Target {
                    crate_ident: text(&target["name"], "target name")?.replace('-', "_"),
                    src_path: PathBuf::from(text(&target["src_path"], "src_path")?),
                });
            }
        }
        packages.push(Package {
            name: text(&package["name"], "package name")?,
            targets,
        });
    }
    packages.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Workspace { root, packages })
}
