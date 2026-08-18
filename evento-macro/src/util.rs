use syn::{Error, FnArg, GenericArgument, PatType, PathArguments, Type, TypePath};

/// Inserts an elided lifetime as the first generic argument of a handler's
/// event parameter — `Event<AccountOpened>` becomes `Event<'_, AccountOpened>`
/// — so user handler signatures stay lifetime-free now that
/// `metadata::Event` / `metadata::RawEvent` borrow the raw event.
pub(crate) fn inject_elided_lifetime(arg: &mut FnArg) {
    let FnArg::Typed(PatType { ty, .. }) = arg else {
        return;
    };
    let Type::Path(type_path) = ty.as_mut() else {
        return;
    };
    let Some(segment) = type_path.path.segments.last_mut() else {
        return;
    };
    let PathArguments::AngleBracketed(args) = &mut segment.arguments else {
        return;
    };
    // Respect an explicitly written lifetime.
    if matches!(args.args.first(), Some(GenericArgument::Lifetime(_))) {
        return;
    }
    args.args.insert(0, syn::parse_quote!('_));
}

/// Extracts the full type and its first generic type argument from a typed
/// fn argument — e.g. `event: Event<AccountOpened>` yields
/// `(Event<AccountOpened>, AccountOpened)`.
pub(crate) fn extract_type_with_first_generic(arg: &FnArg) -> syn::Result<(&Type, &TypePath)> {
    let FnArg::Typed(PatType { ty, .. }) = arg else {
        return Err(Error::new_spanned(arg, "expected typed argument"));
    };

    let Type::Path(type_path) = ty.as_ref() else {
        return Err(Error::new_spanned(ty, "expected path type with generic"));
    };

    let segment = type_path
        .path
        .segments
        .last()
        .ok_or_else(|| Error::new_spanned(type_path, "empty type path"))?;

    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return Err(Error::new_spanned(
            segment,
            format!("expected generic arguments on {}", segment.ident),
        ));
    };

    let inner = args
        .args
        .iter()
        .find_map(|arg| match arg {
            GenericArgument::Type(Type::Path(p)) => Some(p),
            _ => None,
        })
        .ok_or_else(|| Error::new_spanned(args, "expected type argument"))?;

    Ok((ty.as_ref(), inner))
}

/// Best-effort write of a macro expansion to a file for inspection.
///
/// Resolves the output directory at expansion time (the *user* crate's
/// environment): `$CARGO_TARGET_DIR`, then `<manifest>/target`, then the
/// system temp dir. Never fails the build — the caller always returns the
/// real token stream.
pub(crate) fn write_debug_expansion(file_name: &str, tokens: &impl std::fmt::Display) {
    let target_dir = std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("CARGO_MANIFEST_DIR")
                .map(|dir| std::path::PathBuf::from(dir).join("target"))
        })
        .unwrap_or_else(std::env::temp_dir);
    let path = target_dir.join(file_name);

    match std::fs::create_dir_all(&target_dir)
        .and_then(|_| std::fs::write(&path, tokens.to_string()))
    {
        Ok(()) => eprintln!("evento: debug expansion written to {}", path.display()),
        Err(e) => eprintln!(
            "evento: could not write debug expansion to {}: {e}",
            path.display()
        ),
    }
}
