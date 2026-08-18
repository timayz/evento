use syn::{FnArg, GenericArgument, PatType, PathArguments, Type};

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
