use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse_quote, Error, FnArg, GenericArgument, ImplItem, ImplItemFn, ItemImpl, Pat, PathArguments,
    Type,
};

pub fn command_impl(attr: TokenStream, input: &ItemImpl) -> syn::Result<TokenStream> {
    syn::parse::<syn::parse::Nothing>(attr)
        .map_err(|e| Error::new(e.span(), "#[evento::command] takes no arguments"))?;

    let mut items = Vec::with_capacity(input.items.len());
    for item in &input.items {
        match item {
            ImplItem::Fn(f) if has_routing_param(f)? => items.extend(expand_routing_fn(f)?),
            other => items.push(other.clone()),
        }
    }

    let mut output = input.clone();
    output.items = items;
    Ok(quote!(#output).into())
}

/// A method opts in by making its *last* parameter `routing_key: Option<String>`.
fn has_routing_param(f: &ImplItemFn) -> syn::Result<bool> {
    let Some(FnArg::Typed(pt)) = f.sig.inputs.last() else {
        return Ok(false);
    };
    let Pat::Ident(pi) = pt.pat.as_ref() else {
        return Ok(false);
    };
    if pi.ident != "routing_key" {
        return Ok(false);
    }
    if !is_option_string(&pt.ty) {
        return Err(Error::new_spanned(
            &pt.ty,
            "`routing_key` parameter must have type `Option<String>`",
        ));
    }
    Ok(true)
}

fn is_option_string(ty: &Type) -> bool {
    let Type::Path(tp) = ty else { return false };
    let Some(segment) = tp.path.segments.last() else {
        return false;
    };
    if segment.ident != "Option" {
        return false;
    }
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return false;
    };
    let mut args = args.args.iter();
    let (Some(GenericArgument::Type(Type::Path(inner))), None) = (args.next(), args.next()) else {
        return false;
    };
    inner
        .path
        .segments
        .last()
        .is_some_and(|s| s.ident == "String" && s.arguments.is_none())
}

/// Expands one `routing_key: Option<String>`-taking method into three:
/// `name_opt` (the original body, hidden), `name` (forwards `None`), and
/// `name_with_routing` (forwards `Some(key)`), matching the hand-written
/// `x` / `x_with_routing` convention.
fn expand_routing_fn(f: &ImplItemFn) -> syn::Result<Vec<ImplItem>> {
    match f.sig.inputs.first() {
        Some(FnArg::Receiver(recv))
            if matches!(recv.kind, syn::ReceiverKind::Reference(_, _, None)) => {}
        _ => {
            return Err(Error::new_spanned(
                &f.sig,
                "#[evento::command] methods with a `routing_key` parameter must take `&self`",
            ));
        }
    }
    if f.sig.asyncness.is_none() {
        return Err(Error::new_spanned(
            &f.sig,
            "#[evento::command] methods with a `routing_key` parameter must be async",
        ));
    }

    // Idents of every parameter between the receiver and `routing_key`, for
    // forwarding from the generated wrappers.
    let inputs: Vec<&FnArg> = f.sig.inputs.iter().collect();
    let mut idents = Vec::with_capacity(inputs.len().saturating_sub(2));
    for arg in &inputs[1..inputs.len() - 1] {
        let FnArg::Typed(pt) = arg else { continue };
        let Pat::Ident(pi) = pt.pat.as_ref() else {
            return Err(Error::new_spanned(
                &pt.pat,
                "#[evento::command] parameters must be simple identifiers",
            ));
        };
        idents.push(pi.ident.clone());
    }

    let name = &f.sig.ident;
    let opt_name = format_ident!("{name}_opt");
    let with_name = format_ident!("{name}_with_routing");

    let mut opt_fn = f.clone();
    opt_fn.sig.ident = opt_name.clone();
    opt_fn.attrs.push(parse_quote!(#[doc(hidden)]));

    let mut base_fn = f.clone();
    base_fn.sig.inputs.pop();
    base_fn.block = parse_quote!({ self.#opt_name(#(#idents,)* None).await });

    let mut with_fn = f.clone();
    with_fn.sig.ident = with_name;
    with_fn.sig.inputs.pop();
    with_fn
        .sig
        .inputs
        .push(parse_quote!(routing_key: impl ::core::convert::Into<::std::string::String>));
    with_fn.block = parse_quote!({
        self.#opt_name(
            #(#idents,)*
            ::core::option::Option::Some(::core::convert::Into::into(routing_key)),
        )
        .await
    });

    Ok(vec![
        ImplItem::Fn(opt_fn),
        ImplItem::Fn(base_fn),
        ImplItem::Fn(with_fn),
    ])
}
