use proc_macro::TokenStream;
use quote::quote;
use syn::{Error, GenericArgument, ItemFn, PathArguments, ReturnType, Type};

pub fn snapshot_impl(input: &ItemFn, debug: bool) -> syn::Result<TokenStream> {
    let fn_name = &input.sig.ident;

    // Extract return type: anyhow::Result<Option<AccountBalanceView>>
    let return_type = match &input.sig.output {
        ReturnType::Type(_, ty) => ty,
        ReturnType::Default => {
            return Err(Error::new_spanned(
                &input.sig,
                "expected return type: anyhow::Result<Option<T>>",
            ));
        }
    };

    // Extract the inner type (AccountBalanceView) from Result<Option<T>>
    // let projection_type = extract_result_option_inner(return_type)?;

    // Level 1: Result<...>
    let option_type = extract_generic_inner(return_type, "Result")?;

    // Level 2: Option<...>
    let projection_type = extract_generic_inner(option_type, "Option")?;

    let output = quote! {
        #input

        impl ::evento::projection::Snapshot for #projection_type {
            fn restore<'a>(
                context: &'a ::evento::context::RwContext,
                id: String,
                aggregators: &'a ::std::collections::HashMap<String, String>,
            ) -> ::std::pin::Pin<Box<dyn ::std::future::Future<Output = ::anyhow::Result<Option<Self>>> + Send + 'a>> {
                Box::pin(async move { #fn_name(context, id, aggregators).await })
            }
        }
    };

    if !debug {
        return Ok(output.into());
    }

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let debug_path =
        std::path::PathBuf::from(&manifest_dir).join("../target/evento_debug_snapshot_macro.rs"); // adjust ../ as needed

    std::fs::write(&debug_path, output.to_string()).ok();

    let debug_path_str = debug_path
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .to_string();

    Ok(quote! {
        include!(#debug_path_str);
    }
    .into())
}

// Extract inner type from Wrapper<T>
fn extract_generic_inner<'a>(ty: &'a Type, expected: &str) -> syn::Result<&'a Type> {
    let Type::Path(type_path) = ty else {
        return Err(Error::new_spanned(ty, format!("expected {}<T>", expected)));
    };

    let segment = type_path
        .path
        .segments
        .last()
        .ok_or_else(|| Error::new_spanned(type_path, "empty type path"))?;

    if segment.ident != expected {
        return Err(Error::new_spanned(
            segment,
            format!("expected {}, found {}", expected, segment.ident),
        ));
    }

    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return Err(Error::new_spanned(
            segment,
            format!("expected {}<T>", expected),
        ));
    };

    args.args
        .iter()
        .find_map(|arg| match arg {
            GenericArgument::Type(t) => Some(t),
            _ => None,
        })
        .ok_or_else(|| Error::new_spanned(args, format!("expected type in {}<T>", expected)))
}
