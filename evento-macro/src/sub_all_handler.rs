use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Error, FnArg, GenericArgument, ItemFn, PatType, PathArguments, Type, TypePath};

pub fn handler_next_impl(input: &ItemFn, debug: bool) -> syn::Result<TokenStream> {
    let fn_name = &input.sig.ident;

    // Extract parameters
    let mut params = input.sig.inputs.iter();

    let _ = params.next();
    // First param: Event<AccountOpened>
    let event_arg = params.next().ok_or_else(|| {
        Error::new_spanned(&input.sig, "expected first parameter: event: Event<T>")
    })?;
    let (_event_full_type, event_inner_type) = extract_type_with_first_generic(event_arg)?;

    // Generate struct name: AccountOpened -> AccountOpenedHandler
    let handler_struct = format_ident!("{}Handler", fn_name.to_string().to_case(Case::UpperCamel));

    let output = quote! {
        pub struct #handler_struct;

        fn #fn_name() -> #handler_struct { #handler_struct }

        impl #handler_struct {
            #input
        }

        impl<E: ::evento::Executor> ::evento::subscription::Handler<E> for #handler_struct {
            fn handle<'a>(
                &'a self,
                context: &'a ::evento::subscription::Context<'a, E>,
                event: &'a ::evento::Event,
            ) -> ::std::pin::Pin<Box<dyn ::std::future::Future<Output = ::anyhow::Result<()>> + Send + 'a>> {
                Box::pin(async move {
                    let event = ::evento::metadata::RawEvent(event.clone(), ::std::marker::PhantomData);
                    Self::#fn_name(context, event).await
                })
            }

            fn event_name(&self) -> &'static str {
                "all"
            }

            fn aggregator_type(&self) -> &'static str {
                use ::evento::Aggregator as _;
                #event_inner_type::aggregator_type()
            }
        }
    };

    if !debug {
        return Ok(output.into());
    }

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let debug_path =
        std::path::PathBuf::from(&manifest_dir).join("../target/evento_debug_handler_macro.rs"); // adjust ../ as needed

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

// Extract full type and first generic type argument
// e.g., `EventData<AccountOpened, true>` -> (full type, AccountOpened)
fn extract_type_with_first_generic(arg: &FnArg) -> syn::Result<(&Type, &TypePath)> {
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

    // Find first Type::Path argument
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
