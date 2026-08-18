use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Error, FnArg, ItemFn, PatType, Type};

pub fn handler_next_impl(input: &ItemFn, debug: bool) -> syn::Result<TokenStream> {
    // `metadata::Event` borrows the raw event, so the (macro-owned) handler
    // signature gets its lifetime injected — users keep writing `Event<T>`.
    let mut input = input.clone();
    if let Some(arg) = input.sig.inputs.first_mut() {
        crate::util::inject_elided_lifetime(arg);
    }
    let input = &input;
    let fn_name = &input.sig.ident;
    let vis = &input.vis;

    // Extract parameters
    let mut params = input.sig.inputs.iter();

    // First param: Event<AccountOpened>
    let event_arg = params.next().ok_or_else(|| {
        Error::new_spanned(&input.sig, "expected first parameter: event: Event<T>")
    })?;
    let (event_full_type, event_inner_type) =
        crate::util::extract_type_with_first_generic(event_arg)?;

    // Second param: the projection reference, e.g. `&mut AccountBalanceView`
    let action_arg = params.next().ok_or_else(|| {
        Error::new_spanned(
            &input.sig,
            "expected second parameter: `&mut YourProjection`",
        )
    })?;
    let projection_type = extract_projection_type(action_arg)?;

    // Generate struct name: AccountOpened -> AccountOpenedHandler
    let handler_struct = format_ident!("{}Handler", fn_name.to_string().to_case(Case::UpperCamel));

    let output = quote! {
        #vis struct #handler_struct;

        #vis fn #fn_name() -> #handler_struct { #handler_struct }

        impl #handler_struct {
            #input
        }

        impl ::evento::projection::Handler<#projection_type> for #handler_struct {
            fn handle<'a>(
                &'a self,
                projection: &'a mut #projection_type,
                event: &'a ::evento::Event,
            ) -> ::std::pin::Pin<Box<dyn ::std::future::Future<Output = ::anyhow::Result<()>> + Send + 'a>> {
                Box::pin(async move {
                    let event: #event_full_type = match event.try_into() {
                        Ok(data) => data,
                        Err(e) => return Err(e.into()),
                    };

                    Self::#fn_name(event, projection).await
                })
            }

            fn event_name(&self) -> &'static str {
                use ::evento::AggregateEvent as _;
                #event_inner_type::event_name()
            }

            fn aggregate_type(&self) -> &'static str {
                use ::evento::Aggregate as _;
                #event_inner_type::aggregate_type()
            }
        }
    };

    if debug {
        crate::util::write_debug_expansion("evento_debug_handler_macro.rs", &output);
    }

    Ok(output.into())
}

// Extract `AccountBalanceView` from `&mut AccountBalanceView`
fn extract_projection_type(arg: &FnArg) -> syn::Result<&Type> {
    let FnArg::Typed(PatType { ty, .. }) = arg else {
        return Err(Error::new_spanned(arg, "expected typed argument"));
    };

    let Type::Reference(type_path) = ty.as_ref() else {
        return Err(Error::new_spanned(
            ty,
            "expected a mutable reference like `&mut YourProjection`",
        ));
    };

    Ok(type_path.elem.as_ref())
}
