use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{Error, ItemFn};

pub fn subscription_all_next_impl(input: &ItemFn) -> syn::Result<TokenStream> {
    // The typed event borrows the raw event, so the (macro-owned) handler
    // signature gets its lifetime injected — users keep writing `RawEvent<A>`.
    let mut input = input.clone();
    if let Some(arg) = input.sig.inputs.iter_mut().nth(1) {
        crate::util::inject_elided_lifetime(arg);
    }
    let input = &input;
    let fn_name = &input.sig.ident;
    let vis = &input.vis;

    // Extract parameters
    let mut params = input.sig.inputs.iter();

    let _ = params.next();
    // Second param: RawEvent<BankAccount>
    let event_arg = params.next().ok_or_else(|| {
        Error::new_spanned(&input.sig, "expected second parameter: event: RawEvent<A>")
    })?;
    let (_event_full_type, event_inner_type) =
        crate::util::extract_type_with_first_generic(event_arg)?;

    // Generate struct name: AccountOpened -> AccountOpenedHandler
    let handler_struct = format_ident!("{}Handler", fn_name.to_string().to_case(Case::UpperCamel));

    let output = quote! {
        #vis struct #handler_struct;

        #vis fn #fn_name() -> #handler_struct { #handler_struct }

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
                    let event = ::evento::metadata::RawEvent(event, ::std::marker::PhantomData);
                    Self::#fn_name(context, event).await
                })
            }

            fn event_name(&self) -> &'static str {
                "all"
            }

            fn aggregate_type(&self) -> &'static str {
                use ::evento::Aggregate as _;
                #event_inner_type::aggregate_type()
            }
        }
    };

    Ok(output.into())
}
