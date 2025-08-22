use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use sha3::{Digest, Sha3_256};
use std::ops::Deref;
use syn::{parse_macro_input, spanned::Spanned, Ident, ItemFn, ItemImpl, ItemStruct};

#[proc_macro_attribute]
pub fn handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    let item: ItemFn = parse_macro_input!(item);
    let fn_ident = item.sig.ident.to_owned();
    let struct_ident = item.sig.ident.to_string().to_case(Case::UpperCamel);
    let struct_ident = Ident::new(&struct_ident, item.span());
    let aggregator_ident = Ident::new(&attr.to_string(), item.span());
    let Some(syn::FnArg::Typed(event_arg)) = item.sig.inputs.get(1) else {
        return syn::Error::new_spanned(item, "Unable to find event input")
            .into_compile_error()
            .into();
    };

    let syn::Type::Path(ty) = *event_arg.ty.clone() else {
        return syn::Error::new_spanned(item, "Unable to find event input type")
            .into_compile_error()
            .into();
    };

    let Some(event_arg_segment) = ty.path.segments.first() else {
        return syn::Error::new_spanned(item, "Unable to find event input type iden")
            .into_compile_error()
            .into();
    };

    let syn::PathArguments::AngleBracketed(ref arguments) = event_arg_segment.arguments else {
        return syn::Error::new_spanned(item, "Unable to find event input type iden")
            .into_compile_error()
            .into();
    };

    let Some(syn::GenericArgument::Type(syn::Type::Path(ty))) = arguments.args.first() else {
        return syn::Error::new_spanned(item, "Unable to find event input type iden")
            .into_compile_error()
            .into();
    };

    let Some(segment) = ty.path.segments.first() else {
        return syn::Error::new_spanned(item, "Unable to find event input type iden")
            .into_compile_error()
            .into();
    };

    let event_ident = segment.ident.to_owned();

    quote! {
        struct #struct_ident;

        fn #fn_ident() -> #struct_ident { #struct_ident }

        impl<E: evento::Executor> evento::SubscribeHandler<E> for #struct_ident {
            fn handle<'async_trait>(&'async_trait self, context: &'async_trait evento::Context<'_, E>) -> std::pin::Pin<Box<dyn std::future::Future<Output=anyhow::Result<()>> + Send + 'async_trait>>
            where
                Self: Sync + 'async_trait
            {
                Box::pin(async move {
                    if let Some(data) = context.event.to_details()? {
                        return Self::#fn_ident(context, data).await;
                    }

                    Ok(())
                })
            }

            fn aggregator_type(&self) -> &'static str{ #aggregator_ident::name() }
            fn event_name(&self) -> &'static str { #event_ident::name() }
        }

        impl #struct_ident {
            #item
        }
    }.into()
}

#[proc_macro_attribute]
pub fn aggregator(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let item: ItemImpl = parse_macro_input!(item);
    let mut hasher = Sha3_256::new();

    let syn::Type::Path(item_path) = item.self_ty.deref() else {
        return syn::Error::new_spanned(item, "Unable to find name of impl struct")
            .into_compile_error()
            .into();
    };

    let Some(ident) = item_path.path.get_ident() else {
        return syn::Error::new_spanned(item, "Unable to get ident of impl struct")
            .into_compile_error()
            .into();
    };

    let handler_fns = item
        .items
        .iter()
        .filter_map(|item| {
            let syn::ImplItem::Fn(item_fn) = item else {
                return None;
            };

            hasher.update(item_fn.to_token_stream().to_string());
            let ident = item_fn.sig.ident.clone();

            Some(quote! {
                if let Some(data) = event.to_details()? {
                    self.#ident(data).await?;
                    return Ok(());
                }
            })
        })
        .collect::<proc_macro2::TokenStream>();

    let revision = format!("{:x}", hasher.finalize());
    let name = ident.to_string();

    quote! {
        #item

        impl evento::Aggregator for #ident {
            fn aggregate<'async_trait>(&'async_trait mut self, event: &'async_trait evento::Event) -> std::pin::Pin<Box<dyn std::future::Future<Output=anyhow::Result<()>> + Send + 'async_trait>>
            where
                Self: Sync + 'async_trait
            {
                Box::pin(async move {
                    #handler_fns

                    Ok(())
                })
            }

            fn revision() -> &'static str {
                #revision
            }
        }

        impl evento::AggregatorName for #ident {
            fn name() -> &'static str {
                #name
            }
        }
    }
    .into()
}

#[proc_macro_derive(AggregatorName)]
pub fn derive_aggregator_name(input: TokenStream) -> TokenStream {
    let ItemStruct { ident, .. } = parse_macro_input!(input);
    let name = ident.to_string();

    quote! {
        impl evento::AggregatorName for #ident {
            fn name() -> &'static str {
                #name
            }
        }
    }
    .into()
}
