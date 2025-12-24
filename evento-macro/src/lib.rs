//! # Evento Macros
//!
//! This crate provides procedural macros for the Evento event sourcing framework.
//! These macros simplify the implementation of aggregators and event handlers by
//! generating boilerplate code automatically.
//!
//! ## Macros
//!
//! - [`aggregator`] - Implements the [`Aggregator`] trait for structs with event handler methods
//! - [`handler`] - Creates event handler functions for use with subscriptions
//! - [`AggregatorName`] - Derives the [`AggregatorName`] trait for event types
//!
//! ## Usage
//!
//! This crate is typically used through the main `evento` crate with the `macro` feature enabled:
//!
//! ```toml
//! [dependencies]
//! evento = { version = "1.0", features = ["macro"] }
//! ```
//!
//! ## Examples
//!
//! See the individual macro documentation for detailed usage examples.

use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, ToTokens};
use sha3::{Digest, Sha3_256};
use std::ops::Deref;
use syn::{
    parse_macro_input, punctuated::Punctuated, spanned::Spanned, Error, Fields, FnArg,
    GenericArgument, Ident, ItemEnum, ItemFn, ItemImpl, ItemStruct, Meta, PatType, PathArguments,
    ReturnType, Token, Type, TypePath,
};

#[proc_macro_attribute]
pub fn aggregator(attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemEnum);

    let enum_name = &input.ident;
    let enum_name_str = enum_name.to_string();
    let vis = &input.vis;

    let user_derives = if attr.is_empty() {
        vec![]
    } else {
        let parser = Punctuated::<Meta, Token![,]>::parse_terminated;
        let parsed = syn::parse::Parser::parse(parser, attr).unwrap_or_default();
        parsed.into_iter().collect::<Vec<_>>()
    };

    // Generate a struct for each variant
    let structs = input.variants.iter().map(|variant| {
        let variant_name = &variant.ident;
        let variant_name_str = variant_name.to_string();
        let attrs = &variant.attrs; // preserves doc comments

        // Mandatory + user derives
        let derives = if user_derives.is_empty() {
            quote! { #[derive(Debug, Clone, PartialEq, Default, bincode::Encode, bincode::Decode)] }
        } else {
            quote! { #[derive(Debug, Clone, PartialEq, Default, bincode::Encode, bincode::Decode, #(#user_derives),*)] }
        };

        let impl_event = quote! {
            impl evento::projection::Aggregator for #variant_name {
                fn aggregator_type() -> &'static str {
                    #enum_name_str
                }
            }

            impl evento::projection::Event for #variant_name {
                fn event_name() -> &'static str {
                    #variant_name_str
                }
            }
        };

        match &variant.fields {
            Fields::Named(fields) => {
                let fields = fields.named.iter().map(|f| {
                    let field_name = &f.ident;
                    let field_ty = &f.ty;
                    let field_attrs = &f.attrs;
                    quote! {
                        #(#field_attrs)*
                        pub #field_name: #field_ty
                    }
                });

                quote! {
                    #(#attrs)*
                    #derives
                    #vis struct #variant_name {
                        #(#fields),*
                    }
                    #impl_event
                }
            }
            Fields::Unnamed(fields) => {
                let fields = fields.unnamed.iter().map(|f| {
                    let field_ty = &f.ty;
                    quote! { pub #field_ty }
                });

                quote! {
                    #(#attrs)*
                    #derives
                    #vis struct #variant_name(#(#fields),*);
                    #impl_event
                }
            }
            Fields::Unit => {
                quote! {
                    #(#attrs)*
                    #derives
                    #vis struct #variant_name;
                    #impl_event
                }
            }
        }
    });

    // Optionally keep the original enum too
    quote! {
        #(#structs)*

        // If you want to also keep the enum, add:
        // #input
    }
    .into()
}

#[proc_macro_attribute]
pub fn handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    match handler_next_impl(&input, false) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn debug_handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    match handler_next_impl(&input, true) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}

fn handler_next_impl(input: &ItemFn, debug: bool) -> syn::Result<TokenStream> {
    let fn_name = &input.sig.ident;

    // Extract parameters
    let mut params = input.sig.inputs.iter();

    // First param: Event<AccountOpened>
    let event_arg = params.next().ok_or_else(|| {
        Error::new_spanned(&input.sig, "expected first parameter: event: Event<T>")
    })?;
    let (event_full_type, event_inner_type) = extract_type_with_first_generic(event_arg)?;

    // Second param: Action<'_, AccountBalanceView, E>
    let action_arg = params.next().ok_or_else(|| {
        Error::new_spanned(
            &input.sig,
            "expected second parameter: action: Action<'_, P, E>",
        )
    })?;
    let projection_type = extract_projection_type(action_arg)?;

    // Generate struct name: AccountOpened -> AccountOpenedHandler
    let handler_struct = format_ident!("{}Handler", fn_name.to_string().to_case(Case::UpperCamel));

    let output = quote! {
        pub struct #handler_struct;

        fn #fn_name() -> #handler_struct { #handler_struct }

        impl #handler_struct {
            #input
        }

        impl<E: ::evento::Executor> ::evento::projection::Handler<#projection_type, E> for #handler_struct {
            fn apply<'a>(
                &'a self,
                projection: &'a mut #projection_type,
                event: &'a ::evento::Event,
            ) -> ::std::pin::Pin<Box<dyn ::std::future::Future<Output = ::anyhow::Result<()>> + Send + 'a>> {
                Box::pin(async move {
                    let event: #event_full_type = match event.try_into() {
                        Ok(data) => data,
                        Err(e) => return Err(e.into()),
                    };
                    Self::#fn_name(
                        event,
                        ::evento::projection::Action::<'_, _, E>::Apply(projection),
                    )
                    .await
                })
            }

            fn handle<'a>(
                &'a self,
                context: &'a ::evento::projection::Context<'a, E>,
                event: &'a ::evento::Event,
            ) -> ::std::pin::Pin<Box<dyn ::std::future::Future<Output = ::anyhow::Result<()>> + Send + 'a>> {
                Box::pin(async move {
                    let event: #event_full_type = match event.try_into() {
                        Ok(data) => data,
                        Err(e) => return Err(e.into()),
                    };
                    Self::#fn_name(event, ::evento::projection::Action::Handle(context)).await
                })
            }

            fn event_name(&self) -> &'static str {
                use ::evento::projection::Event as _;
                #event_inner_type::event_name()
            }

            fn aggregator_type(&self) -> &'static str {
                use ::evento::projection::Aggregator as _;
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

// Extract `AccountBalanceView` from `Action<'_, AccountBalanceView, E>`
fn extract_projection_type(arg: &FnArg) -> syn::Result<&TypePath> {
    let FnArg::Typed(PatType { ty, .. }) = arg else {
        return Err(Error::new_spanned(arg, "expected typed argument"));
    };

    let Type::Path(type_path) = ty.as_ref() else {
        return Err(Error::new_spanned(
            ty,
            "expected path type like Action<'_, P, E>",
        ));
    };

    let segment = type_path
        .path
        .segments
        .last()
        .ok_or_else(|| Error::new_spanned(type_path, "empty type path"))?;

    if segment.ident != "Action" {
        return Err(Error::new_spanned(
            segment,
            format!("expected Action<'_, P, E>, found {}", segment.ident),
        ));
    }

    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return Err(Error::new_spanned(
            segment,
            "expected generic arguments: Action<'_, P, E>",
        ));
    };

    // Find first Type argument (skip lifetime)
    let inner = args
        .args
        .iter()
        .find_map(|arg| match arg {
            GenericArgument::Type(Type::Path(p)) => Some(p),
            _ => None,
        })
        .ok_or_else(|| Error::new_spanned(args, "expected projection type in Action<'_, P, E>"))?;

    Ok(inner)
}

#[proc_macro_attribute]
pub fn snapshot(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    match snapshot_impl(&input, false) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}

#[proc_macro_attribute]
pub fn debug_snapshot(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    match snapshot_impl(&input, true) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}

fn snapshot_impl(input: &ItemFn, debug: bool) -> syn::Result<TokenStream> {
    let fn_name = &input.sig.ident;
    let fn_vis = &input.vis;
    let fn_stmts = &input.block.stmts;

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
    let projection_type = extract_result_option_inner(return_type)?;

    let output = quote! {
        #fn_vis async fn #fn_name(
            context: &::evento::context::RwContext,
            id: String,
        ) -> ::anyhow::Result<Option<#projection_type>> {
            #(#fn_stmts)*
        }

        impl ::evento::projection::Snapshot for #projection_type {
            fn restore<'a>(
                context: &'a ::evento::context::RwContext,
                id: String,
            ) -> ::std::pin::Pin<Box<dyn ::std::future::Future<Output = ::anyhow::Result<Option<Self>>> + Send + 'a>> {
                Box::pin(async move { #fn_name(context, id).await })
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

// Extract T from Result<Option<T>> or anyhow::Result<Option<T>>
fn extract_result_option_inner(ty: &Type) -> syn::Result<TokenStream2> {
    let Type::Path(type_path) = ty else {
        return Err(Error::new_spanned(ty, "expected Result<Option<T>>"));
    };

    let segment = type_path
        .path
        .segments
        .last()
        .ok_or_else(|| Error::new_spanned(type_path, "empty type path"))?;

    if segment.ident != "Result" {
        return Err(Error::new_spanned(
            segment,
            format!("expected Result, found {}", segment.ident),
        ));
    }

    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return Err(Error::new_spanned(segment, "expected Result<Option<T>>"));
    };

    // Get first type arg (Option<T>)
    let option_type = args
        .args
        .iter()
        .find_map(|arg| match arg {
            GenericArgument::Type(t) => Some(t),
            _ => None,
        })
        .ok_or_else(|| Error::new_spanned(args, "expected type argument"))?;

    // Extract T from Option<T>
    let Type::Path(option_path) = option_type else {
        return Err(Error::new_spanned(option_type, "expected Option<T>"));
    };

    let option_segment = option_path
        .path
        .segments
        .last()
        .ok_or_else(|| Error::new_spanned(option_path, "empty type path"))?;

    if option_segment.ident != "Option" {
        return Err(Error::new_spanned(
            option_segment,
            format!("expected Option, found {}", option_segment.ident),
        ));
    }

    let PathArguments::AngleBracketed(option_args) = &option_segment.arguments else {
        return Err(Error::new_spanned(option_segment, "expected Option<T>"));
    };

    let inner = option_args
        .args
        .iter()
        .find_map(|arg| match arg {
            GenericArgument::Type(t) => Some(t),
            _ => None,
        })
        .ok_or_else(|| Error::new_spanned(option_args, "expected type in Option<T>"))?;

    Ok(inner.to_token_stream())
}
