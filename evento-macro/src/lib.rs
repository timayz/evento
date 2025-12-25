//! Procedural macros for the Evento event sourcing framework.
//!
//! This crate provides macros that eliminate boilerplate when building event-sourced
//! applications with Evento. It generates trait implementations, handler structs,
//! and serialization code automatically.
//!
//! # Macros
//!
//! | Macro | Type | Purpose |
//! |-------|------|---------|
//! | [`aggregator`] | Attribute | Transform enum into event structs with trait impls |
//! | [`handler`] | Attribute | Create event handler from async function |
//! | [`snapshot`] | Attribute | Implement snapshot restoration for projections |
//! | [`debug_handler`] | Attribute | Like `handler` but outputs generated code for debugging |
//! | [`debug_snapshot`] | Attribute | Like `snapshot` but outputs generated code for debugging |
//!
//! # Usage
//!
//! This crate is typically used through the main `evento` crate with the `macro` feature
//! enabled (on by default):
//!
//! ```toml
//! [dependencies]
//! evento = "1.8"
//! ```
//!
//! # Examples
//!
//! ## Defining Events with `#[evento::aggregator]`
//!
//! Transform an enum into individual event structs:
//!
//! ```rust,ignore
//! #[evento::aggregator]
//! pub enum BankAccount {
//!     /// Event raised when a new bank account is opened
//!     AccountOpened {
//!         owner_id: String,
//!         owner_name: String,
//!         initial_balance: i64,
//!     },
//!
//!     MoneyDeposited {
//!         amount: i64,
//!         transaction_id: String,
//!     },
//!
//!     MoneyWithdrawn {
//!         amount: i64,
//!         transaction_id: String,
//!     },
//! }
//! ```
//!
//! This generates:
//! - `AccountOpened`, `MoneyDeposited`, `MoneyWithdrawn` structs
//! - `Aggregator` and `Event` trait implementations for each
//! - Automatic derives: `Debug`, `Clone`, `PartialEq`, `Default`, and rkyv serialization
//!
//! ## Creating Handlers with `#[evento::handler]`
//!
//! ```rust,ignore
//! #[evento::handler]
//! async fn handle_money_deposited<E: Executor>(
//!     event: Event<MoneyDeposited>,
//!     action: Action<'_, AccountBalanceView, E>,
//! ) -> anyhow::Result<()> {
//!     match action {
//!         Action::Apply(row) => {
//!             row.balance += event.data.amount;
//!         }
//!         Action::Handle(_context) => {}
//!     };
//!     Ok(())
//! }
//!
//! // Use in a projection
//! let projection = Projection::new("account-balance")
//!     .handler(handle_money_deposited());
//! ```
//!
//! ## Snapshot Restoration with `#[evento::snapshot]`
//!
//! ```rust,ignore
//! #[evento::snapshot]
//! async fn restore(
//!     context: &evento::context::RwContext,
//!     id: String,
//! ) -> anyhow::Result<Option<LoadResult<AccountBalanceView>>> {
//!     // Load snapshot from database or return None
//!     Ok(None)
//! }
//! ```
//!
//! # Requirements
//!
//! When using these macros, your types must meet certain requirements:
//!
//! - **Events** (from `#[aggregator]`): Automatically derive required traits
//! - **Projections**: Must implement `Default`, `Send`, `Sync`, `Clone`
//! - **Handler functions**: Must be `async` and return `anyhow::Result<()>`
//!
//! # Serialization
//!
//! Events are serialized using [rkyv](https://rkyv.org/) for zero-copy deserialization.
//! The `#[aggregator]` macro automatically adds the required rkyv derives.

use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse_macro_input, punctuated::Punctuated, Error, Fields, FnArg, GenericArgument, ItemEnum,
    ItemFn, Meta, PatType, PathArguments, ReturnType, Token, Type, TypePath,
};

/// Transforms an enum into individual event structs with trait implementations.
///
/// This macro takes an enum where each variant represents an event type and generates:
/// - Individual public structs for each variant
/// - `Aggregator` trait implementation (provides `aggregator_type()`)
/// - `Event` trait implementation (provides `event_name()`)
/// - Automatic derives: `Debug`, `Clone`, `PartialEq`, `Default`, and rkyv serialization
///
/// # Aggregator Type Format
///
/// The aggregator type is formatted as `"{package_name}/{enum_name}"`, e.g., `"bank/BankAccount"`.
///
/// # Example
///
/// ```rust,ignore
/// #[evento::aggregator]
/// pub enum BankAccount {
///     /// Event raised when account is opened
///     AccountOpened {
///         owner_id: String,
///         owner_name: String,
///         initial_balance: i64,
///     },
///
///     MoneyDeposited {
///         amount: i64,
///         transaction_id: String,
///     },
/// }
///
/// // Generated structs can be used directly:
/// let event = AccountOpened {
///     owner_id: "user123".into(),
///     owner_name: "John".into(),
///     initial_balance: 1000,
/// };
/// ```
///
/// # Additional Derives
///
/// Pass additional derives as arguments:
///
/// ```rust,ignore
/// #[evento::aggregator(serde::Serialize, serde::Deserialize)]
/// pub enum MyEvents {
///     // variants...
/// }
/// ```
///
/// # Variant Types
///
/// Supports all enum variant types:
/// - Named fields: `Variant { field: Type }`
/// - Tuple fields: `Variant(Type1, Type2)`
/// - Unit variants: `Variant`
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
            quote! { #[derive(Debug, Clone, PartialEq, Default, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)] }
        } else {
            quote! { #[derive(Debug, Clone, PartialEq, Default, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, #(#user_derives),*)] }
        };

        let impl_event = quote! {
            impl evento::projection::Aggregator for #variant_name {
                fn aggregator_type() -> &'static str {
                    static NAME: std::sync::LazyLock<String> = std::sync::LazyLock::new(||{
                        format!("{}/{}", env!("CARGO_PKG_NAME"), #enum_name_str)
                    });

                    &NAME
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

        #[derive(Default)]
        #vis struct #enum_name;

        impl evento::projection::Aggregator for #enum_name {
            fn aggregator_type() -> &'static str {
                static NAME: std::sync::LazyLock<String> = std::sync::LazyLock::new(||{
                    format!("{}/{}", env!("CARGO_PKG_NAME"), #enum_name_str)
                });

                &NAME
            }
        }
    }
    .into()
}

/// Creates an event handler from an async function.
///
/// This macro transforms an async function into a handler struct that implements
/// the `Handler<P, E>` trait for use with projections.
///
/// # Function Signature
///
/// The function must have this signature:
///
/// ```rust,ignore
/// async fn handler_name<E: Executor>(
///     event: Event<EventType>,
///     action: Action<'_, ProjectionType, E>,
/// ) -> anyhow::Result<()>
/// ```
///
/// # Generated Code
///
/// For a function `handle_money_deposited`, the macro generates:
/// - `HandleMoneyDepositedHandler` struct
/// - `handle_money_deposited()` constructor function
/// - `Handler<ProjectionType, E>` trait implementation
///
/// # Example
///
/// ```rust,ignore
/// #[evento::handler]
/// async fn handle_money_deposited<E: Executor>(
///     event: Event<MoneyDeposited>,
///     action: Action<'_, AccountBalanceView, E>,
/// ) -> anyhow::Result<()> {
///     match action {
///         Action::Apply(row) => {
///             row.balance += event.data.amount;
///         }
///         Action::Handle(_context) => {
///             // Side effects, notifications, etc.
///         }
///     };
///     Ok(())
/// }
///
/// // Register with projection
/// let projection = Projection::new("account-balance")
///     .handler(handle_money_deposited());
/// ```
///
/// # Action Variants
///
/// - `Action::Apply(row)` - Mutate projection state (for rebuilding from events)
/// - `Action::Handle(context)` - Handle side effects during live processing
#[proc_macro_attribute]
pub fn handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    match handler_next_impl(&input, false) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}

/// Debug variant of [`handler`] that writes generated code to a file.
///
/// The generated code is written to `target/evento_debug_handler_macro.rs`
/// for inspection. Useful for understanding what the macro produces.
///
/// # Example
///
/// ```rust,ignore
/// #[evento::debug_handler]
/// async fn handle_event<E: Executor>(
///     event: Event<MyEvent>,
///     action: Action<'_, MyView, E>,
/// ) -> anyhow::Result<()> {
///     // ...
/// }
/// ```
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

/// Implements the `Snapshot` trait for projection state restoration.
///
/// This macro takes an async function that restores a projection from a snapshot
/// and generates the `Snapshot` trait implementation.
///
/// # Function Signature
///
/// The function must have this signature:
///
/// ```rust,ignore
/// async fn restore(
///     context: &evento::context::RwContext,
///     id: String,
/// ) -> anyhow::Result<Option<LoadResult<ProjectionType>>>
/// ```
///
/// # Return Value
///
/// - `Ok(Some(LoadResult { ... }))` - Snapshot found, restore from this state
/// - `Ok(None)` - No snapshot, rebuild from events
/// - `Err(...)` - Error during restoration
///
/// # Example
///
/// ```rust,ignore
/// #[evento::snapshot]
/// async fn restore(
///     context: &evento::context::RwContext,
///     id: String,
/// ) -> anyhow::Result<Option<LoadResult<AccountBalanceView>>> {
///     // Query snapshot from database
///     let snapshot = context.read()
///         .query_snapshot::<AccountBalanceView>(&id)
///         .await?;
///
///     Ok(snapshot)
/// }
/// ```
///
/// # Generated Code
///
/// The macro generates a `Snapshot` trait implementation for the projection type
/// extracted from the return type's `LoadResult<T>`.
#[proc_macro_attribute]
pub fn snapshot(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    match snapshot_impl(&input, false) {
        Ok(tokens) => tokens,
        Err(e) => e.to_compile_error().into(),
    }
}

/// Debug variant of [`snapshot`] that writes generated code to a file.
///
/// The generated code is written to `target/evento_debug_snapshot_macro.rs`
/// for inspection. Useful for understanding what the macro produces.
///
/// # Example
///
/// ```rust,ignore
/// #[evento::debug_snapshot]
/// async fn restore(
///     context: &evento::context::RwContext,
///     id: String,
/// ) -> anyhow::Result<Option<LoadResult<MyView>>> {
///     Ok(None)
/// }
/// ```
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
    let load_result_type = extract_generic_inner(option_type, "Option")?;

    // Level 3: LoadResult<T>
    let projection_type = extract_generic_inner(load_result_type, "LoadResult")?;

    let output = quote! {
        #input

        impl ::evento::projection::Snapshot for #projection_type {
            fn restore<'a>(
                context: &'a ::evento::context::RwContext,
                id: String,
            ) -> ::std::pin::Pin<Box<dyn ::std::future::Future<Output = ::anyhow::Result<Option<evento::LoadResult<Self>>>> + Send + 'a>> {
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
