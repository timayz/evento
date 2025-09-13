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
use quote::{quote, ToTokens};
use sha3::{Digest, Sha3_256};
use std::ops::Deref;
use syn::{parse_macro_input, spanned::Spanned, Ident, ItemFn, ItemImpl, ItemStruct};

/// Creates an event handler for use with event subscriptions
///
/// The `#[evento::handler(AggregateType)]` attribute macro transforms a function into an event handler
/// that can be used with [`evento::subscribe`]. The macro generates the necessary boilerplate to
/// integrate with the subscription system.
///
/// # Syntax
///
/// ```ignore
/// #[evento::handler(AggregateType)]
/// async fn handler_name<E: evento::Executor>(
///     context: &evento::Context<'_, E>,
///     event: EventDetails<EventType>,
/// ) -> anyhow::Result<()> {
///     // Handler logic
///     Ok(())
/// }
/// ```
///
/// # Parameters
///
/// - `AggregateType`: The aggregate type this handler is associated with
///
/// # Function Requirements
///
/// The decorated function must:
/// - Be `async`
/// - Take `&evento::Context<'_, E>` as first parameter where `E: evento::Executor`
/// - Take `EventDetails<SomeEventType>` as second parameter
/// - Return `anyhow::Result<()>`
///
/// # Examples
///
/// ```no_run
/// use evento::{Context, EventDetails, Executor};
/// use bincode::{Encode, Decode};
///
/// # use evento::AggregatorName;
/// # #[derive(AggregatorName, Encode, Decode)]
/// # struct UserCreated { name: String }
/// # #[derive(Default, Encode, Decode, Clone, Debug)]
/// # struct User;
/// # #[evento::aggregator]
/// # impl User {}
///
/// #[evento::handler(User)]
/// async fn on_user_created<E: Executor>(
///     context: &Context<'_, E>,
///     event: EventDetails<UserCreated>,
/// ) -> anyhow::Result<()> {
///     println!("User created: {}", event.data.name);
///     
///     // Can trigger side effects, call external services, etc.
///     
///     Ok(())
/// }
///
/// // Use with subscription
/// # async fn setup(executor: evento::Sqlite) -> anyhow::Result<()> {
/// evento::subscribe("user-handlers")
///     .aggregator::<User>()
///     .handler(on_user_created())
///     .run(&executor)
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// # Generated Code
///
/// The macro generates:
/// - A struct implementing [`evento::SubscribeHandler`]
/// - A constructor function returning an instance of that struct
/// - Type-safe event filtering based on the event type
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

/// Implements the [`evento::Aggregator`] trait for structs with event handler methods
///
/// The `#[evento::aggregator]` attribute macro automatically implements the [`evento::Aggregator`]
/// trait by generating an `aggregate` method that dispatches events to the appropriate handler
/// methods based on event type.
///
/// # Syntax
///
/// ```ignore
/// #[evento::aggregator]
/// impl AggregateStruct {
///     async fn event_handler_name(&mut self, event: EventDetails<EventType>) -> anyhow::Result<()> {
///         // Update self based on event
///         Ok(())
///     }
/// }
/// ```
///
/// # Requirements
///
/// - The struct must implement all required traits for [`evento::Aggregator`]:
///   - `Default`, `Send`, `Sync`, `Clone`, `Debug`
///   - `bincode::Encode`, `bincode::Decode`
///   - [`evento::AggregatorName`]
/// - Handler methods must be `async`
/// - Handler methods must take `&mut self` and `EventDetails<SomeEventType>`
/// - Handler methods must return `anyhow::Result<()>`
///
/// # Event Matching
///
/// The macro matches events to handler methods by calling [`evento::Event::to_details`] with
/// the event type inferred from the handler method's parameter type.
///
/// # Examples
///
/// ```no_run
/// use evento::{EventDetails, AggregatorName};
/// use serde::{Deserialize, Serialize};
/// use bincode::{Encode, Decode};
///
/// #[derive(AggregatorName, Encode, Decode)]
/// struct UserCreated {
///     name: String,
///     email: String,
/// }
///
/// #[derive(AggregatorName, Encode, Decode)]
/// struct UserEmailChanged {
///     email: String,
/// }
///
/// #[derive(Default, Serialize, Deserialize, Encode, Decode, Clone, Debug)]
/// struct User {
///     name: String,
///     email: String,
///     version: u32,
/// }
///
/// #[evento::aggregator]
/// impl User {
///     async fn user_created(&mut self, event: EventDetails<UserCreated>) -> anyhow::Result<()> {
///         self.name = event.data.name;
///         self.email = event.data.email;
///         self.version = event.version as u32;
///         Ok(())
///     }
///
///     async fn user_email_changed(&mut self, event: EventDetails<UserEmailChanged>) -> anyhow::Result<()> {
///         self.email = event.data.email;
///         self.version = event.version as u32;
///         Ok(())
///     }
/// }
/// ```
///
/// # Generated Code
///
/// The macro generates:
/// - Implementation of [`evento::Aggregator::aggregate`] with event dispatching
/// - Implementation of [`evento::Aggregator::revision`] based on a hash of the handler methods
/// - Implementation of [`evento::AggregatorName`] using the package name and struct name
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
                static NAME: std::sync::LazyLock<String> = std::sync::LazyLock::new(||{
                    format!("{}/{}", env!("CARGO_PKG_NAME"), #name)
                });

                &NAME
            }
        }
    }
    .into()
}

/// Derives the [`evento::AggregatorName`] trait for event types
///
/// The `#[derive(AggregatorName)]` macro automatically implements the [`evento::AggregatorName`]
/// trait, which provides a name identifier for the type. This is essential for event types
/// as it allows the event system to identify and match events by name.
///
/// # Usage
///
/// Apply this derive macro to structs that represent events:
///
/// ```no_run
/// use evento::AggregatorName;
/// use bincode::{Encode, Decode};
///
/// #[derive(AggregatorName, Encode, Decode)]
/// struct UserCreated {
///     name: String,
///     email: String,
/// }
///
/// // The derived implementation returns the struct name as a string
/// assert_eq!(UserCreated::name(), "UserCreated");
/// ```
///
/// # Requirements
///
/// Event types using this derive must also implement:
/// - `bincode::Encode` and `bincode::Decode` for serialization
/// - Usually derived together: `#[derive(AggregatorName, Encode, Decode)]`
///
/// # Generated Implementation
///
/// The macro generates a simple implementation that returns the struct name as a static string:
///
/// ```ignore
/// impl evento::AggregatorName for YourEventType {
///     fn name() -> &'static str {
///         "YourEventType"
///     }
/// }
/// ```
///
/// # Event Identification
///
/// The name returned by this trait is used by the event system to:
/// - Match events to their types during deserialization
/// - Route events to appropriate handlers
/// - Filter events in subscriptions
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
