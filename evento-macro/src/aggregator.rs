use proc_macro::TokenStream;
use quote::{format_ident, quote, quote_spanned, ToTokens};
use syn::{
    punctuated::Punctuated, Attribute, Error, Expr, ExprLit, Fields, Ident, ItemEnum, Lit, LitStr,
    Meta, Token, Variant, Visibility,
};

/// Parsed arguments for `#[evento::aggregate(name = "...", Derive1, Derive2)]`.
struct AggregateArgs {
    /// Explicit aggregate type override; defaults to `"{pkg}/{Enum}"`.
    name: Option<LitStr>,
    derives: Vec<Meta>,
}

fn parse_args(attr: TokenStream) -> syn::Result<AggregateArgs> {
    let mut args = AggregateArgs {
        name: None,
        derives: vec![],
    };

    if attr.is_empty() {
        return Ok(args);
    }

    let parser = Punctuated::<Meta, Token![,]>::parse_terminated;
    let parsed = syn::parse::Parser::parse(parser, attr)?;

    for meta in parsed {
        match meta {
            Meta::NameValue(nv) if nv.path.is_ident("name") => {
                let Expr::Lit(ExprLit {
                    lit: Lit::Str(lit), ..
                }) = &nv.value
                else {
                    return Err(Error::new_spanned(
                        &nv.value,
                        "expected a string literal: `name = \"myapp/MyAggregate\"`",
                    ));
                };
                if lit.value().is_empty() {
                    return Err(Error::new_spanned(lit, "aggregate name must not be empty"));
                }
                if args.name.is_some() {
                    return Err(Error::new_spanned(&nv.path, "duplicate `name` option"));
                }
                args.name = Some(lit.clone());
            }
            Meta::NameValue(nv) => {
                return Err(Error::new_spanned(
                    &nv.path,
                    "unknown option; expected `name = \"...\"` or derive paths like `serde::Serialize`",
                ));
            }
            meta => args.derives.push(meta),
        }
    }

    Ok(args)
}

/// What `#[evento(...)]` on a variant says, plus the attributes to re-emit on
/// the generated struct.
struct VariantInfo<'a> {
    variant: &'a Variant,
    /// Stored event name: `#[evento(name = "...")]`, else the variant ident.
    event_name: String,
    /// `#[evento(upcast_to = <Variant>)]`: the newer event this one converts to.
    upcast_to: Option<Ident>,
    attrs: Vec<&'a Attribute>,
}

/// Extracts the `#[evento(name = "...", upcast_to = <Variant>)]` options from a
/// variant's attributes and keeps the remaining attributes to re-emit.
fn parse_variant(variant: &Variant) -> syn::Result<VariantInfo<'_>> {
    let mut event_name: Option<LitStr> = None;
    let mut upcast_to: Option<Ident> = None;
    let mut attrs = vec![];

    for attr in &variant.attrs {
        if !attr.path().is_ident("evento") {
            attrs.push(attr);
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                let lit: LitStr = meta.value()?.parse()?;
                if lit.value().is_empty() {
                    return Err(Error::new_spanned(lit, "event name must not be empty"));
                }
                if event_name.is_some() {
                    return Err(meta.error("duplicate `name` option"));
                }
                event_name = Some(lit);
                return Ok(());
            }

            if meta.path.is_ident("upcast_to") {
                let target: Ident = meta.value()?.parse().map_err(|e| {
                    Error::new(
                        e.span(),
                        "expected a variant of this enum: `upcast_to = MyEventV2`",
                    )
                })?;
                if upcast_to.is_some() {
                    return Err(meta.error("duplicate `upcast_to` option"));
                }
                upcast_to = Some(target);
                return Ok(());
            }

            Err(meta.error("unknown option; expected `name = \"...\"` or `upcast_to = <Variant>`"))
        })?;
    }

    Ok(VariantInfo {
        variant,
        event_name: event_name
            .map(|lit| lit.value())
            .unwrap_or_else(|| variant.ident.to_string()),
        upcast_to,
        attrs,
    })
}

/// Checks the `upcast_to` graph: every target is a sibling variant, and no
/// chain loops. Each variant has at most one out-edge, so a walk suffices.
fn check_upcasts(variants: &[VariantInfo]) -> syn::Result<()> {
    for (i, info) in variants.iter().enumerate() {
        if let Some(other) = variants[..i]
            .iter()
            .find(|other| other.event_name == info.event_name)
        {
            return Err(Error::new_spanned(
                &info.variant.ident,
                format!(
                    "event name `{}` is already used by variant `{}`",
                    info.event_name, other.variant.ident
                ),
            ));
        }
    }

    let target_of = |ident: &Ident| {
        variants
            .iter()
            .find(|info| &info.variant.ident == ident)
            .and_then(|info| info.upcast_to.as_ref())
    };

    for info in variants {
        let Some(target) = &info.upcast_to else {
            continue;
        };
        let start = &info.variant.ident;

        if target == start {
            return Err(Error::new_spanned(
                target,
                "an event cannot upcast to itself",
            ));
        }
        if !variants.iter().any(|other| &other.variant.ident == target) {
            return Err(Error::new_spanned(
                target,
                format!("no variant named `{target}` in this enum; `upcast_to` must name a sibling variant"),
            ));
        }

        let mut path = vec![start.to_string(), target.to_string()];
        let mut current = target;
        while let Some(next) = target_of(current) {
            path.push(next.to_string());
            if next == start {
                return Err(Error::new_spanned(
                    target,
                    format!("`upcast_to` cycle: {}", path.join(" -> ")),
                ));
            }
            // A loop that does not include `start` is reported on its own
            // variants; stop here rather than walking it for ever.
            if path.len() > variants.len() + 1 {
                break;
            }
            current = next;
        }
    }

    Ok(())
}

/// The `upcasters()` override for `target`: one [`Upcaster`] per older variant
/// whose `upcast_to` chain reaches it, the chain folded into a single typed
/// function (one decode, one encode, whatever its length).
fn upcasters_impl(target: &VariantInfo, variants: &[VariantInfo]) -> syn::Result<impl ToTokens> {
    let mut fns = vec![];
    let mut entries = vec![];

    for source in variants {
        // Follow `source`'s chain; keep it if it reaches `target`.
        let mut steps: Vec<(&Ident, &Ident)> = vec![];
        let mut current = source;
        let reached = loop {
            let Some(next) = &current.upcast_to else {
                break false;
            };
            steps.push((&current.variant.ident, next));
            if next == &target.variant.ident {
                break true;
            }
            current = variants
                .iter()
                .find(|info| &info.variant.ident == next)
                .expect("checked by check_upcasts");
        };
        if !reached {
            continue;
        }

        let hops = u8::try_from(steps.len()).map_err(|_| {
            Error::new_spanned(&source.variant.ident, "`upcast_to` chain is too long")
        })?;
        let fn_name = format_ident!("__upcast_{}", fns.len());
        let source_ident = &source.variant.ident;
        // Spanned on the `upcast_to = New` that asks for the conversion, so a
        // missing `From` impl is reported there.
        let conversions = steps.iter().map(|(old, new)| {
            quote_spanned! {new.span()=>
                let event: #new = <#new as evento::UpcastFrom<#old>>::upcast_from(event);
            }
        });

        fns.push(quote! {
            fn #fn_name(data: &[u8]) -> ::core::result::Result<Vec<u8>, bitcode::Error> {
                let event: #source_ident = bitcode::decode(data)?;
                #(#conversions)*
                Ok(bitcode::encode(&event))
            }
        });
        let from = &source.event_name;
        entries.push(quote! { evento::Upcaster::new(#from, #hops, #fn_name) });
    }

    if entries.is_empty() {
        return Ok(quote! {});
    }

    Ok(quote! {
        fn upcasters() -> &'static [evento::Upcaster] {
            #(#fns)*

            const UPCASTERS: &[evento::Upcaster] = &[#(#entries),*];
            UPCASTERS
        }
    })
}

/// Event struct fields are always emitted `pub`; bare `pub` or inherited
/// visibility on the enum variant's fields is accepted, anything else is an
/// error.
fn check_field_vis(fields: &Fields) -> syn::Result<()> {
    for field in fields {
        if let Visibility::Restricted(vis) = &field.vis {
            return Err(Error::new_spanned(
                vis,
                "event struct fields are always public; remove the visibility qualifier",
            ));
        }
    }
    Ok(())
}

pub fn aggregator(attr: TokenStream, item: TokenStream) -> syn::Result<TokenStream> {
    let input: ItemEnum = syn::parse(item)?;

    let enum_name = &input.ident;
    let enum_name_str = enum_name.to_string();
    let vis = &input.vis;

    let args = parse_args(attr)?;
    let user_derives = &args.derives;

    let aggregate_type_body = match &args.name {
        Some(name) => quote! { #name },
        None => quote! {
            static NAME: std::sync::LazyLock<String> = std::sync::LazyLock::new(||{
                format!("{}/{}", env!("CARGO_PKG_NAME"), #enum_name_str)
            });

            &NAME
        },
    };

    let variants = input
        .variants
        .iter()
        .map(parse_variant)
        .collect::<syn::Result<Vec<_>>>()?;
    check_upcasts(&variants)?;

    // The events enum is a sibling of the per-variant structs, so a variant of
    // that exact name would collide with it.
    let events_enum_name = format_ident!("{}Event", enum_name);
    if let Some(clash) = variants
        .iter()
        .find(|info| info.variant.ident == events_enum_name)
    {
        return Err(Error::new_spanned(
            &clash.variant.ident,
            format!(
                "variant `{events_enum_name}` collides with the events enum generated for this aggregate; rename the variant"
            ),
        ));
    }

    // Generate a struct for each variant
    let structs = variants
        .iter()
        .map(|info| {
            let variant = info.variant;
            let variant_name = &variant.ident;
            let attrs = &info.attrs;
            let event_name_str = &info.event_name;
            let upcasters = upcasters_impl(info, &variants)?;
            check_field_vis(&variant.fields)?;

            // Mandatory + user derives
            let derives = if user_derives.is_empty() {
                quote! { #[derive(Debug, Clone, PartialEq, Default, bitcode::Encode, bitcode::Decode)] }
            } else {
                quote! { #[derive(Debug, Clone, PartialEq, Default, bitcode::Encode, bitcode::Decode, #(#user_derives),*)] }
            };

            let impl_event = quote! {
                impl evento::Aggregate for #variant_name {
                    fn aggregate_type() -> &'static str {
                        #aggregate_type_body
                    }
                }

                impl evento::AggregateEvent for #variant_name {
                    fn event_name() -> &'static str {
                        #event_name_str
                    }

                    #upcasters
                }
            };

            let tokens = match &variant.fields {
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
            };

            Ok(tokens)
        })
        .collect::<syn::Result<Vec<_>>>()?;

    // The events enum: one newtype variant per event, so callers can match a
    // stored event exhaustively instead of laddering over `event.name`.
    let variant_idents = variants
        .iter()
        .map(|info| &info.variant.ident)
        .collect::<Vec<_>>();
    let event_names = variants
        .iter()
        .map(|info| &info.event_name)
        .collect::<Vec<_>>();

    // The enum is not the stored wire format, so it gets neither the bitcode
    // derives nor `Default`; user-supplied derives still apply, which is what
    // makes `#[evento::aggregate(serde::Serialize)]` serialize the whole enum.
    let enum_derives = if user_derives.is_empty() {
        quote! { #[derive(Debug, Clone, PartialEq)] }
    } else {
        quote! { #[derive(Debug, Clone, PartialEq, #(#user_derives),*)] }
    };

    let enum_doc = format!(
        "Every event of the [`{enum_name}`] aggregate, reconstructed from a stored event.\n\n\
         Generated by `#[evento::aggregate]`. Build one with \
         `{events_enum_name}::try_from(&event)` to match a stored \
         [`evento::Event`](evento::Event) exhaustively. Events are decoded exactly as \
         stored: `#[evento(upcast_to = ...)]` is not applied."
    );
    let variant_docs = variants
        .iter()
        .map(|info| {
            let ident = &info.variant.ident;
            let name = &info.event_name;
            format!("The [`{ident}`] event, stored as `{name}`.")
        })
        .collect::<Vec<_>>();

    // An aggregate with no variants generates an uninhabited enum: `match self`
    // on a reference to it is rejected, and a lone wildcard arm trips
    // `clippy::match_single_binding`, so both bodies degenerate explicitly.
    let unknown_event = quote! {
        Err(evento::FromEventError::UnknownEvent {
            aggregate_type: expected,
            name: event.name.clone(),
        })
    };
    let (event_name_body, try_from_body) = if variants.is_empty() {
        (quote! { match *self {} }, unknown_event)
    } else {
        (
            quote! {
                match self {
                    #( Self::#variant_idents(_) => #event_names, )*
                }
            },
            quote! {
                match event.name.as_str() {
                    #(
                        #event_names => bitcode::decode::<#variant_idents>(&event.data)
                            .map(Self::#variant_idents)
                            .map_err(|source| evento::FromEventError::Decode {
                                name: event.name.clone(),
                                source,
                            }),
                    )*
                    _ => #unknown_event,
                }
            },
        )
    };

    // Unit marker struct for the aggregate itself (e.g. `Projection::new::<BankAccount>()`)
    Ok(quote! {
        #(#structs)*

        #[derive(Default)]
        #vis struct #enum_name;

        impl evento::Aggregate for #enum_name {
            fn aggregate_type() -> &'static str {
                #aggregate_type_body
            }
        }

        #[doc = #enum_doc]
        #enum_derives
        #vis enum #events_enum_name {
            #(
                #[doc = #variant_docs]
                #variant_idents(#variant_idents),
            )*
        }

        impl #events_enum_name {
            /// The stored event name of the event held by this variant.
            pub fn event_name(&self) -> &'static str {
                #event_name_body
            }
        }

        impl ::core::convert::TryFrom<&evento::Event> for #events_enum_name {
            type Error = evento::FromEventError;

            fn try_from(event: &evento::Event) -> ::core::result::Result<Self, Self::Error> {
                let expected = <#enum_name as evento::Aggregate>::aggregate_type();
                if event.aggregate_type != expected {
                    return Err(evento::FromEventError::AggregateMismatch {
                        expected,
                        got: event.aggregate_type.clone(),
                    });
                }

                #try_from_body
            }
        }

        impl evento::AggregateEvents for #enum_name {
            type Events = #events_enum_name;
        }

        #(
            impl ::core::convert::From<#variant_idents> for #events_enum_name {
                fn from(event: #variant_idents) -> Self {
                    Self::#variant_idents(event)
                }
            }
        )*
    }
    .into())
}
