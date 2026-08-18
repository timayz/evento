use proc_macro::TokenStream;
use quote::quote;
use syn::{
    punctuated::Punctuated, Attribute, Error, Expr, ExprLit, Fields, ItemEnum, Lit, LitStr, Meta,
    Token, Variant, Visibility,
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

/// Extracts an optional `#[evento(name = "...")]` event-name override from a
/// variant's attributes and returns the remaining attributes to re-emit.
fn parse_variant_attrs(variant: &Variant) -> syn::Result<(Option<LitStr>, Vec<&Attribute>)> {
    let mut event_name: Option<LitStr> = None;
    let mut rest = vec![];

    for attr in &variant.attrs {
        if !attr.path().is_ident("evento") {
            rest.push(attr);
            continue;
        }

        attr.parse_nested_meta(|meta| {
            if !meta.path.is_ident("name") {
                return Err(meta.error("unknown option; expected `name = \"...\"`"));
            }
            let lit: LitStr = meta.value()?.parse()?;
            if lit.value().is_empty() {
                return Err(Error::new_spanned(lit, "event name must not be empty"));
            }
            if event_name.is_some() {
                return Err(meta.error("duplicate `name` option"));
            }
            event_name = Some(lit);
            Ok(())
        })?;
    }

    Ok((event_name, rest))
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

    // Generate a struct for each variant
    let structs = input
        .variants
        .iter()
        .map(|variant| {
            let variant_name = &variant.ident;
            let (event_name, attrs) = parse_variant_attrs(variant)?;
            let event_name_str = event_name
                .map(|lit| lit.value())
                .unwrap_or_else(|| variant_name.to_string());
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
    }
    .into())
}
