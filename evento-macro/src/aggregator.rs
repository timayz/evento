use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, punctuated::Punctuated, Fields, ItemEnum, Meta, Token};

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
            quote! { #[derive(Debug, Clone, PartialEq, Default, bitcode::Encode, bitcode::Decode)] }
        } else {
            quote! { #[derive(Debug, Clone, PartialEq, Default, bitcode::Encode, bitcode::Decode, #(#user_derives),*)] }
        };

        let impl_event = quote! {
            impl evento::Aggregator for #variant_name {
                fn aggregator_type() -> &'static str {
                    static NAME: std::sync::LazyLock<String> = std::sync::LazyLock::new(||{
                        format!("{}/{}", env!("CARGO_PKG_NAME"), #enum_name_str)
                    });

                    &NAME
                }
            }

            impl evento::AggregatorEvent for #variant_name {
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

        impl evento::Aggregator for #enum_name {
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
