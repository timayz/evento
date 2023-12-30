#![forbid(unsafe_code)]

extern crate proc_macro;
extern crate proc_macro2;

#[macro_use]
extern crate proc_macro_error;

use convert_case::{Case, Casing};
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use sha3::{Digest, Sha3_256};
use syn::{parse_macro_input, DeriveInput, FieldsNamed};

#[proc_macro_error]
#[proc_macro_derive(Aggregate)]
pub fn aggregate_derive(input: TokenStream) -> TokenStream {
    let DeriveInput { ident, data, .. } = parse_macro_input!(input);

    let syn::Data::Struct(s) = data else {
        abort!(ident, "Derive Aggregate only available on struct");
    };

    let mut hasher = Sha3_256::new();
    hasher.update(ident.to_string());

    if let syn::Fields::Named(FieldsNamed { named, .. }) = s.fields {
        for field in named {
            if let Some(ident) = field.ident {
                hasher.update(ident.to_string());
            }

            if let syn::Type::Path(ty_path) = field.ty {
                let idents_of_path =
                    ty_path
                        .path
                        .segments
                        .iter()
                        .fold(String::new(), |mut acc, v| {
                            acc.push_str(&v.ident.to_string());
                            acc
                        });

                hasher.update(idents_of_path);
            }
        }
    };

    let name = ident.to_string().to_case(Case::Kebab).to_string();
    let version = format!("{:x}", hasher.finalize());

    quote! {
        impl Aggregate for #ident {
            fn aggregate_type() -> &'static str {
                #name
            }

            fn aggregate_version() -> &'static str {
                #version
            }
        }
    }
    .into()
}

#[proc_macro_error]
#[proc_macro_derive(PublisherEvent)]
pub fn publisher_event_derive(input: TokenStream) -> TokenStream {
    let DeriveInput { ident, data, .. } = parse_macro_input!(input);

    let syn::Data::Enum(s) = data else {
        abort!(ident, "Derive PublisherEvent only available on Enum");
    };

    let mut variants = TokenStream2::default();

    for variant in s.variants {
        let v_ident = format_ident!("{}", variant.ident);
        variants.extend::<TokenStream2>(
            quote! {
                impl PublisherEvent for #v_ident {
                    type Output = #ident;

                    fn event_name() -> Self::Output {
                        #ident::#v_ident
                    }
                }
            },
        )
    }

    quote! {
        impl From<#ident> for String {
            fn from(e: #ident) -> Self {
                e.to_string()
            }
        }

        #variants
    }
    .into()
}
