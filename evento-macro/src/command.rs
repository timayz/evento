use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::DeriveInput;

pub fn command_impl(input: DeriveInput, debug: bool) -> syn::Result<TokenStream> {
    let vis = &input.vis;
    let struct_name = &input.ident;
    let data_struct_name = format_ident!("{}Data", struct_name);

    let fields = match &input.data {
        syn::Data::Struct(data) => match &data.fields {
            syn::Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    &input,
                    "my_macro only supports structs with named fields",
                ))
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &input,
                "my_macro only supports structs",
            ))
        }
    };

    let field_defs: Vec<_> = fields.iter().collect();
    let other_attrs: Vec<_> = input.attrs.iter().collect();

    let output = quote! {
        #[derive(Default, Clone)]
        #(#other_attrs)*
        #vis struct #data_struct_name {
            #(#field_defs),*
        }

        #vis struct #struct_name<'a, E: ::evento::Executor> {
            data: #data_struct_name,
            aggregator_id: String,
            executor: &'a E,
            pub event_version: u16,
            pub event_routing_key: Option<String>,
        }

        impl<'a, E: ::evento::Executor> ::core::ops::Deref for #struct_name<'a, E> {
            type Target = #data_struct_name;

            fn deref(&self) -> &Self::Target {
                &self.data
            }
        }

        impl<'a, E: ::evento::Executor> ::core::ops::DerefMut for #struct_name<'a, E> {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.data
            }
        }

        impl<'a, E: ::evento::Executor> #struct_name<'a, E> {
            pub fn new(aggregator_id: impl Into<String>, loaded: ::evento::LoadResult<#data_struct_name>, executor: &'a E) -> Self {
                let aggregator_id = aggregator_id.into();

                Self {
                    aggregator_id,
                    data: loaded.item,
                    event_version: loaded.version,
                    event_routing_key: loaded.routing_key,
                    executor,
                }
            }

            fn aggregator(&self) -> ::evento::AggregatorBuilder {
                evento::aggregator(&self.aggregator_id)
                    .original_version(self.event_version)
                    .routing_key_opt(self.event_routing_key.to_owned())
            }
        }
    };

    if !debug {
        return Ok(output.into());
    }

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let debug_path =
        std::path::PathBuf::from(&manifest_dir).join("../target/evento_debug_command_macro.rs"); // adjust ../ as needed

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
