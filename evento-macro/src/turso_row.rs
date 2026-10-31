use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Result};

/// What a field's `#[turso(...)]` options say: the column it reads
/// (`rename = "..."`, else the field's name) and whether it defaults when
/// that column is not selected (`default`).
struct FieldOptions {
    column: Option<String>,
    default: bool,
}

fn field_options(field: &syn::Field) -> Result<FieldOptions> {
    let mut options = FieldOptions {
        column: None,
        default: false,
    };
    for attr in &field.attrs {
        if !attr.path().is_ident("turso") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("default") {
                options.default = true;
                Ok(())
            } else if meta.path.is_ident("rename") {
                let value: syn::LitStr = meta.value()?.parse()?;
                options.column = Some(value.value());
                Ok(())
            } else {
                Err(meta
                    .error("unknown `turso` option; expected `default` or `rename = \"column\"`"))
            }
        })?;
    }
    Ok(options)
}

/// `impl evento::turso::FromRow`: one field per column of the same name (or
/// the one `rename` gives).
pub fn from_row_impl(input: &DeriveInput) -> Result<TokenStream> {
    let struct_name = &input.ident;
    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    struct_name,
                    "FromRow only supports structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                struct_name,
                "FromRow only supports structs",
            ));
        }
    };

    let mut reads = Vec::with_capacity(fields.len());
    for field in fields {
        let name = field.ident.as_ref().expect("named field");
        let options = field_options(field)?;
        let column = options.column.unwrap_or_else(|| name.to_string());
        reads.push(if options.default {
            quote! {
                #name: row.try_get(#column)?.unwrap_or_default()
            }
        } else {
            quote! {
                #name: row.get(#column)?
            }
        });
    }

    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics ::evento::turso::FromRow for #struct_name #ty_generics #where_clause {
            fn from_row(row: &::evento::turso::Row<'_>) -> ::anyhow::Result<Self> {
                Ok(Self {
                    #(#reads,)*
                })
            }
        }
    }
    .into())
}
