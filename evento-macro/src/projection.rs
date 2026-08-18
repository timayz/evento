use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    Data, DeriveInput, Error, Fields, Ident, Path, Result, Token, Type,
};

/// Parsed arguments for
/// `#[evento::projection(cursor = <Type>, id = <field>, Derive1, Derive2, ...)]`
struct ProjectionCursorArgs {
    /// Cursor field type; defaults to `String`.
    cursor: Option<Type>,
    /// Field holding the aggregate id; when set, `ProjectionAggregate` is
    /// implemented too.
    id: Option<Ident>,
    derives: Vec<Path>,
}

impl Parse for ProjectionCursorArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        let mut args = Self {
            cursor: None,
            id: None,
            derives: vec![],
        };

        while !input.is_empty() {
            if input.peek(Ident) && input.peek2(Token![=]) {
                let key: Ident = input.parse()?;
                input.parse::<Token![=]>()?;
                match key.to_string().as_str() {
                    "cursor" if args.cursor.is_none() => args.cursor = Some(input.parse()?),
                    "id" if args.id.is_none() => args.id = Some(input.parse()?),
                    "cursor" | "id" => {
                        return Err(Error::new(key.span(), format!("duplicate `{key}` option")));
                    }
                    _ => {
                        return Err(Error::new(
                            key.span(),
                            "unknown option; expected `cursor = <Type>`, `id = <field>`, or derive paths",
                        ));
                    }
                }
            } else {
                args.derives.push(input.parse()?);
            }

            if input.is_empty() {
                break;
            }
            input.parse::<Token![,]>()?;
        }

        Ok(args)
    }
}

/// Implementation for the projection_cursor attribute macro
pub fn projection_cursor_impl(attr: TokenStream, input: &DeriveInput) -> Result<TokenStream> {
    let args: ProjectionCursorArgs = syn::parse2(attr.into())?;

    let struct_name = &input.ident;
    let vis = &input.vis;
    let generics = &input.generics;

    // Filter out #[derive(...)] from existing attrs - we'll regenerate it
    let other_attrs: Vec<_> = input
        .attrs
        .iter()
        .filter(|a| !a.path().is_ident("derive"))
        .collect();

    // Collect existing derives from the original struct
    let mut existing_derives: Vec<Path> = vec![];
    for attr in &input.attrs {
        if attr.path().is_ident("derive") {
            attr.parse_nested_meta(|meta| {
                existing_derives.push(meta.path);
                Ok(())
            })?;
        }
    }

    // Combine: Default + Clone + existing derives + custom derives from attr
    let custom_derives = &args.derives;

    // Extract existing fields
    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            Fields::Unnamed(_) => {
                return Err(syn::Error::new_spanned(
                    struct_name,
                    "projection_cursor only supports structs with named fields",
                ));
            }
            Fields::Unit => {
                return Err(syn::Error::new_spanned(
                    struct_name,
                    "projection_cursor does not support unit structs",
                ));
            }
        },
        Data::Enum(_) => {
            return Err(syn::Error::new_spanned(
                struct_name,
                "projection_cursor does not support enums",
            ));
        }
        Data::Union(_) => {
            return Err(syn::Error::new_spanned(
                struct_name,
                "projection_cursor does not support unions",
            ));
        }
    };

    if let Some(id) = &args.id {
        if !fields.iter().any(|f| f.ident.as_ref() == Some(id)) {
            let available = fields
                .iter()
                .filter_map(|f| f.ident.as_ref().map(|i| format!("`{i}`")))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(Error::new(
                id.span(),
                format!("no field named `{id}`; available fields: {available}"),
            ));
        }
    }

    let existing_fields: Vec<_> = fields
        .iter()
        .map(|f| {
            let field_attrs = &f.attrs;
            let field_vis = &f.vis;
            let field_name = &f.ident;
            let field_ty = &f.ty;
            quote! {
                #(#field_attrs)*
                #field_vis #field_name: #field_ty
            }
        })
        .collect();
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    // Default `String` cursor keeps its historical codegen (`cursor::Value` has
    // no `From<Value> for String`); custom types go through
    // `From<cursor::Value>` / `Into<cursor::Value>` + `Clone`.
    let (cursor_ty, cursor_impl) = match &args.cursor {
        None => (
            quote! { String },
            quote! {
                fn set_cursor(&mut self, v: &::evento::cursor::Value) {
                    self.cursor = v.to_string();
                }

                fn get_cursor(&self) -> ::evento::cursor::Value {
                    self.cursor.to_owned().into()
                }
            },
        ),
        Some(ty) => (
            quote! { #ty },
            quote! {
                fn set_cursor(&mut self, v: &::evento::cursor::Value) {
                    self.cursor = ::core::convert::Into::into(::core::clone::Clone::clone(v));
                }

                fn get_cursor(&self) -> ::evento::cursor::Value {
                    ::core::convert::Into::into(::core::clone::Clone::clone(&self.cursor))
                }
            },
        ),
    };

    let projection_aggregate = args.id.as_ref().map(|id| {
        quote! {
            impl #impl_generics ::evento::projection::ProjectionAggregate for #struct_name #ty_generics #where_clause {
                fn aggregate_id(&self) -> String {
                    self.#id.to_string()
                }
            }
        }
    });

    Ok(quote! {
        #[derive(Default, Clone, #(#existing_derives,)* #(#custom_derives),*)]
        #(#other_attrs)*
        #vis struct #struct_name #generics {
            #(#existing_fields,)*
            pub cursor: #cursor_ty,
            pub aggregate_version: u16,
        }

        impl #impl_generics ::evento::ProjectionCursor for #struct_name #ty_generics #where_clause {
            #cursor_impl

            fn set_aggregate_version(&mut self, v: u16) {
                self.aggregate_version = v;
            }

            fn get_aggregate_version(&self) -> u16 {
                self.aggregate_version
            }
        }

        #projection_aggregate
    }
    .into())
}
