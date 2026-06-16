use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    punctuated::Punctuated,
    Data, DeriveInput, Fields, Path, Result, Token,
};

/// Parsed arguments for #[projection_cursor(Derive1, Derive2, ...)]
struct ProjectionCursorArgs {
    derives: Vec<Path>,
}

impl Parse for ProjectionCursorArgs {
    fn parse(input: ParseStream) -> Result<Self> {
        if input.is_empty() {
            return Ok(Self { derives: vec![] });
        }

        let derives = Punctuated::<Path, Token![,]>::parse_terminated(input)?;
        Ok(Self {
            derives: derives.into_iter().collect(),
        })
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

    Ok(quote! {
        #[derive(Default, Clone, #(#existing_derives,)* #(#custom_derives),*)]
        #(#other_attrs)*
        #vis struct #struct_name #generics {
            #(#existing_fields,)*
            pub cursor: String,
        }

        impl #impl_generics ::evento::ProjectionCursor for #struct_name #ty_generics #where_clause {
            fn set_cursor(&mut self, v: &::evento::cursor::Value) {
                self.cursor = v.to_string();
            }

            fn get_cursor(&self) -> ::evento::cursor::Value {
                self.cursor.to_owned().into()
            }
        }
    }
    .into())
}
