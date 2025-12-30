use proc_macro::TokenStream;
use quote::{format_ident, quote};
use std::collections::{HashMap, HashSet};
use syn::{Data, DeriveInput, Fields, Result, Type};

/// Holds parsed cursor field information
#[derive(Clone)]
struct CursorField {
    field_name: syn::Ident,
    field_type: Type,
    column_path: syn::Path,
    order: usize,
}

/// Parse the #[cursor(...)] attribute
/// Supports two formats:
/// - Named: #[cursor(name, Column::Variant, order)]
/// - Unnamed: #[cursor(Column::Variant, order)] - uses "cursor" as default name
///   Returns (cursor_name, column_path, order)
fn parse_cursor_attr(attr: &syn::Attribute) -> Result<Option<(String, syn::Path, usize)>> {
    if !attr.path().is_ident("cursor") {
        return Ok(None);
    }

    let result = attr.parse_args_with(|input: syn::parse::ParseStream| {
        // Try to determine if this is named or unnamed format
        // by looking at the first identifier and what follows

        let first_ident: syn::Ident = input.parse()?;

        if input.peek(syn::Token![::]) {
            // It's a path like `Column::Variant` - unnamed format
            // We need to reconstruct the path starting with first_ident
            let mut segments = syn::punctuated::Punctuated::new();
            segments.push(syn::PathSegment::from(first_ident));

            while input.peek(syn::Token![::]) {
                input.parse::<syn::Token![::]>()?;
                let seg: syn::PathSegment = input.parse()?;
                segments.push(seg);
            }

            let path = syn::Path {
                leading_colon: None,
                segments,
            };

            input.parse::<syn::Token![,]>()?;
            let lit: syn::LitInt = input.parse()?;
            let order: usize = lit.base10_parse()?;

            // Use "cursor" as default name for unnamed format
            Ok(("cursor".to_string(), path, order))
        } else {
            // Named format: name, Column::Variant, order
            let name = first_ident.to_string();

            input.parse::<syn::Token![,]>()?;
            let path: syn::Path = input.parse()?;
            input.parse::<syn::Token![,]>()?;
            let lit: syn::LitInt = input.parse()?;
            let order: usize = lit.base10_parse()?;

            Ok((name, path, order))
        }
    })?;

    Ok(Some(result))
}

/// Generate a short name from a field name (e.g., "created_at" -> "c")
/// Handles duplicates by adding more characters from subsequent parts
fn generate_short_name(field_name: &str, used: &HashSet<String>) -> String {
    let parts: Vec<&str> = field_name.split('_').collect();

    // Try first letter
    let mut short = parts[0].chars().next().unwrap_or('x').to_string();
    if !used.contains(&short) {
        return short;
    }

    // Try adding first letter of subsequent parts
    for part in parts.iter().skip(1) {
        if let Some(c) = part.chars().next() {
            short.push(c);
            if !used.contains(&short) {
                return short;
            }
        }
    }

    // Fallback: add numbers
    let base = parts[0].chars().next().unwrap_or('x').to_string();
    let mut counter = 2;
    loop {
        let candidate = format!("{}{}", base, counter);
        if !used.contains(&candidate) {
            return candidate;
        }
        counter += 1;
    }
}

/// Convert snake_case to PascalCase
fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(c) => c.to_uppercase().chain(chars).collect(),
                None => String::new(),
            }
        })
        .collect()
}

/// Extract the enum type from the column path (e.g., ContactAdmin::Id -> ContactAdmin)
fn extract_enum_type(path: &syn::Path) -> Result<syn::Path> {
    if path.segments.len() < 2 {
        return Err(syn::Error::new_spanned(
            path,
            "Expected path with at least two segments (e.g., Column::Variant)",
        ));
    }

    // Collect all segments except the last one (the variant)
    let segments: syn::punctuated::Punctuated<syn::PathSegment, syn::Token![::]> = path
        .segments
        .iter()
        .take(path.segments.len() - 1)
        .cloned()
        .collect();

    Ok(syn::Path {
        leading_colon: path.leading_colon,
        segments,
    })
}

/// Check if a type is likely a Copy type (primitive types)
fn is_copy_type(ty: &Type) -> bool {
    if let Type::Path(type_path) = ty {
        if let Some(segment) = type_path.path.segments.last() {
            let ident = segment.ident.to_string();
            return matches!(
                ident.as_str(),
                "u8" | "u16"
                    | "u32"
                    | "u64"
                    | "u128"
                    | "usize"
                    | "i8"
                    | "i16"
                    | "i32"
                    | "i64"
                    | "i128"
                    | "isize"
                    | "f32"
                    | "f64"
                    | "bool"
                    | "char"
            );
        }
    }
    false
}

/// Generate code for a single cursor variant
fn generate_cursor_code(
    struct_name: &syn::Ident,
    cursor_name: &str,
    cursor_fields: &mut [CursorField],
) -> Result<TokenStream> {
    // Sort by order (ascending) for the cursor struct
    cursor_fields.sort_by_key(|f| f.order);

    // Generate short names for each field
    let mut used_short_names = HashSet::new();
    let short_names: Vec<syn::Ident> = cursor_fields
        .iter()
        .map(|f| {
            let short = generate_short_name(&f.field_name.to_string(), &used_short_names);
            used_short_names.insert(short.clone());
            format_ident!("{}", short)
        })
        .collect();

    // Extract the column enum type from the first field
    let column_enum_type = extract_enum_type(&cursor_fields[0].column_path)?;

    let field_count = cursor_fields.len();
    // let field_count = Literal::usize_unsuffixed(cursor_fields.len());

    // Generate cursor struct fields with doc comments
    let cursor_struct_fields = cursor_fields.iter().zip(&short_names).map(|(f, short)| {
        let field_type = &f.field_type;
        let doc = format!(" {}", f.field_name);
        quote! {
            #[doc = #doc]
            pub #short: #field_type
        }
    });

    // Sort by order descending for columns() and values() (higher order first)
    let mut sorted_indices: Vec<usize> = (0..cursor_fields.len()).collect();
    sorted_indices.sort_by(|&a, &b| cursor_fields[b].order.cmp(&cursor_fields[a].order));

    // Generate columns (descending order)
    let column_variants = sorted_indices.iter().map(|&i| {
        let variant = cursor_fields[i].column_path.segments.last().unwrap();
        let variant_ident = &variant.ident;
        quote! { #column_enum_type::#variant_ident }
    });

    // Generate values (same descending order)
    let values = sorted_indices.iter().map(|&i| {
        let short = &short_names[i];
        quote! { cursor.#short.into() }
    });

    // Check if this is the default "cursor" name (no wrapper needed)
    if cursor_name == "cursor" {
        let cursor_struct_name = format_ident!("{}Cursor", struct_name);

        // Generate serialize assignments (direct access to self)
        let serialize_assignments = cursor_fields.iter().zip(&short_names).map(|(f, short)| {
            let field_name = &f.field_name;
            if is_copy_type(&f.field_type) {
                quote! { #short: self.#field_name }
            } else {
                quote! { #short: self.#field_name.to_owned() }
            }
        });

        Ok(quote! {
            // Cursor struct
            #[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
            pub struct #cursor_struct_name {
                #(#cursor_struct_fields),*
            }

            impl evento::cursor::Cursor for #struct_name {
                type T = #cursor_struct_name;

                fn serialize(&self) -> Self::T {
                    #cursor_struct_name {
                        #(#serialize_assignments),*
                    }
                }
            }

            impl evento::sql::Bind for #struct_name {
                type T = #column_enum_type;
                type I = [Self::T; #field_count];
                type V = [sea_query::Expr; #field_count];
                type Cursor = Self;

                fn columns() -> Self::I {
                    [#(#column_variants),*]
                }

                fn values(
                    cursor: <<Self as evento::sql::Bind>::Cursor as evento::cursor::Cursor>::T,
                ) -> Self::V {
                    [#(#values),*]
                }
            }
        }
        .into())
    } else {
        // Named cursor - create newtype wrapper
        let wrapper_name = format_ident!("{}{}", struct_name, to_pascal_case(cursor_name));
        let cursor_struct_name =
            format_ident!("{}{}Cursor", struct_name, to_pascal_case(cursor_name));

        // Generate serialize assignments (access through self.0)
        let serialize_assignments: Vec<_> = cursor_fields
            .iter()
            .zip(&short_names)
            .map(|(f, short)| {
                let field_name = &f.field_name;
                if is_copy_type(&f.field_type) {
                    quote! { #short: self.0.#field_name }
                } else {
                    quote! { #short: self.0.#field_name.to_owned() }
                }
            })
            .collect();

        Ok(quote! {
            // Newtype wrapper
            #[derive(Debug, Clone)]
            pub struct #wrapper_name(pub #struct_name);

            impl ::core::ops::Deref for #wrapper_name {
                type Target = #struct_name;

                fn deref(&self) -> &Self::Target {
                    &self.0
                }
            }

            impl<'r, R: sqlx::Row> sqlx::FromRow<'r, R> for #wrapper_name
            where
                #struct_name: ::sqlx::FromRow<'r, R>,
            {
                fn from_row(row: &'r R) -> ::sqlx::Result<Self> {
                    Ok(#wrapper_name(#struct_name::from_row(row)?))
                }
            }

            // Cursor struct
            #[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
            pub struct #cursor_struct_name {
                #(#cursor_struct_fields),*
            }

            impl evento::cursor::Cursor for #wrapper_name {
                type T = #cursor_struct_name;

                fn serialize(&self) -> Self::T {
                    #cursor_struct_name {
                        #(#serialize_assignments),*
                    }
                }
            }

            impl evento::sql::Bind for #wrapper_name {
                type T = #column_enum_type;
                type I = [Self::T; #field_count];
                type V = [sea_query::Expr; #field_count];
                type Cursor = Self;

                fn columns() -> Self::I {
                    [#(#column_variants),*]
                }

                fn values(
                    cursor: <<Self as evento::sql::Bind>::Cursor as evento::cursor::Cursor>::T,
                ) -> Self::V {
                    [#(#values),*]
                }
            }
        }
        .into())
    }
}

/// Main implementation for the Cursor derive macro
pub fn cursor_impl(input: &DeriveInput) -> Result<TokenStream> {
    let struct_name = &input.ident;

    // Extract fields with #[cursor] attribute
    let fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => &fields.named,
            Fields::Unnamed(_) => {
                return Err(syn::Error::new_spanned(
                    struct_name,
                    "Cursor derive only supports structs with named fields",
                ));
            }
            Fields::Unit => {
                return Err(syn::Error::new_spanned(
                    struct_name,
                    "Cursor derive does not support unit structs",
                ));
            }
        },
        Data::Enum(_) => {
            return Err(syn::Error::new_spanned(
                struct_name,
                "Cursor derive does not support enums",
            ));
        }
        Data::Union(_) => {
            return Err(syn::Error::new_spanned(
                struct_name,
                "Cursor derive does not support unions",
            ));
        }
    };

    // Group fields by cursor name
    let mut cursor_groups: HashMap<String, Vec<CursorField>> = HashMap::new();

    for field in fields {
        for attr in &field.attrs {
            if let Some((cursor_name, column_path, order)) = parse_cursor_attr(attr)? {
                cursor_groups
                    .entry(cursor_name)
                    .or_default()
                    .push(CursorField {
                        field_name: field.ident.clone().unwrap(),
                        field_type: field.ty.clone(),
                        column_path,
                        order,
                    });
            }
        }
    }

    if cursor_groups.is_empty() {
        return Err(syn::Error::new_spanned(
            struct_name,
            "No fields marked with #[cursor] attribute. Add #[cursor(name, Column::Variant, order)] to at least one field.",
        ));
    }

    // Generate code for each cursor variant
    let mut all_tokens = TokenStream::new();

    for (cursor_name, mut fields) in cursor_groups {
        let tokens = generate_cursor_code(struct_name, &cursor_name, &mut fields)?;
        all_tokens.extend(tokens);
    }

    Ok(all_tokens)
}
