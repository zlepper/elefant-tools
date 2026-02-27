use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Lit, Meta};

/// Derive macro for PostgreSQL enum types.
///
/// Generates implementations for `PostgresEnum`, `FromSqlBase`, `FromSqlBinary`,
/// `FromSqlText`, and `ToSql`.
///
/// # Attributes
///
/// - `#[postgres(name = "...")]` on the enum: sets the PostgreSQL type name.
///   Defaults to the snake_case of the Rust enum name.
/// - `#[postgres(label = "...")]` on a variant: sets the PostgreSQL label.
///   Defaults to the snake_case of the variant name.
///
/// # Example
///
/// ```rust,ignore
/// #[derive(PostgresEnum)]
/// #[postgres(name = "mood")]
/// enum Mood {
///     Happy,
///     #[postgres(label = "very_sad")]
///     Sad,
///     Neutral,
/// }
/// ```
#[proc_macro_derive(PostgresEnum, attributes(postgres))]
pub fn derive_postgres_enum(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match impl_postgres_enum(&input) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn impl_postgres_enum(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;

    let variants = match &input.data {
        Data::Enum(data) => &data.variants,
        _ => {
            return Err(syn::Error::new_spanned(
                input,
                "PostgresEnum can only be derived for enums",
            ))
        }
    };

    // Validate all variants are unit variants
    for variant in variants {
        if !matches!(variant.fields, Fields::Unit) {
            return Err(syn::Error::new_spanned(
                variant,
                "PostgresEnum only supports unit variants (no fields)",
            ));
        }
    }

    // Get the PostgreSQL type name
    let pg_type_name =
        get_pg_attr(&input.attrs, "name")?.unwrap_or_else(|| to_snake_case(&name.to_string()));

    // Build variant info: (ident, pg_label)
    let variant_info: Vec<(&syn::Ident, String)> = variants
        .iter()
        .map(|v| {
            let label = get_pg_attr(&v.attrs, "label")?
                .unwrap_or_else(|| to_snake_case(&v.ident.to_string()));
            Ok((&v.ident, label))
        })
        .collect::<syn::Result<Vec<_>>>()?;

    let variant_idents: Vec<_> = variant_info.iter().map(|(ident, _)| ident).collect();
    let variant_labels: Vec<_> = variant_info
        .iter()
        .map(|(_, label)| label.as_str())
        .collect();

    // Build match arms for to_label
    let to_label_arms = variant_idents
        .iter()
        .zip(variant_labels.iter())
        .map(|(ident, label)| {
            quote! { #name::#ident => #label }
        });

    // Build match arms for from_label
    let from_label_arms = variant_idents
        .iter()
        .zip(variant_labels.iter())
        .map(|(ident, label)| {
            quote! { #label => Ok(#name::#ident) }
        });

    // Build display list for error message
    let labels_display = variant_labels.join(", ");
    let from_label_error = format!(
        "Unknown label for PostgreSQL enum '{}': {{}}. Valid labels: {}",
        pg_type_name, labels_display
    );

    let found_crate = proc_macro_crate::crate_name("elefant-client")
        .expect("elefant-client is present in Cargo.toml");
    let crate_path = match found_crate {
        proc_macro_crate::FoundCrate::Itself => quote!(crate),
        proc_macro_crate::FoundCrate::Name(name) => {
            let ident = syn::Ident::new(&name, Span::call_site());
            quote!(#ident)
        }
    };

    Ok(quote! {
        impl #crate_path::PostgresEnum for #name {
            const PG_TYPE_NAME: &'static str = #pg_type_name;

            fn to_label(&self) -> &'static str {
                match self {
                    #(#to_label_arms,)*
                }
            }

            fn from_label(label: &str) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                match label {
                    #(#from_label_arms,)*
                    other => Err(format!(#from_label_error, other).into()),
                }
            }
        }

        impl<'a> #crate_path::FromSqlBase<'a> for #name {
            fn accepts_postgres_type(_oid: i32) -> bool {
                false // No static OID for enums
            }

            fn accepts_with_registry(
                field: &#crate_path::FieldDescription,
                registry: &#crate_path::EnumTypeRegistry,
            ) -> bool {
                registry.has_oid_for_type(#pg_type_name, field.data_type_oid)
            }
        }

        impl<'a> #crate_path::FromSqlBinary<'a> for #name {
            fn from_sql_binary(
                raw: &'a [u8],
                _field: &#crate_path::FieldDescription,
            ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                let label = std::str::from_utf8(raw)?;
                <Self as #crate_path::PostgresEnum>::from_label(label)
            }
        }

        impl<'a> #crate_path::FromSqlText<'a> for #name {
            fn from_sql_text(
                raw: &'a str,
                _field: &#crate_path::FieldDescription,
            ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
                <Self as #crate_path::PostgresEnum>::from_label(raw)
            }
        }

        impl #crate_path::ToSql for #name {
            fn to_sql_binary(
                &self,
                target_buffer: &mut Vec<u8>,
            ) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
                let label = <Self as #crate_path::PostgresEnum>::to_label(self);
                target_buffer.extend_from_slice(label.as_bytes());
                Ok(())
            }
        }
    })
}

/// Extract `#[postgres(key = "...")]` from attributes, where `key` is e.g. `"name"` or `"label"`.
fn get_pg_attr(attrs: &[syn::Attribute], key: &str) -> syn::Result<Option<String>> {
    for attr in attrs {
        if !attr.path().is_ident("postgres") {
            continue;
        }
        let nested = attr.parse_args_with(
            syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
        )?;
        for meta in nested {
            if let Meta::NameValue(nv) = meta {
                if nv.path.is_ident(key) {
                    if let syn::Expr::Lit(expr_lit) = &nv.value {
                        if let Lit::Str(s) = &expr_lit.lit {
                            return Ok(Some(s.value()));
                        }
                    }
                    return Err(syn::Error::new_spanned(
                        nv,
                        format!("expected string literal for `{key}`"),
                    ));
                }
            }
        }
    }
    Ok(None)
}

/// Convert a PascalCase or camelCase name to snake_case.
fn to_snake_case(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 4);
    let mut chars = s.chars().peekable();

    while let Some(c) = chars.next() {
        if c.is_uppercase() {
            if !result.is_empty() {
                // Insert underscore before uppercase if:
                // - previous char was lowercase, or
                // - next char is lowercase (handles "XMLParser" -> "xml_parser")
                let prev_was_lower = result
                    .chars()
                    .last()
                    .is_some_and(|p| p.is_lowercase() || p.is_ascii_digit());
                let next_is_lower = chars.peek().is_some_and(|n| n.is_lowercase());

                if prev_was_lower || next_is_lower {
                    result.push('_');
                }
            }
            result.push(c.to_lowercase().next().unwrap());
        } else {
            result.push(c);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::to_snake_case;

    #[test]
    fn test_snake_case() {
        assert_eq!(to_snake_case("Mood"), "mood");
        assert_eq!(to_snake_case("UserRole"), "user_role");
        assert_eq!(to_snake_case("VeryHappy"), "very_happy");
        assert_eq!(to_snake_case("XMLParser"), "xml_parser");
        assert_eq!(to_snake_case("SimpleEnum"), "simple_enum");
        assert_eq!(to_snake_case("A"), "a");
        assert_eq!(to_snake_case("AB"), "ab");
        assert_eq!(to_snake_case("ABCDef"), "abc_def");
    }
}
