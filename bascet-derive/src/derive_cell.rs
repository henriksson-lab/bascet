use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};
use heck::ToSnakeCase;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

static FIELD_USAGE: OnceLock<Mutex<HashMap<(String, String), Vec<String>>>> = OnceLock::new();
pub fn derive_provide_impl(input: TokenStream, trait_name: &str) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;
    let struct_name_str = struct_name.to_string();

    let attr_name = trait_name.strip_prefix("Provide")
        .unwrap_or_else(|| panic!("Trait name '{}' must start with 'Provide'", trait_name));
    let snake_case_name = attr_name.to_snake_case();

    let (field_name, found_field) = match &input.data {
        Data::Struct(data) => {
            match &data.fields {
                Fields::Named(fields) => {
                    let attr_field = fields.named.iter().find(|f| {
                        f.attrs.iter().any(|attr| {
                            attr.path().is_ident("cell") &&
                            attr.parse_args::<syn::Ident>()
                                .map(|id| id == attr_name)
                                .unwrap_or(false)
                        })
                    });

                    if let Some(field) = attr_field {
                        (field.ident.as_ref(), Some(field))
                    } else {
                        let fallback_field = fields.named.iter().find(|f| {
                            f.ident.as_ref().map(|i| i == &snake_case_name).unwrap_or(false)
                        });
                        (fallback_field.and_then(|f| f.ident.as_ref()), fallback_field)
                    }
                }
                _ => panic!("Only named fields are supported"),
            }
        }
        _ => panic!("Only structs are supported"),
    };

    let field_name = field_name.unwrap_or_else(|| panic!(
        "No field with #[cell({})] attribute or field named '{}' found",
        attr_name, snake_case_name
    ));

    let field_name_str = field_name.to_string();

    let usage_map = FIELD_USAGE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = usage_map.lock().unwrap();

    let key = (struct_name_str.clone(), field_name_str.clone());
    let traits = map.entry(key).or_insert_with(Vec::new);

    if !traits.is_empty() {
        eprintln!(
            "warning: Field '{}' in struct '{}' is used by {} but is already used by: {}",
            field_name_str,
            struct_name_str,
            trait_name,
            traits.join(", ")
        );
    }
    traits.push(trait_name.to_string());

    let trait_path: syn::Path = syn::parse_str(&format!("crate::cell::marker::{}", trait_name))
        .expect("Failed to parse trait path");

    let field_type = match &input.data {
        Data::Struct(data) => {
            match &data.fields {
                Fields::Named(fields) => {
                    fields.named.iter()
                        .find(|f| f.ident.as_ref() == Some(field_name))
                        .map(|f| &f.ty)
                }
                _ => None
            }
        }
        _ => None
    }.expect("Failed to get field type");

    let expanded = quote! {
        impl #trait_path for #struct_name {
            type Type = #field_type;

            fn value(&self) -> Self::Type {
                self.#field_name.clone()
            }
        }
    };

    TokenStream::from(expanded)
}

pub fn derive_use_managed_ref_impl(_input: TokenStream) -> TokenStream {
    unimplemented!()
}