use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Ident, Token, parse::Parse, parse::ParseStream};
use heck::ToSnakeCase;
use std::collections::HashMap;

enum TraitSpec {
    Default { trait_ident: Ident },
    Override { trait_ident: Ident, field_ident: Ident },
}

impl Parse for TraitSpec {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let trait_name: Ident = input.parse()?;
        if input.peek(Token![=]) {
            input.parse::<Token![=]>()?;
            let field_name: Ident = input.parse()?;
            Ok(TraitSpec::Override { trait_ident: trait_name, field_ident: field_name })
        } else {
            Ok(TraitSpec::Default { trait_ident: trait_name })
        }
    }
}

struct TraitList {
    specs: syn::punctuated::Punctuated<TraitSpec, Token![,]>,
}

impl Parse for TraitList {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Ok(TraitList {
            specs: input.parse_terminated(TraitSpec::parse, Token![,])?,
        })
    }
}

pub fn attrs(attrs: TokenStream, item: TokenStream) -> TokenStream {
    let trait_list = parse_macro_input!(attrs as TraitList);
    let mut input = parse_macro_input!(item as DeriveInput);
    let struct_name = &input.ident;

    let fields = match &mut input.data {
        Data::Struct(data) => match &mut data.fields {
            Fields::Named(fields) => &mut fields.named,
            _ => panic!("Only named fields are supported"),
        },
        _ => panic!("Only structs are supported"),
    };

    let mut trait_to_field: HashMap<String, (Ident, syn::Type)> = HashMap::new();
    let mut field_usage: HashMap<String, Vec<String>> = HashMap::new();

    for spec in trait_list.specs.iter() {
        let (trait_name, field_name, field_type) = match spec {
            TraitSpec::Default { trait_ident } => {
                let name = trait_ident.to_string();
                let snake = name.to_snake_case();
                let field = fields.iter()
                    .find(|f| f.ident.as_ref().map(|i| i == &snake).unwrap_or(false))
                    .unwrap_or_else(|| panic!("No field '{}' for trait {}", snake, name));
                (name, field.ident.as_ref().unwrap().clone(), field.ty.clone())
            }
            TraitSpec::Override { trait_ident, field_ident } => {
                let name = trait_ident.to_string();
                let field = fields.iter()
                    .find(|f| f.ident.as_ref() == Some(field_ident))
                    .unwrap_or_else(|| panic!("Field '{}' not found for trait {}", field_ident, name));
                (name, field_ident.clone(), field.ty.clone())
            }
        };

        trait_to_field.insert(trait_name.clone(), (field_name.clone(), field_type));
        field_usage.entry(field_name.to_string())
            .or_insert_with(Vec::new)
            .push(format!("Provide{}", trait_name));
    }

    for (field_name, traits) in &field_usage {
        if traits.len() > 1 {
            let msg = format!(
                "Field `{}` is used by multiple traits: {}",
                field_name,
                traits.join(", ")
            );
            for field in fields.iter_mut() {
                if field.ident.as_ref().map(|i| i.to_string()) == Some(field_name.clone()) {
                    field.attrs.push(syn::parse_quote! { #[deprecated(note = #msg)] });
                    break;
                }
            }
        }
    }

    let trait_impls = trait_list.specs.iter().map(|spec| {
        let trait_name = match spec {
            TraitSpec::Default { trait_ident } => trait_ident.to_string(),
            TraitSpec::Override { trait_ident, .. } => trait_ident.to_string(),
        };
        let trait_path: syn::Path = syn::parse_str(&format!("crate::cell::marker::Provide{}", trait_name)).unwrap();
        let (field_name, field_type) = &trait_to_field[&trait_name];

        quote! {
            impl #trait_path for #struct_name {
                type Type = #field_type;
                fn value(&self) -> &Self::Type {
                    &self.#field_name
                }
                fn value_mut(&mut self) -> &mut Self::Type {
                    &mut self.#field_name
                }
            }
        }
    });

    let expanded = quote! {
        #input
        #(#trait_impls)*
    };

    TokenStream::from(expanded)
}
