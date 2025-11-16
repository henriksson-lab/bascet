use heck::ToSnakeCase;
use proc_macro::TokenStream;
use quote::quote;
use std::collections::HashMap;
use syn::{
    parse::Parse, parse::ParseStream, parse_macro_input, Data, DeriveInput, Fields, Ident, Token,
};

enum TraitSpec {
    Default {
        trait_ident: Ident,
    },
    Override {
        trait_ident: Ident,
        field_ident: Ident,
    },
}

impl Parse for TraitSpec {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let trait_ident = input.parse()?;
        if input.peek(Token![=]) {
            input.parse::<Token![=]>()?;
            let field_ident = input.parse()?;
            return Ok(TraitSpec::Override {
                trait_ident,
                field_ident,
            });
        }
        Ok(TraitSpec::Default { trait_ident })
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

pub fn derive_composite(item: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(item as DeriveInput);
    let name = input.ident.clone();

    let trait_list = input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("attrs"))
        .map(|attr| {
            attr.parse_args::<TraitList>()
                .expect("Invalid attrs syntax")
        })
        .expect("Missing #[attrs(...)] attribute");

    let Data::Struct(data) = &mut input.data else {
        panic!("Only structs supported");
    };
    let Fields::Named(named_fields) = &mut data.fields else {
        panic!("Only named fields supported");
    };
    let fields = &mut named_fields.named;

    let mut map: HashMap<String, (Ident, syn::Type)> = HashMap::new();

    for spec in &trait_list.specs {
        let (tname, fname, ftype) = match spec {
            TraitSpec::Default { trait_ident } => {
                let tname = trait_ident.to_string();
                let snake = tname.to_snake_case();
                let field = fields
                    .iter()
                    .find(|f| f.ident.as_ref() == Some(&Ident::new(&snake, trait_ident.span())))
                    .unwrap_or_else(|| panic!("No field '{}' for {}", snake, tname));
                (tname, field.ident.clone().unwrap(), field.ty.clone())
            }
            TraitSpec::Override {
                trait_ident,
                field_ident,
            } => {
                let tname = trait_ident.to_string();
                let field = fields
                    .iter()
                    .find(|f| f.ident.as_ref() == Some(field_ident))
                    .unwrap_or_else(|| panic!("Field '{}' not found for {}", field_ident, tname));
                (tname, field_ident.clone(), field.ty.clone())
            }
        };

        map.insert(tname, (fname, ftype));
    }

    input.attrs.retain(|attr| !attr.path().is_ident("attrs"));

    let attr_idents = trait_list.specs.iter().map(|spec| match spec {
        TraitSpec::Default { trait_ident } | TraitSpec::Override { trait_ident, .. } => trait_ident,
    });

    let impls = trait_list.specs.iter().map(|spec| {
        let trait_ident = match spec {
            TraitSpec::Default { trait_ident } | TraitSpec::Override { trait_ident, .. } => {
                trait_ident
            }
        };
        let (fname, ftype) = &map[&trait_ident.to_string()];
        quote! {
            impl bascet_core::Get<#trait_ident> for #name {
                type Value = #ftype;
                fn get(&self) -> &Self::Value { &self.#fname }
                fn get_mut(&mut self) -> &mut Self::Value { &mut self.#fname }
            }
        }
    });

    TokenStream::from(quote! {
        impl bascet_core::Composite for #name {
            type Attrs = (#(#attr_idents),*);
        }
        #(#impls)*
    })
}
