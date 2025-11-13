use heck::ToSnakeCase;
use proc_macro::TokenStream;
use quote::quote;
use std::collections::HashMap;
use syn::{
    parse::Parse, parse::ParseStream, parse_macro_input, Data, DeriveInput, Fields, Ident, Token,
};

enum TraitSpec {
    Default { trait_ident: Ident },
    Override { trait_ident: Ident, field_ident: Ident },
    NoBuild { trait_ident: Ident, nobuild_type: syn::Type },
}

impl Parse for TraitSpec {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let trait_ident: Ident = input.parse()?;

        if input.peek(syn::token::Paren) {
            let content;
            syn::parenthesized!(content in input);
            let flag: Ident = content.parse()?;
            if flag != "nobuild" {
                return Err(syn::Error::new(flag.span(), "Expected 'nobuild'"));
            }
            content.parse::<Token![:]>()?;
            let nobuild_type = content.parse()?;
            return Ok(TraitSpec::NoBuild { trait_ident, nobuild_type });
        }

        if input.peek(Token![=]) {
            input.parse::<Token![=]>()?;
            let field_ident = input.parse()?;
            return Ok(TraitSpec::Override { trait_ident, field_ident });
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

pub fn attrs(attrs: TokenStream, item: TokenStream) -> TokenStream {
    let trait_list = parse_macro_input!(attrs as TraitList);
    let mut input = parse_macro_input!(item as DeriveInput);
    let struct_name = input.ident.clone();

    let Data::Struct(data) = &mut input.data else {
        panic!("Only structs are supported");
    };
    let Fields::Named(named_fields) = &mut data.fields else {
        panic!("Only named fields are supported");
    };
    let fields = &mut named_fields.named;

    let mut trait_to_field: HashMap<String, (Ident, syn::Type, usize)> = HashMap::new();
    let mut field_usage: HashMap<String, Vec<String>> = HashMap::new();

    for spec in &trait_list.specs {
        let (name, fname, ftype, idx) = match spec {
            TraitSpec::Default { trait_ident } => {
                let name = trait_ident.to_string();
                let snake = name.to_snake_case();
                let (idx, field) = fields
                    .iter()
                    .enumerate()
                    .find(|(_, f)| f.ident.as_ref() == Some(&Ident::new(&snake, trait_ident.span())))
                    .unwrap_or_else(|| panic!("No field '{}' for trait {}", snake, name));
                (name, field.ident.clone().unwrap(), field.ty.clone(), idx)
            }
            TraitSpec::Override { trait_ident, field_ident } => {
                let name = trait_ident.to_string();
                let (idx, field) = fields
                    .iter()
                    .enumerate()
                    .find(|(_, f)| f.ident.as_ref() == Some(field_ident))
                    .unwrap_or_else(|| panic!("Field '{}' not found for trait {}", field_ident, name));
                (name, field_ident.clone(), field.ty.clone(), idx)
            }
            TraitSpec::NoBuild { .. } => continue,
        };

        trait_to_field.insert(name.clone(), (fname.clone(), ftype, idx));
        field_usage
            .entry(fname.to_string())
            .or_default()
            .push(format!("Provide{}", name));
    }

    for (fname, traits) in &field_usage {
        if traits.len() > 1 {
            let msg = format!("Field `{}` is used by multiple traits: {}", fname, traits.join(", "));
            if let Some(field) = fields
                .iter_mut()
                .find(|f| f.ident.as_ref().map(|i| i.to_string()).as_ref() == Some(fname))
            {
                field.attrs.push(syn::parse_quote! { #[deprecated(note = #msg)] });
            }
        }
    }

    let trait_impls = trait_list.specs.iter().map(|spec| {
        let (name, is_nobuild) = match spec {
            TraitSpec::Default { trait_ident } | TraitSpec::Override { trait_ident, .. } => {
                (trait_ident.to_string(), false)
            }
            TraitSpec::NoBuild { trait_ident, .. } => (trait_ident.to_string(), true),
        };

        let trait_path: syn::Path =
            syn::parse_str(&format!("crate::cell::attr::Provide{}", name)).unwrap();

        if is_nobuild {
            quote! {
                impl #trait_path for #struct_name {
                    type Type = ();
                    fn as_ref(&self) -> &Self::Type {
                        unreachable!("Cannot get {} - marked as nobuild", stringify!(#name))
                    }
                    fn as_mut(&mut self) -> &mut Self::Type {
                        unreachable!("Cannot get {} - marked as nobuild", stringify!(#name))
                    }
                }
            }
        } else {
            let (fname, ftype, _) = &trait_to_field[&name];
            quote! {
                impl #trait_path for #struct_name {
                    type Type = #ftype;
                    fn as_ref(&self) -> &Self::Type { &self.#fname }
                    fn as_mut(&mut self) -> &mut Self::Type { &mut self.#fname }
                }
            }
        }
    });

    let builder_name = Ident::new(&format!("{}Builder", struct_name), struct_name.span());
    let field_names: Vec<_> = fields.iter().map(|f| f.ident.clone()).collect();
    let field_types: Vec<_> = fields.iter().map(|f| f.ty.clone()).collect();

    let (field_defaults, field_setters): (Vec<_>, Vec<_>) = fields
        .iter_mut()
        .map(|f| {
            let mut default = None;
            let mut setter = None;
            f.attrs.retain(|attr| {
                if attr.path().is_ident("build_default") {
                    default = attr.parse_args::<syn::Expr>().ok();
                    false
                } else if attr.path().is_ident("build_set") {
                    setter = attr.parse_args::<syn::Expr>().ok();
                    false
                } else {
                    true
                }
            });
            (default, setter)
        })
        .unzip();

    let build_impls = trait_list.specs.iter().map(|spec| {
        match spec {
            TraitSpec::Default { trait_ident } | TraitSpec::Override { trait_ident, .. } => {
                let name = trait_ident.to_string();
                let attr_path: syn::Path =
                    syn::parse_str(&format!("crate::cell::attr::{}", name)).unwrap();
                let (fname, ftype, idx) = &trait_to_field[&name];

                let body = match &field_setters[*idx] {
                    Some(setter) => quote! { (#setter)(builder, value) },
                    None => quote! { builder.#fname = value; builder },
                };

                quote! {
                    impl crate::cell::core::Build<#builder_name> for #attr_path {
                        type Type = #ftype;
                        fn build(mut builder: #builder_name, value: Self::Type) -> #builder_name {
                            #body
                        }
                    }
                }
            }
            TraitSpec::NoBuild { trait_ident, nobuild_type } => {
                let name = trait_ident.to_string();
                let attr_path: syn::Path =
                    syn::parse_str(&format!("crate::cell::attr::{}", name)).unwrap();
                quote! {
                    impl crate::cell::core::Build<#builder_name> for #attr_path {
                        type Type = #nobuild_type;
                        fn build(builder: #builder_name, _value: Self::Type) -> #builder_name {
                            builder
                        }
                    }
                }
            }
        }
    });

    let default_impl = if field_defaults.iter().any(|d| d.is_some()) {
        let inits = field_names.iter().zip(&field_defaults).map(|(name, default)| {
            match default {
                Some(expr) => quote! { #name: (#expr)() },
                None => quote! { #name: Default::default() },
            }
        });
        quote! {
            impl Default for #builder_name {
                fn default() -> Self {
                    Self { #(#inits,)* }
                }
            }
        }
    } else {
        quote! { #[derive(Default)] }
    };

    let expanded = quote! {
        #input
        #(#trait_impls)*

        #default_impl
        pub struct #builder_name {
            #(pub #field_names: #field_types,)*
        }

        impl crate::cell::core::Builder for #builder_name {
            type Builds = #struct_name;
            fn build(self) -> Self::Builds {
                Self::Builds { #(#field_names: self.#field_names,)* }
            }
        }

        impl crate::cell::core::Cell for #struct_name {
            type Builder = #builder_name;
            fn builder() -> Self::Builder { Default::default() }
        }

        #(#build_impls)*
    };

    TokenStream::from(expanded)
}