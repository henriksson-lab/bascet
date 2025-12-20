use heck::ToSnakeCase;
use proc_macro::TokenStream;
use quote::quote;
use std::collections::HashMap;
use syn::{
    parse::Parse, parse::ParseStream, parse_macro_input, Data, DeriveInput, Fields, Ident, Token,
    Type,
};

enum AttrSpec {
    Default {
        trait_ident: Ident,
    },
    Override {
        trait_ident: Ident,
        field_ident: Ident,
    },
}

impl Parse for AttrSpec {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let trait_ident = input.parse()?;
        if input.peek(Token![=]) {
            input.parse::<Token![=]>()?;
            let field_ident = input.parse()?;
            return Ok(AttrSpec::Override {
                trait_ident,
                field_ident,
            });
        }
        Ok(AttrSpec::Default { trait_ident })
    }
}

struct CompositeDef {
    attrs: Option<syn::punctuated::Punctuated<AttrSpec, Token![,]>>,
    backing: Option<Type>,
    marker: Option<Type>,
    intermediate: Option<Type>,
}

struct AttrDef {
    trait_ident: Ident,
    field_ident: Ident,
    field_type: Type,
    is_collection: bool,
}

impl Parse for CompositeDef {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        use syn::parenthesized;

        let mut attrs = None;
        let mut backing = None;
        let mut marker = None;
        let mut intermediate = None;

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;

            match key.to_string().as_str() {
                "attrs" => {
                    let content;
                    parenthesized!(content in input);
                    attrs = Some(content.parse_terminated(AttrSpec::parse, Token![,])?);
                }
                "backing" => {
                    backing = Some(input.parse()?);
                }
                "marker" => {
                    marker = Some(input.parse()?);
                }
                "intermediate" => {
                    intermediate = Some(input.parse()?);
                }
                _ => return Err(syn::Error::new(key.span(), "Unknown bascet parameter")),
            }

            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(CompositeDef {
            attrs,
            backing,
            marker,
            intermediate,
        })
    }
}

pub fn derive_composite(item: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(item as DeriveInput);
    let name = input.ident.clone();

    let bascet_attr = input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("bascet"))
        .map(|attr| {
            attr.parse_args::<CompositeDef>()
                .expect("Invalid bascet syntax")
        })
        .expect("Missing #[bascet(...)] attribute");

    let trait_list_specs = bascet_attr
        .attrs
        .expect("Missing attrs in #[bascet(...)]. Specify: attrs = (Id, Sequence, ...)");

    let backing_type = bascet_attr.backing;

    let marker_type = bascet_attr
        .marker
        .expect("Missing marker in #[bascet(...)]. Specify: marker = AsRecord");

    let Data::Struct(data) = &mut input.data else {
        panic!("Only structs supported");
    };
    let Fields::Named(named_fields) = &mut data.fields else {
        panic!("Only named fields supported");
    };
    let fields = &mut named_fields.named;

    let attr_defs: Vec<AttrDef> = trait_list_specs
        .iter()
        .map(|spec| {
            let (trait_ident, field_ident, field_type, is_collection) = match spec {
                AttrSpec::Default { trait_ident } => {
                    let tname = trait_ident.to_string();
                    let snake = tname.to_snake_case();
                    let field = fields
                        .iter()
                        .find(|f| f.ident.as_ref() == Some(&Ident::new(&snake, trait_ident.span())))
                        .unwrap_or_else(|| panic!("No field '{}' for {}", snake, tname));
                    let is_collection = field.attrs.iter().any(|a| a.path().is_ident("collection"));
                    (
                        trait_ident.clone(),
                        field.ident.clone().unwrap(),
                        field.ty.clone(),
                        is_collection,
                    )
                }
                AttrSpec::Override {
                    trait_ident,
                    field_ident,
                } => {
                    let tname = trait_ident.to_string();
                    let field = fields
                        .iter()
                        .find(|f| f.ident.as_ref() == Some(field_ident))
                        .unwrap_or_else(|| {
                            panic!("Field '{}' not found for {}", field_ident, tname)
                        });
                    let is_collection = field.attrs.iter().any(|a| a.path().is_ident("collection"));
                    (
                        trait_ident.clone(),
                        field_ident.clone(),
                        field.ty.clone(),
                        is_collection,
                    )
                }
            };

            AttrDef {
                trait_ident,
                field_ident,
                field_type,
                is_collection,
            }
        })
        .collect();

    for field in fields.iter_mut() {
        field
            .attrs
            .retain(|attr| !attr.path().is_ident("collection"));
    }

    let attr_idents = attr_defs.iter().map(|def| &def.trait_ident);

    let attr_impls = attr_defs.iter().map(|def| {
        let trait_ident = &def.trait_ident;
        let fname = &def.field_ident;
        let ftype = &def.field_type;
        quote! {
            impl bascet_core::Get<#trait_ident> for #name {
                type Value = #ftype;
                fn as_ref(&self) -> &Self::Value { &self.#fname }
                fn as_mut(&mut self) -> &mut Self::Value { &mut self.#fname }
            }
        }
    });

    let attr_type = quote! {
        type Attrs = (#(#attr_idents),*);
    };

    let (backing_type_assoc, backing_impl) = if let Some(backing_ty) = backing_type {
        let backing_ident = match &backing_ty {
            Type::Path(type_path) => type_path
                .path
                .segments
                .last()
                .map(|seg| seg.ident.clone())
                .expect("Empty path in backing type"),
            _ => panic!("Backing type must be a path type"),
        };

        let backing_field_name = backing_ident.to_string().to_snake_case();
        let backing_field_ident = Ident::new(&backing_field_name, backing_ident.span());

        let backing_field = fields
            .iter()
            .find(|f| f.ident.as_ref() == Some(&backing_field_ident))
            .unwrap_or_else(|| {
                panic!(
                    "No field '{}' for backing {}",
                    backing_field_name, backing_ident
                )
            });

        let backing_field_type = &backing_field.ty;

        let backing_type_assoc = quote! { type Backing = bascet_core::#backing_ty; };
        let backing_impl = quote! {
            impl bascet_core::Get<bascet_core::#backing_ty> for #name {
                type Value = #backing_field_type;
                fn as_ref(&self) -> &Self::Value { &self.#backing_field_ident }
                fn as_mut(&mut self) -> &mut Self::Value { &mut self.#backing_field_ident }
            }
        };
        (backing_type_assoc, Some(backing_impl))
    } else {
        (quote! { type Backing = bascet_core::OwnedBacking; }, None)
    };

    let marker_type_assoc = quote! {
        type Marker = bascet_core::#marker_type;
    };

    let intermediate_type_assoc = if let Some(intermediate_type) = bascet_attr.intermediate {
        quote! {
            type Intermediate = #intermediate_type;
        }
    } else {
        quote! {
            type Intermediate = Self;
        }
    };

    let collection_attrs: Vec<_> = attr_defs
        .iter()
        .filter(|def| def.is_collection)
        .map(|def| &def.trait_ident)
        .collect();

    let single_attrs: Vec<_> = attr_defs
        .iter()
        .filter(|def| !def.is_collection)
        .map(|def| &def.trait_ident)
        .collect();

    let collection_type_assoc = if !collection_attrs.is_empty() {
        quote! { type Collection = (#(#collection_attrs),*); }
    } else {
        quote! { type Collection = (); }
    };

    let single_type_assoc = if !single_attrs.is_empty() {
        quote! { type Single = (#(#single_attrs),*); }
    } else {
        quote! { type Single = (); }
    };

    TokenStream::from(quote! {
        impl bascet_core::Composite for #name {
            #attr_type
            #single_type_assoc
            #collection_type_assoc

            #marker_type_assoc
            #intermediate_type_assoc

            #backing_type_assoc
        }
        #(#attr_impls)*
        #backing_impl
    })
}
