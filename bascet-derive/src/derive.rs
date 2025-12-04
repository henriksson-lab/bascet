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

struct BascetAttr {
    attrs: Option<syn::punctuated::Punctuated<AttrSpec, Token![,]>>,
    backing: Option<Ident>,
    marker: Option<Ident>,
}

impl Parse for BascetAttr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        use syn::parenthesized;

        let mut attrs = None;
        let mut backing = None;
        let mut marker = None;

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
                _ => return Err(syn::Error::new(key.span(), "Unknown bascet parameter")),
            }

            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(BascetAttr { attrs, backing, marker })
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
            attr.parse_args::<BascetAttr>()
                .expect("Invalid bascet syntax")
        })
        .expect("Missing #[bascet(...)] attribute");

    let trait_list_specs = bascet_attr
        .attrs
        .expect("Missing attrs in #[bascet(...)]. Specify: attrs = (Id, Sequence, ...)");

    let backing_ident = bascet_attr.backing;

    let marker_ident = bascet_attr
        .marker
        .expect("Missing marker in #[bascet(...)]. Specify: marker = AsRecord");

    let Data::Struct(data) = &mut input.data else {
        panic!("Only structs supported");
    };
    let Fields::Named(named_fields) = &mut data.fields else {
        panic!("Only named fields supported");
    };
    let fields = &mut named_fields.named;

    let mut map: HashMap<String, (Ident, syn::Type)> = HashMap::new();

    for spec in &trait_list_specs {
        let (tname, fname, ftype) = match spec {
            AttrSpec::Default { trait_ident } => {
                let tname = trait_ident.to_string();
                let snake = tname.to_snake_case();
                let field = fields
                    .iter()
                    .find(|f| f.ident.as_ref() == Some(&Ident::new(&snake, trait_ident.span())))
                    .unwrap_or_else(|| panic!("No field '{}' for {}", snake, tname));
                (tname, field.ident.clone().unwrap(), field.ty.clone())
            }
            AttrSpec::Override {
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

    input.attrs.retain(|attr| !attr.path().is_ident("bascet"));

    let attr_idents = trait_list_specs.iter().map(|spec| match spec {
        AttrSpec::Default { trait_ident } | AttrSpec::Override { trait_ident, .. } => trait_ident,
    });

    let attr_impls = trait_list_specs.iter().map(|spec| {
        let trait_ident = match spec {
            AttrSpec::Default { trait_ident } | AttrSpec::Override { trait_ident, .. } => {
                trait_ident
            }
        };
        let (fname, ftype) = &map[&trait_ident.to_string()];
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

    let (backing_type, backing_impl) = if let Some(backing_ident) = backing_ident {
        let backing_field_name = backing_ident.to_string().to_snake_case();
        let backing_field_ident = Ident::new(&backing_field_name, backing_ident.span());

        let backing_field = fields
            .iter()
            .find(|f| f.ident.as_ref() == Some(&backing_field_ident))
            .unwrap_or_else(|| panic!("No field '{}' for backing {}", backing_field_name, backing_ident));

        let backing_field_type = &backing_field.ty;

        let backing_type = quote! { type Backing = #backing_ident; };
        let backing_impl = quote! {
            impl bascet_core::Get<#backing_ident> for #name {
                type Value = #backing_field_type;
                fn as_ref(&self) -> &Self::Value { &self.#backing_field_ident }
                fn as_mut(&mut self) -> &mut Self::Value { &mut self.#backing_field_ident }
            }
        };
        (backing_type, Some(backing_impl))
    } else {
        (quote! { type Backing = bascet_core::OwnedBacking; }, None)
    };

    let marker_type = quote! {
        type Marker = bascet_core::#marker_ident;
    };

    TokenStream::from(quote! {
        impl bascet_core::Composite for #name {
            #attr_type
            #backing_type
            #marker_type
        }
        #(#attr_impls)*
        #backing_impl
    })
}

struct ContextSpec {
    marker: Ident,
    context_type: Type,
}

impl Parse for ContextSpec {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let marker = input.parse()?;
        input.parse::<Token![:]>()?;
        let context_type = input.parse()?;
        Ok(ContextSpec {
            marker,
            context_type,
        })
    }
}

pub fn derive_contexts(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let name = &input.ident;

    let contexts_attr = input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("context"))
        .expect("Missing #[context(...)] attribute");

    let specs = contexts_attr
        .parse_args_with(syn::punctuated::Punctuated::<ContextSpec, Token![,]>::parse_terminated)
        .expect("Invalid context syntax");

    let impls = specs.iter().map(|spec| {
        let marker = &spec.marker;
        let context_type = &spec.context_type;
        quote! {
            impl bascet_core::Context<bascet_core::#marker> for #name {
                type Context = #context_type;
                type Marker = bascet_core::#marker;
            }
        }
    });

    TokenStream::from(quote! { #(#impls)* })
}
