use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, Ident, Token, Type,
};

struct ParserInput {
    trait_name: Ident,
    mappings: Vec<(Ident, Type)>,
}

impl Parse for ParserInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let trait_name: Ident = input.parse()?;
        input.parse::<Token![,]>()?;

        let mut mappings = Vec::new();
        while !input.is_empty() {
            let marker: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            let item_type: Type = input.parse()?;
            mappings.push((marker, item_type));
            if !input.is_empty() {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(ParserInput {
            trait_name,
            mappings,
        })
    }
}

pub fn define_parser(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as ParserInput);
    let trait_name = &parsed.trait_name;

    let trait_impls = parsed.mappings.iter().map(|(marker, item_type)| {
        quote! {
            impl #trait_name for #marker {
                type Item = #item_type;
            }
        }
    });

    let expanded = quote! {
        pub trait #trait_name {
            type Item;
        }

        #(#trait_impls)*
    };

    TokenStream::from(expanded)
}
