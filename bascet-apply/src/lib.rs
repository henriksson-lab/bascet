use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, Expr, Ident, Result, Token, Type,
};

struct Input {
    m: Type,
    target: Expr,
    attrs: Vec<(Ident, Expr)>,
}

impl Parse for Input {
    fn parse(input: ParseStream) -> Result<Self> {
        let m = input.parse()?;
        input.parse::<Token![,]>()?;
        let target = input.parse()?;
        input.parse::<Token![,]>()?;

        let content;
        syn::braced!(content in input);

        let mut attrs = Vec::new();
        while !content.is_empty() {
            let attr = content.parse()?;
            content.parse::<Token![=>]>()?;
            let value = content.parse()?;
            attrs.push((attr, value));
            if !content.is_empty() {
                content.parse::<Token![,]>()?;
            }
        }

        Ok(Input { m, target, attrs })
    }
}

#[proc_macro]
pub fn apply_selected(input: TokenStream) -> TokenStream {
    let Input { m, target, attrs } = parse_macro_input!(input as Input);

    let selected = match &m {
        Type::Tuple(t) => t
            .elems
            .iter()
            .filter_map(|ty| match ty {
                Type::Path(p) => p.path.get_ident().cloned(),
                _ => None,
            })
            .collect(),
        Type::Path(p) => p.path.get_ident().cloned().into_iter().collect(),
        _ => Vec::new(),
    };

    let calls = attrs.iter().filter(|(a, _)| selected.contains(a)).map(
        |(attr, val)| quote! {
            ::bascet_core::Tagged::<#attr, _>::new(#val).put(&mut #target);
        },
    );

    quote! {{
        use ::bascet_core::Put as _;
        #(#calls)*
    }}
    .into()
}
