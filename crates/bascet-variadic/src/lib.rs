use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use std::collections::HashMap;
use syn::parse::ParseStream;
use syn::{Ident, Token};

mod ast;
mod emit;

use ast::{IterExpr, Pattern, parse_iterable};
use emit::emit;

#[proc_macro]
pub fn variadic(input: TokenStream) -> TokenStream {
    let input2: TokenStream2 = input.into();
    syn::parse::Parser::parse2(
        |input: ParseStream| -> syn::Result<TokenStream2> {
            let mut env = HashMap::new();
            while input.peek(Ident) && input.peek2(Token![=]) {
                let name = input.parse::<Ident>()?.to_string();
                input.parse::<Token![=]>()?;
                let values = parse_iterable(input)?;
                env.insert(name, values);
                if input.peek(Token![for]) {
                    break;
                }
                input.parse::<Token![,]>()?;
            }
            input.parse::<Token![for]>()?;
            let pattern = input.parse::<Pattern>()?;
            input.parse::<Token![in]>()?;
            let expr = input.parse::<IterExpr>()?;
            input.parse::<Token![=>]>()?;
            let content;
            syn::braced!(content in input);
            let item: TokenStream2 = content.parse()?;
            emit(item, &pattern, expr.eval(&env)?, &env)
        },
        input2,
    )
    .unwrap_or_else(|e| e.to_compile_error())
    .into()
}
