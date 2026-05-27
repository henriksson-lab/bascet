use proc_macro::TokenStream;
use std::collections::HashMap;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, Token};

mod ast;
mod emit;
mod iter;

use ast::{parse_iterable, Value};
use emit::{emit, Pattern};
use iter::IterExpr;

struct ExpandArgs { name: String, values: Vec<Value> }

impl Parse for ExpandArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name = input.parse::<Ident>()?.to_string();
        input.parse::<Token![in]>()?;
        Ok(Self { name, values: parse_iterable(input)? })
    }
}

struct IterArgs { env: HashMap<String, Vec<Value>>, pattern: Pattern, expr: IterExpr }

impl Parse for IterArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut env = HashMap::new();
        while input.peek(Ident) && input.peek2(Token![=]) {
            let name = input.parse::<Ident>()?.to_string();
            input.parse::<Token![=]>()?;
            let values = parse_iterable(input)?;
            env.insert(name, values);
            if input.peek(Token![;]) { break; }
            input.parse::<Token![,]>()?;
        }
        if !env.is_empty() {
            input.parse::<Token![;]>()?;
        }
        input.parse::<Token![for]>()?;
        let pattern = input.parse()?;
        input.parse::<Token![in]>()?;
        Ok(Self { env, pattern, expr: input.parse()? })
    }
}

#[proc_macro_attribute]
pub fn expand(args: TokenStream, item: TokenStream) -> TokenStream {
    let ExpandArgs { name, values } = syn::parse_macro_input!(args as ExpandArgs);
    let env = HashMap::from([(name.clone(), values.clone())]);
    emit(item.into(), &Pattern::Ident(name), values, &env).into()
}

#[proc_macro_attribute]
pub fn iter(args: TokenStream, item: TokenStream) -> TokenStream {
    let IterArgs { env, pattern, expr } = syn::parse_macro_input!(args as IterArgs);
    emit(item.into(), &pattern, expr.eval(&env), &env).into()
}
