use proc_macro2::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{LitChar, LitInt, LitStr, Token};

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Char(char),
    Str(String),
    Tuple(Vec<Value>),
}

impl Value {
    pub fn as_token(&self) -> TokenStream {
        use proc_macro2::Literal;
        match self {
            Value::Int(n) => { let l = Literal::i64_unsuffixed(*n); quote! { #l } }
            Value::Char(c) => quote! { #c },
            Value::Str(s) => quote! { #s },
            Value::Tuple(_) => panic!("cannot emit tuple directly into template"),
        }
    }
}

fn parse_literal(input: ParseStream) -> syn::Result<Value> {
    if input.peek(LitInt) {
        Ok(Value::Int(input.parse::<LitInt>()?.base10_parse()?))
    } else if input.peek(LitChar) {
        Ok(Value::Char(input.parse::<LitChar>()?.value()))
    } else if input.peek(LitStr) {
        Ok(Value::Str(input.parse::<LitStr>()?.value()))
    } else {
        Err(input.error("expected integer, char, or string literal"))
    }
}

pub fn parse_iterable(input: ParseStream) -> syn::Result<Vec<Value>> {
    if input.peek(syn::token::Bracket) {
        let content;
        syn::bracketed!(content in input);
        let mut vals = Vec::new();
        while !content.is_empty() {
            vals.push(parse_literal(&content)?);
            if !content.is_empty() {
                content.parse::<Token![,]>()?;
            }
        }
        Ok(vals)
    } else {
        let start: LitInt = input.parse()?;
        let start: i64 = start.base10_parse()?;
        if input.peek(Token![..=]) {
            input.parse::<Token![..=]>()?;
            let end: i64 = input.parse::<LitInt>()?.base10_parse()?;
            Ok((start..=end).map(Value::Int).collect())
        } else {
            input.parse::<Token![..]>()?;
            let end: i64 = input.parse::<LitInt>()?.base10_parse()?;
            Ok((start..end).map(Value::Int).collect())
        }
    }
}
