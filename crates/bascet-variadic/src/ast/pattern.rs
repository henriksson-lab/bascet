use std::collections::HashMap;

use proc_macro2::Span;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, Token};

use super::value::Lit;

pub enum Pattern {
    Ident(String),
    Tuple(Vec<Pattern>),
}

impl Parse for Pattern {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        match input.peek(syn::token::Paren) {
            true => {
                let content;
                syn::parenthesized!(content in input);
                let mut pats = Vec::new();
                while !content.is_empty() {
                    pats.push(content.parse::<Pattern>()?);
                    if !content.is_empty() {
                        content.parse::<Token![,]>()?;
                    }
                }
                Ok(Pattern::Tuple(pats))
            }
            false => Ok(Pattern::Ident(input.parse::<Ident>()?.to_string())),
        }
    }
}

pub fn resolve(pattern: &Pattern, value: &Lit, map: &mut HashMap<String, Lit>) -> syn::Result<()> {
    match (pattern, value) {
        (Pattern::Ident(name), val) => {
            map.insert(name.clone(), val.clone());
            Ok(())
        }
        (Pattern::Tuple(pats), Lit::Tuple(vals)) => {
            for (p, v) in pats.iter().zip(vals.iter()) {
                resolve(p, v, map)?;
            }
            Ok(())
        }
        _ => Err(syn::Error::new(
            Span::call_site(),
            "pattern does not match value structure",
        )),
    }
}
