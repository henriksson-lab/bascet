use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::parse::ParseStream;
use syn::{LitChar, LitInt, LitStr, Token};

#[derive(Debug, Clone, PartialEq)]
pub enum Lit {
    Int(i64),
    Char(char),
    Str(String),
    Tuple(Vec<Lit>),
    Ident(String),
}

pub struct Iterable;

impl Lit {
    pub fn as_token(&self) -> syn::Result<TokenStream> {
        use proc_macro2::Literal;
        match self {
            Lit::Int(n) => {
                let l = Literal::i64_unsuffixed(*n);
                Ok(quote! { #l })
            }
            Lit::Char(c) => Ok(quote! { #c }),
            Lit::Str(s) => Ok(quote! { #s }),
            Lit::Tuple(_) => Err(syn::Error::new(
                Span::call_site(),
                "cannot emit tuple directly into template",
            )),
            Lit::Ident(_) => Err(syn::Error::new(
                Span::call_site(),
                "unresolved identifier in template",
            )),
        }
    }

    fn from_stream(input: ParseStream) -> syn::Result<Self> {
        if input.peek(LitInt) {
            Ok(Self::Int(input.parse::<LitInt>()?.base10_parse()?))
        } else if input.peek(LitChar) {
            Ok(Self::Char(input.parse::<LitChar>()?.value()))
        } else if input.peek(LitStr) {
            Ok(Self::Str(input.parse::<LitStr>()?.value()))
        } else {
            Err(input.error("expected integer, char, or string literal"))
        }
    }
}

impl Iterable {
    pub fn from_stream(input: ParseStream) -> syn::Result<Vec<Lit>> {
        if input.peek(syn::token::Bracket) {
            let content;
            syn::bracketed!(content in input);
            let mut vals = Vec::new();
            while !content.is_empty() {
                vals.push(Lit::from_stream(&content)?);
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
                Ok((start..=end).map(Lit::Int).collect())
            } else {
                input.parse::<Token![..]>()?;
                let end: i64 = input.parse::<LitInt>()?.base10_parse()?;
                Ok((start..end).map(Lit::Int).collect())
            }
        }
    }
}
