use std::collections::HashMap;
use proc_macro2::{Delimiter, Group, TokenStream, TokenTree};
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, Token};
use crate::ast::Value;

pub enum Pattern {
    Ident(String),
    Tuple(Vec<Pattern>),
}

impl Parse for Pattern {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.peek(syn::token::Paren) {
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
        } else {
            Ok(Pattern::Ident(input.parse::<Ident>()?.to_string()))
        }
    }
}

pub fn resolve(pattern: &Pattern, value: &Value, map: &mut HashMap<String, Value>) {
    match (pattern, value) {
        (Pattern::Ident(name), val) => { map.insert(name.clone(), val.clone()); }
        (Pattern::Tuple(pats), Value::Tuple(vals)) => {
            for (p, v) in pats.iter().zip(vals.iter()) {
                resolve(p, v, map);
            }
        }
        _ => panic!("pattern does not match value structure"),
    }
}

pub fn emit(ts: TokenStream, pattern: &Pattern, values: Vec<Value>, env: &HashMap<String, Vec<Value>>) -> TokenStream {
    let mut out = TokenStream::new();
    for val in values {
        let mut bindings = HashMap::new();
        resolve(pattern, &val, &mut bindings);
        out.extend(Transcriber { bindings: &bindings, env }.run(ts.clone(), None));
    }
    out
}

struct Transcriber<'a> {
    bindings: &'a HashMap<String, Value>,
    env: &'a HashMap<String, Vec<Value>>,
}

impl<'a> Transcriber<'a> {
    fn run(&self, ts: TokenStream, index: Option<&Value>) -> TokenStream {
        let tokens: Vec<TokenTree> = ts.into_iter().collect();
        let mut out = TokenStream::new();
        let mut i = 0;
        while i < tokens.len() {
            if let Some((expanded, consumed)) = self.try_expand(&tokens, i) {
                out.extend(expanded);
                i += consumed;
                continue;
            }
            match &tokens[i] {
                TokenTree::Punct(p) if p.as_char() == '#' && index.is_some() => {
                    let next_is_bracket = i + 1 < tokens.len()
                        && matches!(&tokens[i + 1], TokenTree::Group(g) if g.delimiter() == Delimiter::Bracket);
                    if next_is_bracket {
                        out.extend(std::iter::once(tokens[i].clone()));
                    } else {
                        out.extend(index.unwrap().as_token());
                    }
                    i += 1;
                }
                TokenTree::Ident(ident) => {
                    if let Some(val) = self.bindings.get(&ident.to_string()) {
                        out.extend(val.as_token());
                    } else {
                        out.extend(quote! { #ident });
                    }
                    i += 1;
                }
                TokenTree::Group(group) => {
                    let inner = self.run(group.stream(), index);
                    out.extend(std::iter::once(TokenTree::Group(Group::new(group.delimiter(), inner))));
                    i += 1;
                }
                other => {
                    out.extend(std::iter::once(other.clone()));
                    i += 1;
                }
            }
        }
        out
    }

    fn try_expand(&self, tokens: &[TokenTree], i: usize) -> Option<(TokenStream, usize)> {
        let mut j = i;

        let TokenTree::Punct(p) = tokens.get(j)? else { return None; };
        if p.as_char() != '@' { return None; }
        j += 1;

        let TokenTree::Ident(var_ident) = tokens.get(j)? else { return None; };
        let var_name = var_ident.to_string();
        let Some(Value::Int(n)) = self.bindings.get(&var_name) else { return None; };
        let n = *n;
        j += 1;

        let TokenTree::Group(bracket) = tokens.get(j)? else { return None; };
        if bracket.delimiter() != Delimiter::Bracket { return None; }
        let template = bracket.stream();
        j += 1;

        let sep = tokens.get(j)
            .and_then(|t| if let TokenTree::Group(g) = t { Some(g) } else { None })
            .filter(|g| g.delimiter() == Delimiter::Parenthesis)
            .and_then(|g| {
                j += 1;
                let parser = |input: ParseStream| -> syn::Result<String> {
                    input.parse::<Ident>()?;
                    input.parse::<Token![=]>()?;
                    Ok(input.parse::<syn::LitStr>()?.value())
                };
                syn::parse::Parser::parse2(parser, g.stream()).ok()
            });

        let consumed = j - i;

        let iter_values: Vec<Value> = self.env.get(&var_name)
            .map(|seq| seq.iter().filter(|v| matches!(v, Value::Int(m) if *m <= n)).cloned().collect())
            .unwrap_or_default();

        let mut out = TokenStream::new();
        for (k, val) in iter_values.iter().enumerate() {
            if k > 0 {
                if let Some(ref s) = sep {
                    out.extend(s.parse::<TokenStream>().unwrap_or_default());
                }
            }
            out.extend(self.run(template.clone(), Some(val)));
        }

        Some((out, consumed))
    }
}