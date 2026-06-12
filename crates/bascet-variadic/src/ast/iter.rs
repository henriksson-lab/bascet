use std::collections::HashMap;

use proc_macro2::Span;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, Token};

use super::filter::FilterPred;
use super::value::Lit;

pub enum IterExpr {
    Var(String),
    Zip(Box<IterExpr>, Box<IterExpr>),
    Product(Box<IterExpr>, Box<IterExpr>),
    Chain(Box<IterExpr>, Box<IterExpr>),
    Enumerate(Box<IterExpr>),
    Filter(Box<IterExpr>, FilterPred),
}

impl IterExpr {
    pub fn named_positions(&self) -> Vec<String> {
        match self {
            IterExpr::Var(name) => vec![name.clone()],
            IterExpr::Zip(a, b) | IterExpr::Product(a, b) => {
                let mut names = a.named_positions();
                names.extend(b.named_positions());
                names
            }
            IterExpr::Chain(a, _) => a.named_positions(),
            IterExpr::Enumerate(inner) => {
                let mut names = vec!["_idx".to_string()];
                names.extend(inner.named_positions());
                names
            }
            IterExpr::Filter(inner, _) => inner.named_positions(),
        }
    }

    pub fn eval(&self, env: &HashMap<String, Vec<Lit>>) -> syn::Result<Vec<Lit>> {
        match self {
            IterExpr::Var(name) => env
                .get(name)
                .ok_or_else(|| {
                    syn::Error::new(
                        Span::call_site(),
                        format!("unknown iterator variable `{name}`"),
                    )
                })
                .cloned(),

            IterExpr::Zip(a, b) => Ok(a
                .eval(env)?
                .into_iter()
                .zip(b.eval(env)?)
                .map(|(a, b)| Lit::Tuple(vec![a, b]))
                .collect()),

            IterExpr::Product(a, b) => {
                let bs = b.eval(env)?;
                Ok(a.eval(env)?
                    .into_iter()
                    .flat_map(|a| {
                        bs.iter()
                            .cloned()
                            .map(move |b| Lit::Tuple(vec![a.clone(), b]))
                    })
                    .collect())
            }

            IterExpr::Chain(a, b) => {
                let a_pos = a.named_positions();
                let b_pos = b.named_positions();
                if a_pos != b_pos {
                    return Err(syn::Error::new(
                        Span::call_site(),
                        "chain: both iterators must have the same element structure",
                    ));
                }
                let mut out = a.eval(env)?;
                out.extend(b.eval(env)?);
                Ok(out)
            }

            IterExpr::Enumerate(inner) => Ok(inner
                .eval(env)?
                .into_iter()
                .enumerate()
                .map(|(i, v)| Lit::Tuple(vec![Lit::Int(i as i64), v]))
                .collect()),

            IterExpr::Filter(inner, pred) => {
                let positions = inner.named_positions();
                let mut out = Vec::new();
                for v in inner.eval(env)? {
                    if pred.eval(&v, &positions)? {
                        out.push(v);
                    }
                }
                Ok(out)
            }
        }
    }
}

impl Parse for IterExpr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut expr = IterExpr::Var(input.parse::<Ident>()?.to_string());
        while input.peek(Token![.]) {
            input.parse::<Token![.]>()?;
            let method: Ident = input.parse()?;
            expr = match method.to_string().as_str() {
                "enumerate" => {
                    let content;
                    syn::parenthesized!(content in input);
                    let _ = content;
                    IterExpr::Enumerate(Box::new(expr))
                }
                "zip" => {
                    let content;
                    syn::parenthesized!(content in input);
                    IterExpr::Zip(Box::new(expr), Box::new(content.parse()?))
                }
                "product" => {
                    let content;
                    syn::parenthesized!(content in input);
                    IterExpr::Product(Box::new(expr), Box::new(content.parse()?))
                }
                "chain" => {
                    let content;
                    syn::parenthesized!(content in input);
                    IterExpr::Chain(Box::new(expr), Box::new(content.parse()?))
                }
                "filter" => {
                    let content;
                    syn::parenthesized!(content in input);
                    IterExpr::Filter(Box::new(expr), content.parse()?)
                }
                other => {
                    return Err(syn::Error::new(
                        method.span(),
                        format!("unknown iterator method `{other}`"),
                    ));
                }
            };
        }
        Ok(expr)
    }
}
