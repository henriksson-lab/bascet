use std::collections::HashMap;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, Token};
use crate::ast::Value;

pub enum IterExpr {
    Var(String),
    Zip(Box<IterExpr>, Box<IterExpr>),
    Product(Box<IterExpr>, Box<IterExpr>),
    Chain(Box<IterExpr>, Box<IterExpr>),
    Enumerate(Box<IterExpr>),
}

impl IterExpr {
    pub fn eval(&self, env: &HashMap<String, Vec<Value>>) -> Vec<Value> {
        match self {
            IterExpr::Var(name) => env
                .get(name)
                .unwrap_or_else(|| panic!("unknown iterator variable `{name}`"))
                .clone(),

            IterExpr::Zip(a, b) => a
                .eval(env)
                .into_iter()
                .zip(b.eval(env))
                .map(|(a, b)| Value::Tuple(vec![a, b]))
                .collect(),

            IterExpr::Product(a, b) => {
                let bs = b.eval(env);
                a.eval(env)
                    .into_iter()
                    .flat_map(|a| bs.iter().cloned().map(move |b| Value::Tuple(vec![a.clone(), b])))
                    .collect()
            }

            IterExpr::Chain(a, b) => {
                let mut out = a.eval(env);
                out.extend(b.eval(env));
                out
            }

            IterExpr::Enumerate(inner) => inner
                .eval(env)
                .into_iter()
                .enumerate()
                .map(|(i, v)| Value::Tuple(vec![Value::Int(i as i64), v]))
                .collect(),
        }
    }
}

impl Parse for IterExpr {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut expr = IterExpr::Var(input.parse::<Ident>()?.to_string());
        while input.peek(Token![.]) {
            input.parse::<Token![.]>()?;
            let method: Ident = input.parse()?;
            let content;
            expr = match method.to_string().as_str() {
                "enumerate" => {
                    syn::parenthesized!(content in input);
                    let _ = content;
                    IterExpr::Enumerate(Box::new(expr))
                }
                "zip" => {
                    syn::parenthesized!(content in input);
                    IterExpr::Zip(Box::new(expr), Box::new(content.parse()?))
                }
                "product" => {
                    syn::parenthesized!(content in input);
                    IterExpr::Product(Box::new(expr), Box::new(content.parse()?))
                }
                "chain" => {
                    syn::parenthesized!(content in input);
                    IterExpr::Chain(Box::new(expr), Box::new(content.parse()?))
                }
                other => return Err(syn::Error::new(method.span(), format!("unknown iterator method `{other}`"))),
            };
        }
        Ok(expr)
    }
}
