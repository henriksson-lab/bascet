use proc_macro2::{Punct, Span};
use syn::parse::{Parse, ParseStream};
use syn::{Ident, LitChar, LitInt, LitStr, Token};

use super::value::Lit;

#[derive(Debug, Clone, PartialEq)]
pub enum Cmp {
    Gt,
    Lt,
    Ge,
    Le,
    Eq,
    Ne,
}

#[derive(Debug, Clone)]
pub struct FilterPred {
    pub lhs: Lit,
    pub op: Cmp,
    pub rhs: Lit,
}

impl FilterPred {
    pub fn eval(&self, value: &Lit, positions: &[String]) -> syn::Result<bool> {
        let lhs = Self::resolve(&self.lhs, value, positions)?;
        let rhs = Self::resolve(&self.rhs, value, positions)?;
        match (&lhs, &rhs) {
            (Lit::Int(a), Lit::Int(b)) => Ok(match self.op {
                Cmp::Gt => a > b,
                Cmp::Lt => a < b,
                Cmp::Ge => a >= b,
                Cmp::Le => a <= b,
                Cmp::Eq => a == b,
                Cmp::Ne => a != b,
            }),
            _ => Err(syn::Error::new(
                Span::call_site(),
                "filter comparison requires integer values",
            )),
        }
    }

    fn resolve(operand: &Lit, value: &Lit, positions: &[String]) -> syn::Result<Lit> {
        match operand {
            Lit::Ident(name) => match positions.iter().position(|p| p == name) {
                Some(idx) => match value {
                    Lit::Tuple(vals) => Ok(vals[idx].clone()),
                    v if positions.len() == 1 => Ok(v.clone()),
                    _ => Err(syn::Error::new(
                        Span::call_site(),
                        "cannot index into non-tuple value",
                    )),
                },
                None => Err(syn::Error::new(
                    Span::call_site(),
                    format!("unknown variable `{name}` in filter"),
                )),
            },
            lit => Ok(lit.clone()),
        }
    }
}

impl Lit {
    fn operand(input: ParseStream) -> syn::Result<Self> {
        if input.peek(LitInt) {
            Ok(Self::Int(input.parse::<LitInt>()?.base10_parse()?))
        } else if input.peek(LitChar) {
            Ok(Self::Char(input.parse::<LitChar>()?.value()))
        } else if input.peek(LitStr) {
            Ok(Self::Str(input.parse::<LitStr>()?.value()))
        } else {
            Ok(Self::Ident(input.parse::<Ident>()?.to_string()))
        }
    }
}

impl Cmp {
    fn from_stream(input: ParseStream) -> syn::Result<Self> {
        let first = input.parse::<Punct>()?.as_char();
        match first {
            '>' => {
                if input.peek(Token![=]) {
                    input.parse::<Token![=]>()?;
                    Ok(Self::Ge)
                } else {
                    Ok(Self::Gt)
                }
            }
            '<' => {
                if input.peek(Token![=]) {
                    input.parse::<Token![=]>()?;
                    Ok(Self::Le)
                } else {
                    Ok(Self::Lt)
                }
            }
            '!' => {
                input.parse::<Token![=]>()?;
                Ok(Self::Ne)
            }
            '=' => {
                input.parse::<Token![=]>()?;
                Ok(Self::Eq)
            }
            c => Err(input.error(format!("unexpected operator char `{c}`"))),
        }
    }
}

impl Parse for FilterPred {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let lhs = Lit::operand(input)?;
        let op = Cmp::from_stream(input)?;
        let rhs = Lit::operand(input)?;
        Ok(FilterPred { lhs, op, rhs })
    }
}
