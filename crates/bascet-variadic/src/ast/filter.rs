use proc_macro2::{Punct, Span};
use syn::parse::{Parse, ParseStream};
use syn::{Ident, LitChar, LitInt, LitStr};

use super::value::Lit;

#[derive(Debug, Clone, PartialEq)]
pub enum CmpOp {
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
    pub op: CmpOp,
    pub rhs: Lit,
}

impl FilterPred {
    pub fn eval(&self, value: &Lit, positions: &[String]) -> syn::Result<bool> {
        let lhs = Self::resolve(&self.lhs, value, positions)?;
        let rhs = Self::resolve(&self.rhs, value, positions)?;
        match (&lhs, &rhs) {
            (Lit::Int(a), Lit::Int(b)) => Ok(match self.op {
                CmpOp::Gt => a > b,
                CmpOp::Lt => a < b,
                CmpOp::Ge => a >= b,
                CmpOp::Le => a <= b,
                CmpOp::Eq => a == b,
                CmpOp::Ne => a != b,
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

fn parse_operand(input: ParseStream) -> syn::Result<Lit> {
    if input.peek(LitInt) {
        Ok(Lit::Int(input.parse::<LitInt>()?.base10_parse()?))
    } else if input.peek(LitChar) {
        Ok(Lit::Char(input.parse::<LitChar>()?.value()))
    } else if input.peek(LitStr) {
        Ok(Lit::Str(input.parse::<LitStr>()?.value()))
    } else {
        Ok(Lit::Ident(input.parse::<Ident>()?.to_string()))
    }
}

fn parse_punct(input: ParseStream) -> syn::Result<char> {
    Ok(input.parse::<Punct>()?.as_char())
}

fn try_parse_eq(input: ParseStream) -> bool {
    if input.peek(syn::Token![=]) {
        let _ = input.parse::<syn::Token![=]>();
        true
    } else {
        false
    }
}

impl Parse for FilterPred {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let lhs = parse_operand(input)?;
        let first = parse_punct(input)?;
        let op = match first {
            '>' => {
                if try_parse_eq(input) {
                    CmpOp::Ge
                } else {
                    CmpOp::Gt
                }
            }
            '<' => {
                if try_parse_eq(input) {
                    CmpOp::Le
                } else {
                    CmpOp::Lt
                }
            }
            '!' => {
                parse_punct(input)?;
                CmpOp::Ne
            }
            '=' => {
                parse_punct(input)?;
                CmpOp::Eq
            }
            c => return Err(input.error(format!("unexpected operator char `{c}`"))),
        };
        let rhs = parse_operand(input)?;
        Ok(FilterPred { lhs, op, rhs })
    }
}
