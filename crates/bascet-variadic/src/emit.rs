use crate::ast::{Lit, Pattern};
use proc_macro2::{Delimiter, Group, TokenStream, TokenTree};
use quote::quote;
use std::collections::HashMap;
use syn::parse::ParseStream;
use syn::{Ident, Token};

pub struct Transcriber;

struct Frame<'a> {
    bindings: &'a HashMap<String, Lit>,
}

impl Transcriber {
    pub fn emit(
        ts: TokenStream,
        pattern: &Pattern,
        values: Vec<Lit>,
    ) -> syn::Result<TokenStream> {
        let mut out = TokenStream::new();
        for val in values {
            let mut bindings = HashMap::new();
            pattern.bind(&val, &mut bindings)?;
            out.extend(
                Frame {
                    bindings: &bindings,
                }
                .transcribe(ts.clone(), None)?,
            );
        }
        Ok(out)
    }
}

impl<'a> Frame<'a> {
    fn transcribe(&self, ts: TokenStream, index: Option<&Lit>) -> syn::Result<TokenStream> {
        let tokens: Vec<TokenTree> = ts.into_iter().collect();
        let mut out = TokenStream::new();
        let mut i = 0;
        while i < tokens.len() {
            if let Some((expanded, consumed)) = self.expand(&tokens, i)? {
                out.extend(expanded);
                i += consumed;
                continue;
            }
            if let Some((expanded, consumed)) = self.concat(&tokens, i, index) {
                out.extend(expanded);
                i += consumed;
                continue;
            }
            match &tokens[i] {
                TokenTree::Punct(p) if p.as_char() == '#' && index.is_some() => {
                    out.extend(index.unwrap().as_token()?);
                    i += 1;
                }
                TokenTree::Ident(ident) => {
                    if let Some(val) = self.bindings.get(&ident.to_string()) {
                        out.extend(val.as_token()?);
                    } else {
                        out.extend(quote! { #ident });
                    }
                    i += 1;
                }
                TokenTree::Group(group) => {
                    let inner = self.transcribe(group.stream(), index)?;
                    out.extend(std::iter::once(TokenTree::Group(Group::new(
                        group.delimiter(),
                        inner,
                    ))));
                    i += 1;
                }
                other => {
                    out.extend(std::iter::once(other.clone()));
                    i += 1;
                }
            }
        }
        Ok(out)
    }

    fn concat(
        &self,
        tokens: &[TokenTree],
        i: usize,
        index: Option<&Lit>,
    ) -> Option<(TokenStream, usize)> {
        let TokenTree::Ident(ident) = tokens.get(i)? else {
            return None;
        };
        let TokenTree::Punct(p) = tokens.get(i + 1)? else {
            return None;
        };
        if p.as_char() != '~' {
            return None;
        }
        let value = match tokens.get(i + 2)? {
            TokenTree::Punct(p) if p.as_char() == '#' => match index? {
                Lit::Int(n) => *n,
                _ => return None,
            },
            TokenTree::Ident(var) => match self.bindings.get(&var.to_string()) {
                Some(Lit::Int(n)) => *n,
                _ => return None,
            },
            _ => return None,
        };
        let new_ident = proc_macro2::Ident::new(&format!("{}{}", ident, value), ident.span());
        Some((quote! { #new_ident }, 3))
    }

    fn expand(&self, tokens: &[TokenTree], i: usize) -> syn::Result<Option<(TokenStream, usize)>> {
        let mut j = i;

        let Some(TokenTree::Punct(p)) = tokens.get(j) else {
            return Ok(None);
        };
        if p.as_char() != '@' {
            return Ok(None);
        }
        j += 1;

        let Some(TokenTree::Ident(var_ident)) = tokens.get(j) else {
            return Ok(None);
        };
        let var_name = var_ident.to_string();
        let Some(Lit::Int(n)) = self.bindings.get(&var_name) else {
            return Ok(None);
        };
        let n = *n;
        j += 1;

        let Some(TokenTree::Group(bracket)) = tokens.get(j) else {
            return Ok(None);
        };
        if bracket.delimiter() != Delimiter::Bracket {
            return Ok(None);
        }
        let template = bracket.stream();
        j += 1;

        let sep = if let Some(TokenTree::Group(g)) = tokens.get(j) {
            if g.delimiter() == Delimiter::Parenthesis {
                let parser = |input: ParseStream| -> syn::Result<String> {
                    input.parse::<Ident>()?;
                    input.parse::<Token![=]>()?;
                    Ok(input.parse::<syn::LitStr>()?.value())
                };
                if let Ok(s) = syn::parse::Parser::parse2(parser, g.stream()) {
                    j += 1;
                    Some(s)
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        let consumed = j - i;

        let iter_values: Vec<Lit> = (0..n).map(Lit::Int).collect();

        let mut out = TokenStream::new();
        for (k, val) in iter_values.iter().enumerate() {
            if k > 0 {
                if let Some(ref s) = sep {
                    out.extend(s.parse::<TokenStream>().unwrap_or_default());
                }
            }
            out.extend(self.transcribe(template.clone(), Some(val))?);
        }

        Ok(Some((out, consumed)))
    }
}
