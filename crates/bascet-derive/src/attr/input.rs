use syn::parse::ParseStream;
use syn::{DeriveInput, Ident, LitInt, Token};

pub struct AttrInput {
    pub name: Ident,
    pub range: (usize, usize),
    pub plural: Ident,
}

struct VariadicRange {
    start: usize,
    end: usize,
}

impl syn::parse::Parse for VariadicRange {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        input.parse::<Ident>()?;
        input.parse::<Token![=]>()?;
        let start: LitInt = input.parse()?;
        input.parse::<Token![..=]>()?;
        let end: LitInt = input.parse()?;
        Ok(VariadicRange {
            start: start.base10_parse()?,
            end: end.base10_parse()?,
        })
    }
}

impl AttrInput {
    pub fn from_derive(input: &DeriveInput) -> syn::Result<Self> {
        let range = input
            .attrs
            .iter()
            .find(|a| a.path().is_ident("variadic"))
            .map(|a| a.parse_args::<VariadicRange>().map(|r| (r.start, r.end)))
            .transpose()?
            .unwrap_or((1, 1));

        let plural = input
            .attrs
            .iter()
            .find(|a| a.path().is_ident("plural"))
            .map(|a| a.parse_args::<Ident>())
            .transpose()?
            .unwrap_or_else(|| Ident::new(&format!("{}s", input.ident), input.ident.span()));

        Ok(AttrInput {
            name: input.ident.clone(),
            range,
            plural,
        })
    }
}
