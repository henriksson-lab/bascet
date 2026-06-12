mod id;
mod input;
mod plural;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

use input::AttrInput;

pub fn derive_attr(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    emit(input).unwrap_or_else(|e| e.to_compile_error()).into()
}

fn emit(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let AttrInput {
        name,
        range: (start, end),
        plural,
        ..
    } = AttrInput::from_derive(&input)?;
    Ok(plural::Plural::new(name, plural, start, end).emit())
}
