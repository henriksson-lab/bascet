pub(crate) mod id;
mod input;
mod plural;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

use input::AttrInput;

pub(crate) struct Attr;

impl Attr {
    pub fn derive(input: TokenStream) -> TokenStream {
        let input = parse_macro_input!(input as DeriveInput);
        AttrInput::from_derive(&input)
            .map(AttrInput::emit)
            .unwrap_or_else(|e| e.to_compile_error())
            .into()
    }
}

impl AttrInput {
    fn emit(self) -> proc_macro2::TokenStream {
        let AttrInput {
            name,
            range: (start, end),
            plural,
            ..
        } = self;
        plural::Plural::new(name, plural, start, end).emit()
    }
}
