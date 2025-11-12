use proc_macro::TokenStream;

mod provide_cell;

#[proc_macro_attribute]
pub fn cell(attrs: TokenStream, item: TokenStream) -> TokenStream {
    provide_cell::attrs(attrs, item)
}