mod derive;

#[proc_macro_derive(Composite, attributes(attrs))]
pub fn derive_composite(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    derive::derive_composite(item)
}
