mod attrs;
mod derive;

#[proc_macro_derive(Composite, attributes(attrs, backing))]
pub fn derive_composite(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    derive::derive_composite(item)
}

#[proc_macro]
pub fn define_attr(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    attrs::define_attr(input)
}

#[proc_macro]
pub fn define_backing(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    attrs::define_backing(input)
}
