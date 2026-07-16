mod attr;

#[proc_macro_derive(Attr, attributes(variadic, plural))]
pub fn derive_attr(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    attr::Attr::derive(input)
}

#[proc_macro]
pub fn attr_id(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    attr::id::AttrId::expand(input)
}
