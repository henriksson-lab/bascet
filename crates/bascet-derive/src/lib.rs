mod attrs;

#[proc_macro]
pub fn define_attr(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    attrs::define_attr(input)
}
