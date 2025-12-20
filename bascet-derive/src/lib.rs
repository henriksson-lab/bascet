mod attrs;
mod composite;
mod runtime;

#[proc_macro_derive(Composite, attributes(bascet, collection))]
pub fn derive_composite(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    composite::derive_composite(item)
}

#[proc_macro]
pub fn define_attr(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    attrs::define_attr(input)
}

#[proc_macro]
pub fn define_backing(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    attrs::define_backing(input)
}

#[proc_macro_derive(Budget, attributes(threads, mem))]
pub fn derive_budget(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    runtime::derive_budget(item)
}
