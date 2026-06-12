mod attr;
mod scheduling;

#[proc_macro_derive(Attr, attributes(variadic, plural))]
pub fn derive_attr(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    attr::derive_attr(input)
}

#[proc_macro_derive(Scheduling, attributes(scheduling))]
pub fn derive_scheduling(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    scheduling::derive_scheduling(input)
}
