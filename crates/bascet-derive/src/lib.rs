mod attr;
mod schedule;

#[proc_macro_derive(Attr, attributes(variadic, plural))]
pub fn derive_attr(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    attr::Attr::derive(input)
}

#[proc_macro_derive(Schedule, attributes(schedule))]
pub fn derive_schedule(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    schedule::Schedule::derive(input)
}
