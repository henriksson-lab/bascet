use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Ident, Token};

pub fn define_attr(input: TokenStream) -> TokenStream {
    let parser = syn::punctuated::Punctuated::<Ident, Token![,]>::parse_terminated;
    let idents = parse_macro_input!(input with parser);

    let impls = idents.iter().map(|name| {
        quote! {
            pub struct #name;
            impl crate::Attr for #name {}
        }
    });

    TokenStream::from(quote! { #(#impls)* })
}

pub fn define_backing(input: TokenStream) -> TokenStream {
    let parser = syn::punctuated::Punctuated::<Ident, Token![,]>::parse_terminated;
    let idents = parse_macro_input!(input with parser);

    let impls = idents.iter().map(|name| {
        quote! {
            pub struct #name;
            impl crate::Backing for #name {}
        }
    });

    TokenStream::from(quote! { #(#impls)* })
}
