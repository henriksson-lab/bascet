use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Ident, Token, parse_macro_input};

pub fn define_attr(input: TokenStream) -> TokenStream {
    let parser = syn::punctuated::Punctuated::<Ident, Token![,]>::parse_terminated;
    let idents = parse_macro_input!(input with parser);

    let impls: TokenStream2 = idents
        .iter()
        .map(|name| {
            quote! {
                pub struct #name;
                impl crate::Attr for #name {
                    const ID: ::std::any::TypeId = ::std::any::TypeId::of::<#name>();
                }
            }
        })
        .collect();

    impls.into()
}
