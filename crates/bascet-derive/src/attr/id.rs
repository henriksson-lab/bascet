use std::hash::{Hash, Hasher};

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

pub const PRIME: u64 = 0x00000100000001b3;

pub struct AttrId;

impl AttrId {
    pub fn from_name(name: &str) -> u64 {
        let mut h = fnv::FnvHasher::default();
        name.hash(&mut h);
        h.finish()
    }

    pub fn digits(value: u64) -> TokenStream {
        let digits = (0..16u32).map(|i| {
            let nibble = (value >> (4 * (15 - i))) & 0xF;
            format_ident!("D{}", nibble)
        });
        quote! { (#(bascet_core::set::attr_id::#digits,)*) }
    }

    pub fn expand(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
        let lit = syn::parse_macro_input!(input as syn::LitInt);
        match lit.base10_parse::<u64>() {
            Ok(value) => Self::digits(value).into(),
            Err(error) => error.to_compile_error().into(),
        }
    }
}
