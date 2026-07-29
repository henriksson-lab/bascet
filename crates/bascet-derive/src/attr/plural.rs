use proc_macro2::{Ident, Literal, TokenStream};
use quote::quote;

pub struct Plural {
    singular: Ident,
    plural: Ident,
    singular_id: u64,
    plural_id: u64,
    start: usize,
    end: usize,
}

impl Plural {
    pub fn new(singular: Ident, plural: Ident, start: usize, end: usize) -> Self {
        Plural {
            singular_id: super::id::AttrId::from_name(&singular.to_string()),
            plural_id: super::id::AttrId::from_name(&plural.to_string()),
            singular,
            plural,
            start,
            end,
        }
    }

    pub fn emit(&self) -> TokenStream {
        let plural = &self.plural;
        let mut out = quote! { pub struct #plural<const N: usize = 1>; };
        out.extend(self.impl_attr());
        out.extend(self.impl_display());
        out.extend(self.impl_inventory());
        out.extend(self.impl_ref());
        out
    }

    fn impl_attr(&self) -> TokenStream {
        let singular = &self.singular;
        let plural = &self.plural;
        (self.start..=self.end)
            .map(|n| {
                let n_lit = Literal::usize_suffixed(n);
                let sing_id = super::id::AttrId::digits(
                    self.singular_id ^ (n as u64).wrapping_mul(super::id::PRIME),
                );
                let plur_id = super::id::AttrId::digits(
                    self.plural_id ^ (n as u64).wrapping_mul(super::id::PRIME),
                );
                quote! {
                    impl bascet_core::Attr for #singular<#n_lit> {
                        type Id = #sing_id;
                    }
                    impl bascet_core::Attr for #plural<#n_lit> {
                        type Id = #plur_id;
                    }
                }
            })
            .collect()
    }

    fn impl_display(&self) -> TokenStream {
        let plural = &self.plural;
        let fmt = format!("{}[{{}}]", plural);
        quote! {
            impl<const N: usize> std::fmt::Display for #plural<N> {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    write!(f, #fmt, N)
                }
            }
        }
    }

    fn impl_inventory(&self) -> TokenStream {
        let singular = &self.singular;
        let plural = &self.plural;
        (self.start..=self.end).flat_map(|n| {
            let sing_name = format!("{}<{}>", singular, n);
            let plur_name = format!("{}<{}>", plural, n);
            let sing_id = Literal::u64_unsuffixed(
                self.singular_id ^ (n as u64).wrapping_mul(super::id::PRIME),
            );
            let plur_id = Literal::u64_unsuffixed(
                self.plural_id ^ (n as u64).wrapping_mul(super::id::PRIME),
            );
            [
                quote! { inventory::submit! { bascet_core::AttrEntry { id: #sing_id, name: #sing_name } } },
                quote! { inventory::submit! { bascet_core::AttrEntry { id: #plur_id, name: #plur_name } } },
            ]
        }).collect()
    }

    fn impl_ref(&self) -> TokenStream {
        let singular = &self.singular;
        let plural = &self.plural;
        (self.start..=self.end)
            .map(|n| {
                let n_lit = Literal::usize_suffixed(n);
                let idx: Vec<_> = (1..=n).map(Literal::usize_suffixed).collect();
                let bounds = idx.iter().map(|i| quote! { S: bascet_core::Ref<#singular<#i>>, });
                let val_types = idx
                    .iter()
                    .map(|i| quote! { <S as bascet_core::Ref<#singular<#i>>>::Value<'a> });
                let get_refs = idx
                    .iter()
                    .map(|i| quote! { bascet_core::Ref::<#singular<#i>>::get_ref(self) });
                quote! {
                    impl<S> bascet_core::Ref<#plural<#n_lit>> for S
                    where #(#bounds)*
                    {
                        type Value<'a> = (#(#val_types,)*) where S: 'a;
                        fn get_ref<'a>(&'a self) -> Self::Value<'a> {
                            (#(#get_refs,)*)
                        }
                    }
                }
            })
            .collect()
    }

}
