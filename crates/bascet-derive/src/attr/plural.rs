use proc_macro2::{Ident, Literal, TokenStream};
use quote::{format_ident, quote};

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
        out.extend(self.impl_coerce());
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
                    impl crate::Attr for #singular<#n_lit> {
                        type Id = #sing_id;
                    }
                    impl crate::Attr for #plural<#n_lit> {
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
                quote! { inventory::submit! { crate::AttrEntry { id: #sing_id, name: #sing_name } } },
                quote! { inventory::submit! { crate::AttrEntry { id: #plur_id, name: #plur_name } } },
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
                let bounds = idx.iter().map(|i| quote! { S: crate::Ref<#singular<#i>>, });
                let val_types = idx
                    .iter()
                    .map(|i| quote! { <S as crate::Ref<#singular<#i>>>::Value<'a> });
                let get_refs = idx
                    .iter()
                    .map(|i| quote! { crate::Ref::<#singular<#i>>::get_ref(self) });
                quote! {
                    impl<S> crate::Ref<#plural<#n_lit>> for S
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

    fn impl_coerce(&self) -> TokenStream {
        let plural = &self.plural;
        let mut out = TokenStream::new();
        for n in self.start..=self.end {
            let n_lit = Literal::usize_suffixed(n);
            let v_types: Vec<Ident> = (1..=n).map(|i| format_ident!("V{}", i)).collect();
            let v_idents: Vec<Ident> = (1..=n).map(|i| format_ident!("v{}", i)).collect();
            for m in self.start..n {
                let m_lit = Literal::usize_suffixed(m);
                let m_v_types = &v_types[..m];
                let m_v_idents = &v_idents[..m];
                out.extend(quote! {
                    impl<#(#v_types,)*> crate::Coerce<#plural<#n_lit>, #plural<#m_lit>> for (#(#v_types,)*) {
                        type Output = (#(#m_v_types,)*);
                        fn coerce(self) -> Self::Output {
                            let (#(#v_idents,)*) = self;
                            (#(#m_v_idents,)*)
                        }
                    }
                });
            }
        }
        out
    }
}
