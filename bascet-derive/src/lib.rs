use proc_macro::TokenStream;

mod derive_cell;

macro_rules! derive_provide {
    ($trait_name:ident) => {
        paste::paste! {
            #[proc_macro_derive($trait_name, attributes(cell))]
            pub fn [<derive_ $trait_name:snake>](input: TokenStream) -> TokenStream {
                derive_cell::derive_provide_impl(input, stringify!($trait_name))
            }
        }
    };
}

derive_provide!(ProvideID);
derive_provide!(ProvideReadPair);
derive_provide!(ProvideRead);
derive_provide!(ProvideQualityPair);
derive_provide!(ProvideQuality);
derive_provide!(ProvideUMI);
derive_provide!(ProvideMetadata);

// #[proc_macro_derive(UsesManagedRef, attributes(cell))]
// pub fn derive_use_managed_ref(input: TokenStream) -> TokenStream {
//     derive_cell::derive_use_managed_ref_impl(input)
// }