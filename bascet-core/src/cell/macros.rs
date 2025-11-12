macro_rules! impl_attr {
    ($attr_name:ident) => {
        paste::paste!{
            // Define the marker trait
            pub trait Provide $attr_name {
                type Type;
                fn value(&self) -> &Self::Type;
                fn value_mut(&mut self) -> &mut Self::Type;
            }
            // Define the wrapper struct
            pub struct $attr_name<R>(pub R);

            // Implement Get
            impl<[<"'" $attr_name:snake>], T> super::Get<[<"'" $attr_name:snake>], T> for $attr_name<&[<"'" $attr_name:snake>] T::Type>
            where
                T: super::marker::$trait_name,
            {
                fn get(cell: &[<"'" $attr_name:snake>] T) -> Self {
                    $attr_name(cell.value())
                }
            }

            // Implement GetMut
            impl<[<"'" $attr_name:snake>], T> super::GetMut<[<"'" $attr_name:snake>], T> for $attr_name<&[<"'" $attr_name:snake>] mut T::Type>
            where
                T: super::marker::$trait_name,
            {
                fn get_mut(cell: &[<"'" $attr_name:snake>] mut T) -> Self {
                    $attr_name(cell.value_mut())
                }
            }

            // Implement Build
            impl<B> super::builder::Build<B> for $attr_name<()>
            where
                B: super::Builder,
                B::Builds: super::marker::$trait_name,
            {
                type Type = <B::Builds as super::marker::$trait_name>::Type;

                fn build(builder: B, _value: Self::Type) -> B {
                    #[cfg(debug_assertions)]
                    {
                        // TODO: add debug logging
                    }
                    builder
                }
            }
        }
    };
}

macro_rules! impl_tuple_provide {
    ($($ty:ident),+) => {
        impl<'a, T, $($ty: super::Get<'a, T>),+> super::Get<'a, T> for ($($ty,)+) {
            fn get(cell: &'a T) -> Self {
                ($($ty::get(cell),)+)
            }
        }
    };
}