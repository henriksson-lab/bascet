#[macro_export]
macro_rules! impl_attrs {
    ($($attr_name:ident),+ $(,)?) => {
        $(
            pub struct $attr_name;
            impl crate::Attr for $attr_name {}
        )+
    };
}

#[macro_export]
macro_rules! impl_tuple_provide {
    ($($ty:ident),+) => {
        impl<'a, T, $($ty: crate::GetRef<'a, T>),+> crate::GetRef<'a, T> for ($($ty,)+) {
            type Output = ($($ty::Output,)+);
            fn get_ref(cell: &'a T) -> Self::Output {
                ($($ty::get_ref(cell),)+)
            }
        }
    };
}
