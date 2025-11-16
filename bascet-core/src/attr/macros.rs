macro_rules! impl_attrs {
    ($($attr_name:ident),+ $(,)?) => {
        $(
            pub struct $attr_name;
            impl crate::Attr for $attr_name {}
        )+
    };
}

macro_rules! impl_tuple_provide {
    ($($ty:ident),+) => {
        impl<'a, T, $($ty: crate::Ref<'a, T>),+> crate::Ref<'a, T> for ($($ty,)+) {
            type Output = ($($ty::Output,)+);
            fn get_ref(cell: &'a T) -> Self::Output {
                ($($ty::get_ref(cell),)+)
            }
        }
    };
}
