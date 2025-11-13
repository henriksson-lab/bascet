macro_rules! impl_attrs {
    ($($attr_name:ident),+ $(,)?) => {
        $(
            paste::paste!{
                pub trait [<Provide $attr_name>] {
                    type Type;
                    fn as_ref(&self) -> &Self::Type;
                    fn as_mut(&mut self) -> &mut Self::Type;
                }

                pub struct $attr_name;

                impl<[<"'" $attr_name:snake>], T> super::traits::GetRef<[<"'" $attr_name:snake>], T> for $attr_name
                where
                    T: [<Provide $attr_name>],
                    T::Type: [<"'" $attr_name:snake>],
                {
                    type Output = &[<"'" $attr_name:snake>] T::Type;
                    fn get_ref(cell: &[<"'" $attr_name:snake>] T) -> Self::Output {
                        cell.as_ref()
                    }
                }

                impl<[<"'" $attr_name:snake>], T> super::traits::GetMut<[<"'" $attr_name:snake>], T> for $attr_name
                where
                    T: [<Provide $attr_name>],
                    T::Type: [<"'" $attr_name:snake>],
                {
                    type Output = &[<"'" $attr_name:snake>] mut T::Type;
                    fn get_mut(cell: &[<"'" $attr_name:snake>] mut T) -> Self::Output {
                        cell.as_mut()
                    }
                }
            }
        )+
    };
}
macro_rules! impl_tuple_provide {
    ($($ty:ident),+) => {
        impl<'a, T, $($ty: super::traits::GetRef<'a, T>),+> super::traits::GetRef<'a, T> for ($($ty,)+) {
            type Output = ($($ty::Output,)+);
            fn get_ref(cell: &'a T) -> Self::Output {
                ($($ty::get_ref(cell),)+)
            }
        }
    };
}
