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

                impl<'a, T> super::core::GetRef<'a, T> for $attr_name
                where
                    T: [<Provide $attr_name>],
                    T::Type: 'a,
                {
                    type Output = &'a T::Type;
                    fn get_ref(core: &'a T) -> Self::Output {
                        core.as_ref()
                    }
                }

                impl<'a, T> super::core::GetMut<'a, T> for $attr_name
                where
                    T: [<Provide $attr_name>],
                    T::Type: 'a,
                {
                    type Output = &'a mut T::Type;
                    fn get_mut(core: &'a mut T) -> Self::Output {
                        core.as_mut()
                    }
                }
            }
        )+
    };
}
macro_rules! impl_tuple_provide {
    ($($ty:ident),+) => {
        impl<'a, T, $($ty: super::core::GetRef<'a, T>),+> super::core::GetRef<'a, T> for ($($ty,)+) {
            type Output = ($($ty::Output,)+);
            fn get_ref(core: &'a T) -> Self::Output {
                ($($ty::get_ref(core),)+)
            }
        }
    };
}
