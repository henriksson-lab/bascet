// Generate tuple implementations for each size from 2 to 16
macro_rules! impl_variadic_get {
    () => {
        impl_variadic_get!(@gen 2);
        impl_variadic_get!(@gen 3);
        impl_variadic_get!(@gen 4);
        impl_variadic_get!(@gen 5);
        impl_variadic_get!(@gen 6);
        impl_variadic_get!(@gen 7);
        impl_variadic_get!(@gen 8);
        impl_variadic_get!(@gen 9);
        impl_variadic_get!(@gen 10);
        impl_variadic_get!(@gen 11);
        impl_variadic_get!(@gen 12);
        impl_variadic_get!(@gen 13);
        impl_variadic_get!(@gen 14);
        impl_variadic_get!(@gen 15);
        impl_variadic_get!(@gen 16);
    };
    (@gen $n:tt) => {
        seq_macro::seq!(N in 1..=$n {
            impl<'a, T, #(A~N: crate::Ref<'a, T>,)*> crate::Ref<'a, T> for (#(A~N,)*) {
                type Output = (#(A~N::Output,)*);
                fn get_ref(cell: &'a T) -> Self::Output {
                    (#(A~N::get_ref(cell),)*)
                }
            }
        });
    };
}
