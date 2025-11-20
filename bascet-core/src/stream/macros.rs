// Generate ParseFrom tuple implementations for each size from 2 to 16
macro_rules! impl_variadic_parse_from {
    () => {
        impl_variadic_parse_from!(@gen 2);
        impl_variadic_parse_from!(@gen 3);
        impl_variadic_parse_from!(@gen 4);
        impl_variadic_parse_from!(@gen 5);
        impl_variadic_parse_from!(@gen 6);
        impl_variadic_parse_from!(@gen 7);
        impl_variadic_parse_from!(@gen 8);
        impl_variadic_parse_from!(@gen 9);
        impl_variadic_parse_from!(@gen 10);
        impl_variadic_parse_from!(@gen 11);
        impl_variadic_parse_from!(@gen 12);
        impl_variadic_parse_from!(@gen 13);
        impl_variadic_parse_from!(@gen 14);
        impl_variadic_parse_from!(@gen 15);
        impl_variadic_parse_from!(@gen 16);
    };
    (@gen $n:tt) => {
        seq_macro::seq!(N in 1..=$n {
            impl<T, S, #(A~N,)*> ParseFrom<(#(A~N,)*), S> for T
            where
                T: #(crate::Get<A~N> +)*,
                S: #(crate::Get<A~N, Value = <T as crate::Get<A~N>>::Value> +)*,
                #(A~N: crate::Attr,)*
                #(<T as crate::Get<A~N>>::Value: Copy,)*
            {
                fn from(&mut self, source: &S) {
                    #(
                        crate::Put::put(crate::Tagged::<A~N, _>::new(*<S as crate::Get<A~N>>::attr(source)), self);
                    )*
                }
            }
        });
    };
}