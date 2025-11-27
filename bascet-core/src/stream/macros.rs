// Generate ParseFrom tuple implementations for each size from 2 to 16
macro_rules! impl_variadic_from_parsed {
    () => {
        impl_variadic_from_parsed!(@gen 2);
        impl_variadic_from_parsed!(@gen 3);
        impl_variadic_from_parsed!(@gen 4);
        impl_variadic_from_parsed!(@gen 5);
        impl_variadic_from_parsed!(@gen 6);
        impl_variadic_from_parsed!(@gen 7);
        impl_variadic_from_parsed!(@gen 8);
        impl_variadic_from_parsed!(@gen 9);
        impl_variadic_from_parsed!(@gen 10);
        impl_variadic_from_parsed!(@gen 11);
        impl_variadic_from_parsed!(@gen 12);
        impl_variadic_from_parsed!(@gen 13);
        impl_variadic_from_parsed!(@gen 14);
        impl_variadic_from_parsed!(@gen 15);
        impl_variadic_from_parsed!(@gen 16);
    };
    (@gen $n:tt) => {
        seq_macro::seq!(N in 1..=$n {
            impl<T, S, #(A~N,)*> FromParsed<(#(A~N,)*), S> for T
            where
                T: #(crate::Get<A~N> + FromParsed<A~N, S> +)*,
                S: #(crate::Get<A~N> +)*,
                #(A~N: crate::Attr,)*
            {
                fn from(&mut self, source: &S) {
                    #(
                        <T as FromParsed<A~N, S>>::from(self, &source);
                    )*
                }
            }
        });
    };
}
