use std::marker::PhantomData;

pub struct GroupRelaxed<Attrs, F> {
    pub(crate) grouping_fn: F,
    _phantom: PhantomData<Attrs>,
}

impl<Attrs, F> GroupRelaxed<Attrs, F> {
    #[inline(always)]
    pub fn new(grouping_fn: F) -> Self {
        Self {
            grouping_fn,
            _phantom: PhantomData,
        }
    }
}

pub struct GroupRelaxedWithContext<ParsedAttrs, ContextAttrs, F> {
    pub(crate) grouping_fn: F,
    _phantom: PhantomData<(ParsedAttrs, ContextAttrs)>,
}

impl<ParsedAttrs, ContextAttrs, F> GroupRelaxedWithContext<ParsedAttrs, ContextAttrs, F> {
    #[inline(always)]
    pub fn new(grouping_fn: F) -> Self {
        Self {
            grouping_fn,
            _phantom: PhantomData,
        }
    }
}

bascet_variadic::variadic! {
    #[expand(attrs = 1..=16)]
    impl<I, C, @attrs[A~#](sep=","), F>
        crate::QueryApply<I, C>
        for GroupRelaxed<(@attrs[A~#](sep=",")), F>
    where
        C: @attrs[crate::Get<A~#>](sep = "+"),
        @attrs[A~#: crate::Attr](sep=","),
        F: Fn((@attrs[&<C as crate::Get<A~#>>::Value](sep=", "))) -> crate::QueryResult,
    {
        #[inline]
        fn apply(&self, _intermediate: &I, context: &C) -> crate::QueryResult {
            (self.grouping_fn)((@attrs[crate::Get::<A~#>::as_ref(context)](sep=", ")))
        }
    }
}

bascet_variadic::variadic! {
    #[expand(intermediate = 1..=16, context = 1..=16)]
    impl<I, C, @intermediate[AI~#](sep=","), @context[AC~#](sep=","), F>
        crate::QueryApply<I, C>
        for GroupRelaxedWithContext<(@intermediate[AI~#](sep=",")), (@context[AC~#](sep=",")), F>
    where
        I: @intermediate[crate::Get<AI~#>](sep = "+"),
        C: @context[crate::Get<AC~#>](sep="+"),
        @intermediate[AI~#: crate::Attr](sep=","),
        @context[AC~#: crate::Attr](sep=","),
        F: Fn(
            (@intermediate[&<I as crate::Get<AI~#>>::Value](sep=", ")),
            (@context[&<C as crate::Get<AC~#>>::Value](sep=", "))
        ) -> crate::QueryResult,
    {
        #[inline]
        fn apply(&self, intermediate: &I, context: &C) -> crate::QueryResult {
            (self.grouping_fn)(
                (@intermediate[crate::Get::<AI~#>::as_ref(intermediate)](sep=", ")),
                (@context[crate::Get::<AC~#>::as_ref(context)](sep=", "))
            )
        }
    }
}

impl<'s, P, D, C, M, Q> crate::Query<'s, P, D, C, M, Q> {
    impl_query!(group_relaxed<A, F>(grouping_fn: F) -> GroupRelaxed<A, F>);
    impl_query!(group_relaxed_with_context<PA, CA, F>(grouping_fn: F) -> GroupRelaxedWithContext<PA, CA, F>);
}
