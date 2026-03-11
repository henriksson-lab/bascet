use std::marker::PhantomData;

pub struct Filter<Attrs, F> {
    pub(crate) predicate: F,
    _phantom: PhantomData<Attrs>,
}

impl<Attrs, F> Filter<Attrs, F> {
    #[inline(always)]
    pub fn new(predicate: F) -> Self {
        Self {
            predicate,
            _phantom: PhantomData,
        }
    }
}

pub struct FilterWithContext<ParsedAttrs, ContextAttrs, F> {
    pub(crate) predicate: F,
    _phantom: PhantomData<(ParsedAttrs, ContextAttrs)>,
}

impl<ParsedAttrs, ContextAttrs, F> FilterWithContext<ParsedAttrs, ContextAttrs, F> {
    #[inline(always)]
    pub fn new(predicate: F) -> Self {
        Self {
            predicate,
            _phantom: PhantomData,
        }
    }
}

bascet_variadic::variadic! {
    #[expand(attrs = 1..=16)]
    impl<I, C, @attrs[A~#](sep=","), F>
        crate::QueryApply<I, C>
        for Filter<(@attrs[A~#](sep=",")), F>
    where
        C: @attrs[crate::Get<A~#>](sep = "+"),
        @attrs[A~#: crate::Attr](sep=","),
        F: Fn((@attrs[&<C as crate::Get<A~#>>::Value](sep=", "))) -> bool,
    {
        #[inline]
        fn apply(&self, _intermediate: &I, context: &C) -> crate::QueryResult {
            if (self.predicate)((@attrs[crate::Get::<A~#>::as_ref(context)](sep=", "))) {
                crate::QueryResult::Keep
            } else {
                crate::QueryResult::Discard
            }
        }
    }
}

bascet_variadic::variadic! {
    #[expand(intermediate = 1..=16, context = 1..=16)]
    impl<I, C, @intermediate[AI~#](sep=","), @context[AC~#](sep=","), F>
        crate::QueryApply<I, C>
        for FilterWithContext<(@intermediate[AI~#](sep=",")), (@context[AC~#](sep=",")), F>
    where
        I: @intermediate[crate::Get<AI~#>](sep = "+"),
        C: @context[crate::Get<AC~#>](sep="+"),
        @intermediate[AI~#: crate::Attr](sep=","),
        @context[AC~#: crate::Attr](sep=","),
        F: Fn(
            (@intermediate[&<I as crate::Get<AI~#>>::Value](sep=", ")),
            (@context[&<C as crate::Get<AC~#>>::Value](sep=", "))
        ) -> bool,
    {
        #[inline]
        fn apply(&self, intermediate: &I, context: &C) -> crate::QueryResult {
            if (self.predicate)(
                (@intermediate[crate::Get::<AI~#>::as_ref(intermediate)](sep=", ")),
                (@context[crate::Get::<AC~#>::as_ref(context)](sep=", "))
            ) {
                crate::QueryResult::Keep
            } else {
                crate::QueryResult::Discard
            }
        }
    }
}

impl<'s, P, D, C, M, Q> crate::Query<'s, P, D, C, M, Q> {
    impl_query!(filter<A, F>(predicate: F) -> Filter<A, F>);
    impl_query!(filter_with_context<PA, CA, F>(predicate: F) -> FilterWithContext<PA, CA, F>);
}
