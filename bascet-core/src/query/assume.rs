use std::marker::PhantomData;

pub struct Assume<Attrs, F> {
    pub(crate) predicate: F,
    pub(crate) message: &'static str,
    _phantom: PhantomData<Attrs>,
}

impl<Attrs, F> Assume<Attrs, F> {
    #[inline(always)]
    pub fn new(predicate: F, message: &'static str) -> Self {
        Self {
            predicate,
            message,
            _phantom: PhantomData,
        }
    }
}

pub struct AssumeWithContext<ParsedAttrs, ContextAttrs, F> {
    pub(crate) predicate: F,
    pub(crate) message: &'static str,
    _phantom: PhantomData<(ParsedAttrs, ContextAttrs)>,
}

impl<ParsedAttrs, ContextAttrs, F> AssumeWithContext<ParsedAttrs, ContextAttrs, F> {
    #[inline(always)]
    pub fn new(predicate: F, message: &'static str) -> Self {
        Self {
            predicate,
            message,
            _phantom: PhantomData,
        }
    }
}

bascet_variadic::variadic! {
    #[expand(attrs = 1..=16)]
    impl<I, C, @attrs[A~#](sep=","), F>
        crate::QueryApply<I, C>
        for Assume<(@attrs[A~#](sep=",")), F>
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
                panic!("{}", self.message);
            }
        }
    }
}

bascet_variadic::variadic! {
    #[expand(intermediate = 1..=16, context = 1..=16)]
    impl<I, C, @intermediate[AI~#](sep=","), @context[AC~#](sep=","), F>
        crate::QueryApply<I, C>
        for AssumeWithContext<(@intermediate[AI~#](sep=",")), (@context[AC~#](sep=",")), F>
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
                panic!("{}", self.message);
            }
        }
    }
}

impl<'s, P, D, C, M, Q> crate::Query<'s, P, D, C, M, Q> {
    impl_query!(assert<A, F>(predicate: F, message: &'static str) -> Assume<A, F>);
    impl_query!(assert_with_context<PA, CA, F>(predicate: F, message: &'static str) -> AssumeWithContext<PA, CA, F>);
}
