use crate::*;
use std::marker::PhantomData;

pub struct Query<'s, P, D, C, M, Queries> {
    stream: &'s mut Stream<P, D, C, M>,
    queries: Queries,
    _phantom: PhantomData<C>,
}

impl<'s, P, D, C, M, Q> Query<'s, P, D, C, M, Q>
where
    Stream<P, D, C, M>: Next<C>,
    C: Composite,
    Q: QueryApply<C::Intermediate, C>,
{
    pub fn next(&mut self) -> Result<Option<C>, ()> {
        self.stream.next_with(&self.queries)
    }

    pub fn next_into<T>(&mut self) -> Result<Option<T>, ()>
    where
        T: Composite + Default,
        T: FromDirect<T::Attrs, C>,
        T: PushBacking<C, C::Backing>,
        C: TakeBacking<C::Backing>,
    {
        match self.stream.next_with(&self.queries)? {
            Some(context) => {
                let mut composite = T::default();
                composite.from_direct(&context);
                composite.push_backing(context.take_backing());
                Ok(Some(composite))
            }
            None => Ok(None),
        }
    }
}

// Start with empty query
impl<'s, P, D, C, M> Stream<P, D, C, M> {
    pub fn query<QC>(&'s mut self) -> Query<'s, P, D, C, M, ()>
    where
        QC: TEq<C>,
    {
        Query {
            stream: self,
            queries: (),
            _phantom: PhantomData,
        }
    }
}

impl<'s, P, D, C, M, Q> Query<'s, P, D, C, M, Q> {
    pub fn append<Qn>(self, query: Qn) -> Query<'s, P, D, C, M, Q::Query>
    where
        Q: QueryAppend<Qn>,
    {
        Query {
            stream: self.stream,
            queries: self.queries.append(query),
            _phantom: PhantomData,
        }
    }
}

impl<Qn> QueryAppend<Qn> for () {
    type Query = (Qn,);
    fn append(self, query: Qn) -> Self::Query {
        (query,)
    }
}

impl<Q0, Qn> QueryAppend<Qn> for (Q0,) {
    type Query = (Q0, Qn);
    fn append(self, query: Qn) -> Self::Query {
        (self.0, query)
    }
}

bascet_variadic::variadic! {
    #[expand(n = 2..=16)]
    impl<@n[Q~#](sep=","), Qn> QueryAppend<Qn> for (@n[Q~#](sep=",")) {
        type Query = (@n[Q~#](sep=","), Qn);
        fn append(self, query: Qn) -> Self::Query {
            (@n[self.#](sep=","), query)
        }
    }
}

// Empty tuple always emits
impl<I, C> crate::QueryApply<I, C> for () {
    #[inline(always)]
    fn apply(&self, _intermediate: &I, _context: &C) -> QueryResult {
        QueryResult::Emit
    }
}

// Single query
impl<I, C, Q0> crate::QueryApply<I, C> for (Q0,)
where
    Q0: crate::QueryApply<I, C>,
{
    #[inline]
    fn apply(&self, intermediate: &I, context: &C) -> crate::QueryResult {
        self.0.apply(intermediate, context)
    }
}

bascet_variadic::variadic! {
    #[expand(n = 2..=16)]
    impl<I, C, @n[Q~#](sep=",")> crate::QueryApply<I, C> for (@n[Q~#](sep=","))
    where
        @n[Q~#: crate::QueryApply<I, C>](sep=","),
    {
        #[inline]
        fn apply(&self, intermediate: &I, context: &C) -> crate::QueryResult {
            let mut result = crate::QueryResult::Keep;
            @n[
                match self.#.apply(intermediate, context) {
                    crate::QueryResult::Discard => return crate::QueryResult::Discard,
                    crate::QueryResult::Emit => result = crate::QueryResult::Emit,
                    crate::QueryResult::Keep => { }
                }
            ]
            result
        }
    }
}
