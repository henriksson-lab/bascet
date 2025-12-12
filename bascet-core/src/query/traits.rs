pub enum QueryResult {
    Keep,
    Emit,
    Discard,
}

pub trait QueryApply<I, C> {
    fn apply(&self, intermediate: &I, context: &C) -> QueryResult;
}

pub trait QueryAppend<Qn> {
    type Query;
    fn append(self, query: Qn) -> Self::Query;
}
