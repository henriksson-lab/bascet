/// # Syntax
/// ```ignore
/// impl_query!(method_name<Generics>(param1: Type1, param2: Type2, ...) -> QueryType);
/// ```
///
/// # Example
/// ```ignore
/// impl<'s, P, D, M, C, I, Q> Query<'s, P, D, M, C, I, Q>
/// where
///     P: Context<M>,
/// {
///     impl_query!(filter<A, F>(predicate: F) -> Filter<A, F>);
///     impl_query!(assume<A, F>(predicate: F, message: &'static str) -> Assume<A, F>);
/// }
/// ```
#[macro_export]
macro_rules! impl_query {
    ($method_name:ident<$($generic:ident),*>($($param:ident: $param_ty:ty),* $(,)?) -> $query_type:ty) => {
        pub fn $method_name<$($generic),*>(self, $($param: $param_ty),*)
            -> crate::Query<'s, P, D, C, M, Q::Query>
        where
            Q: crate::QueryAppend<$query_type>,
        {
            self.append(<$query_type>::new($($param),*))
        }
    };
}
