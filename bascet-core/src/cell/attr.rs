impl_attr!(Id);
impl_attr!(ReadPair);
impl_attr!(Read);
impl_attr!(QualityPair);
impl_attr!(Quality);
impl_attr!(Umi);
impl_attr!(Metadata);


/// This allows one to call .get with multiple items at once:
// let (Id(id), Read(read)) = cell.get::<(Id<&Vec<u8>>, Read<&Vec<u8>>)>();
// Because of the way the internals of this work this needs to be called for each tuple size. Capped at 16 for now.
impl_tuple_provide!(A, B);
impl_tuple_provide!(A, B, C);
impl_tuple_provide!(A, B, C, D);
impl_tuple_provide!(A, B, C, D, E);
impl_tuple_provide!(A, B, C, D, E, F);
impl_tuple_provide!(A, B, C, D, E, F, G);
impl_tuple_provide!(A, B, C, D, E, F, G, H);
impl_tuple_provide!(A, B, C, D, E, F, G, H, I);
impl_tuple_provide!(A, B, C, D, E, F, G, H, I, J);
impl_tuple_provide!(A, B, C, D, E, F, G, H, I, J, K);
impl_tuple_provide!(A, B, C, D, E, F, G, H, I, J, K, L);
impl_tuple_provide!(A, B, C, D, E, F, G, H, I, J, K, L, M);
impl_tuple_provide!(A, B, C, D, E, F, G, H, I, J, K, L, M, N);
impl_tuple_provide!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O);
impl_tuple_provide!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);
