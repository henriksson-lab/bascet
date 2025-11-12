pub trait Cell: Sized {
    type Builder: super::Builder<Product = Self>;
    fn builder() -> Self::Builder;

    fn get<G: super::Get<Self>>(&self) -> G {
        G::get(self)
    }
}
pub struct ManagedRef<T>(pub T);

impl<T> super::Get<T> for ManagedRef<T::Ref>
where
    T: super::marker::UseManagedRef,
{
    fn get(cell: &T) -> Self {
        ManagedRef(<T as super::marker::UseManagedRef>::value(cell))
    }
}

pub struct ID<T>(pub T);

impl<T, U> super::Get<T> for ID<U>
where
    T: super::marker::ProvideID,
    U: From<T::Type>,
{
    fn get(cell: &T) -> Self {
        ID(U::from(<T as super::marker::ProvideID>::value(cell)))
    }
}

pub struct ReadPair<T>(pub T);

impl<T, U> super::Get<T> for ReadPair<U>
where
    T: super::marker::ProvideReadPair,
    U: From<T::Type>,
{
    fn get(cell: &T) -> Self {
        ReadPair(U::from(<T as super::marker::ProvideReadPair>::value(cell)))
    }
}

pub struct Read<T>(pub T);

impl<T, U> super::Get<T> for Read<U>
where
    T: super::marker::ProvideRead,
    U: From<T::Type>,
{
    fn get(cell: &T) -> Self {
        Read(U::from(<T as super::marker::ProvideRead>::value(cell)))
    }
}

pub struct QualityPair<T>(pub T);

impl<T, U> super::Get<T> for QualityPair<U>
where
    T: super::marker::ProvideQualityPair,
    U: From<T::Type>,
{
    fn get(cell: &T) -> Self {
        QualityPair(U::from(<T as super::marker::ProvideQualityPair>::value(cell)))
    }
}

pub struct Quality<T>(pub T);

impl<T, U> super::Get<T> for Quality<U>
where
    T: super::marker::ProvideQuality,
    U: From<T::Type>,
{
    fn get(cell: &T) -> Self {
        Quality(U::from(<T as super::marker::ProvideQuality>::value(cell)))
    }
}

pub struct UMI<T>(pub T);

impl<T, U> super::Get<T> for UMI<U>
where
    T: super::marker::ProvideUMI,
    U: From<T::Type>,
{
    fn get(cell: &T) -> Self {
        UMI(U::from(<T as super::marker::ProvideUMI>::value(cell)))
    }
}

pub struct Metadata<T>(pub T);

impl<T, U> super::Get<T> for Metadata<U>
where
    T: super::marker::ProvideMetadata,
    U: From<T::Type>,
{
    fn get(cell: &T) -> Self {
        Metadata(U::from(<T as super::marker::ProvideMetadata>::value(cell)))
    }
}



macro_rules! impl_tuple_get {
    ($($ty:ident),+) => {
        impl<T, $($ty: super::Get<T>),+> super::Get<T> for ($($ty,)+) {
            fn get(cell: &T) -> Self {
                ($($ty::get(cell),)+)
            }
        }
    };
}

// NOTE: implements tuple getters for up to 16 tuple elements. I do not think more are needed
impl_tuple_get!(A, B);
impl_tuple_get!(A, B, C);
impl_tuple_get!(A, B, C, D);
impl_tuple_get!(A, B, C, D, E);
impl_tuple_get!(A, B, C, D, E, F);
impl_tuple_get!(A, B, C, D, E, F, G);
impl_tuple_get!(A, B, C, D, E, F, G, H);
impl_tuple_get!(A, B, C, D, E, F, G, H, I);
impl_tuple_get!(A, B, C, D, E, F, G, H, I, J);
impl_tuple_get!(A, B, C, D, E, F, G, H, I, J, K);
impl_tuple_get!(A, B, C, D, E, F, G, H, I, J, K, L);
impl_tuple_get!(A, B, C, D, E, F, G, H, I, J, K, L, M);
impl_tuple_get!(A, B, C, D, E, F, G, H, I, J, K, L, M, N);
impl_tuple_get!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O);
impl_tuple_get!(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P);