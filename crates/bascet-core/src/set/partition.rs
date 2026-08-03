use crate::attr::store::Store;
use crate::set::chain::Chain;
use crate::set::member::In;
use crate::set::{Hit, Membership, Miss};

type Forwarded<K, Wants, Provides> =
    <<K as In<Wants>>::Result as Membership>::And<<<K as In<Provides>>::Result as Membership>::Not>;

pub trait Partition<Provides, Wants> {
    type Output;
    fn partition(self) -> Self::Output;
}

pub trait Sift<Provides, Wants, V> {
    type Output;
    fn sift(self) -> Self::Output;
}

impl<Provides, Wants> Partition<Provides, Wants> for () {
    type Output = ();
    fn partition(self) {}
}

impl<S, Rest, Provides, Wants> Partition<Provides, Wants> for (S, Rest)
where
    S: Store,
    <S as Store>::Key: In<Wants> + In<Provides>,
    (S, Rest): Sift<Provides, Wants, Forwarded<<S as Store>::Key, Wants, Provides>>,
{
    type Output =
        <(S, Rest) as Sift<Provides, Wants, Forwarded<<S as Store>::Key, Wants, Provides>>>::Output;
    fn partition(self) -> Self::Output {
        <(S, Rest) as Sift<Provides, Wants, Forwarded<<S as Store>::Key, Wants, Provides>>>::sift(
            self,
        )
    }
}

impl<S, Rest, Provides, Wants> Sift<Provides, Wants, Hit> for (S, Rest)
where
    Rest: Partition<Provides, Wants>,
{
    type Output = (S, <Rest as Partition<Provides, Wants>>::Output);
    fn sift(self) -> Self::Output {
        (self.0, self.1.partition())
    }
}

impl<S, Rest, Provides, Wants> Sift<Provides, Wants, Miss> for (S, Rest)
where
    Rest: Partition<Provides, Wants>,
{
    type Output = <Rest as Partition<Provides, Wants>>::Output;
    fn sift(self) -> Self::Output {
        self.1.partition()
    }
}

pub trait Compose<Provides, Wants, Produced>: 'static {
    type Output;
    fn compose(self, produced: Produced) -> Self::Output;
}

impl<Stores, Provides, Wants, Produced> Compose<Provides, Wants, Produced> for Stores
where
    Stores: Partition<Provides, Wants> + 'static,
    <Stores as Partition<Provides, Wants>>::Output: Chain<Produced>,
{
    type Output = <<Stores as Partition<Provides, Wants>>::Output as Chain<Produced>>::Output;
    fn compose(self, produced: Produced) -> Self::Output {
        <Stores as Partition<Provides, Wants>>::partition(self).chain(produced)
    }
}
