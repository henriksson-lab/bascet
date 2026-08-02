use crate::attr::Attr;
use crate::attr::AttrEq;
use crate::attr::store::Store;
use crate::set::{Hit, Matches, Miss};

type Matched<S, A> = <<<S as Store>::Key as Attr>::Id as Matches<<A as Attr>::Id>>::Result;

#[diagnostic::on_unimplemented(
    message = "no store provides attribute `{A}`",
    label = "this batch has no store keyed to `{A}`",
    note = "a layer can only read attributes its upstream provides"
)]
pub trait Holds<A> {
    type Held: Store;
    fn store(&self) -> &Self::Held;
}

pub trait Seek<A, Found> {
    type Held: Store;
    fn store(&self) -> &Self::Held;
}

impl<A: Attr, S: Store, Rest> Holds<A> for (S, Rest)
where
    <S::Key as Attr>::Id: Matches<<A as Attr>::Id>,
    (S, Rest): Seek<A, Matched<S, A>>,
{
    type Held = <(S, Rest) as Seek<A, Matched<S, A>>>::Held;
    fn store(&self) -> &Self::Held {
        <(S, Rest) as Seek<A, Matched<S, A>>>::store(self)
    }
}

impl<A, S: Store, Rest> Seek<A, Hit> for (S, Rest)
where
    <S as Store>::Key: AttrEq<A>,
{
    type Held = S;
    fn store(&self) -> &S {
        &self.0
    }
}

impl<A, S: Store, Rest> Seek<A, Miss> for (S, Rest)
where
    Rest: Holds<A>,
{
    type Held = <Rest as Holds<A>>::Held;
    fn store(&self) -> &Self::Held {
        <Rest as Holds<A>>::store(&self.1)
    }
}
