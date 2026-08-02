use crate::apply::Apply;
use crate::pipeline::batch::Keys;
use crate::set::ops::partition::Compose;

pub type Provides<Stores, A> = <<A as Apply<Stores>>::Produces as Keys>::Output;

pub type Assembled<Stores, A, W> =
    <Stores as Compose<Provides<Stores, A>, W, <A as Apply<Stores>>::Produces>>::Output;
