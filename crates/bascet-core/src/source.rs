use std::future::Future;
use std::ops::Range;

use crate::layer::Layer;
use crate::owned::Owned;
use crate::set::Set;
use crate::stage::{Mode, Output, Strategy};

pub enum Pull {
    Next,
    Read(Range<u64>),
    Shutdown,
}

pub trait Source: Layer + Owned<Mode, Value = Mode> + Owned<Strategy, Value = Strategy> {
    type Output;

    fn produce<W: Set>(&mut self, req: Pull) -> impl Future<Output = Output<Self::Output>> + Send;
}
