pub mod auto;

pub use auto::Auto;

use crate::apply::Apply;
use crate::layer::Layer;
use crate::pipeline::scheduler::{Id, Signal};
use crate::schedule::Strategy;

pub(crate) trait Coordinate<A: Apply>: Send + 'static {
    fn on_promote(layer: &Layer<A>, signal: &Signal) -> Promotion;
    fn on_demote(layer: &Layer<A>, id: Id) -> Demotion;
}

pub(crate) enum Promotion {
    Idle,
    Upgrade { id: Id, to: Strategy },
    Scale { strategy: Strategy },
}

pub(crate) enum Demotion {
    Idle,
    Release { id: Id },
}
