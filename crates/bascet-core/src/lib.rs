extern crate self as bascet_core;

pub(crate) mod consts;

pub mod apply;
pub mod arena;
pub mod attr;
pub mod owned;
pub mod pipeline;
pub mod runner;
pub mod runtime;
pub mod schedule;
pub mod set;
pub mod sink;
pub mod utils;
pub mod worker;

pub use apply::{Apply, ApplyAsync, Emit, Error};
pub use arena::{Arena, ArenaPool, ArenaSlice, ArenaView};
pub use attr::{Attr, AttrEntry, Coerce, Mut, Put, Record, Ref, Represents};
pub use owned::Owned;
pub use pipeline::Pipeline;
pub use runner::Runner;
pub use runtime::{Runtime, Tier};
pub use schedule::preempt::Preempt;
pub use sink::{channel, drain};
