use std::future::Future;
use std::pin::Pin;

use crate::schedule::Schedule;

pub type Error = Box<dyn std::error::Error + Send + std::marker::Sync>;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Sync;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Async;

pub trait Executable: Clone + Copy + Send + std::marker::Sync + 'static {
    type Outcome<'this, O>
    where
        O: 'this;

    fn default_schedule() -> Schedule;
}

impl Executable for Sync {
    type Outcome<'this, O>
        = Result<Option<O>, Error>
    where
        O: 'this;

    fn default_schedule() -> Schedule {
        Schedule::auto()
    }
}

impl Executable for Async {
    type Outcome<'this, O>
        = Pin<Box<dyn Future<Output = Result<Option<O>, Error>> + Send + 'this>>
    where
        O: 'this;

    fn default_schedule() -> Schedule {
        Schedule::async_default()
    }
}
