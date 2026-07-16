use std::sync::Arc;

use crate::apply::Error;
use crate::runtime::RuntimeInner;
use crate::schedule::Schedule;

pub struct Runner {
    pub(crate) runtime: Arc<RuntimeInner>,
    pub(crate) schedule: Arc<Schedule>,
    pub(crate) sink: usize,
}

impl Runner {
    pub fn join(self) -> Result<(), Error> {
        self.schedule.join_wait(self.sink);
        self.runtime.shutdown.trigger();
        match self.runtime.take_error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
