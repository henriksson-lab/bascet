use std::sync::Arc;

use crate::apply::Error;
use crate::runtime::RuntimeInner;
use crate::runtime::workers::Workers;
use crate::schedule::Schedule;

#[must_use = "dropping a Runner cancels its pipeline; call `.join()` to run to completion"]
pub struct Runner {
    pub(crate) runtime: Arc<RuntimeInner>,
    pub(crate) schedule: Arc<Schedule>,
    pub(crate) sink: usize,
    pub(crate) workers: Workers,
}

impl Runner {
    pub fn join(mut self) -> Result<(), Error> {
        self.schedule.join_wait(self.sink);
        self.workers.join();
        match self.runtime.take_error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl Drop for Runner {
    fn drop(&mut self) {
        self.runtime.shutdown.trigger();
    }
}
