pub mod execute;
pub mod fuse;

use crate::pipeline::batch::Batch;
use crate::set::Lower;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Layer(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("layer panicked: {0}")]
    Panic(String),
}

pub trait Apply<Stores>: Clone + Send + 'static {
    type Produces;
    type Requires: Lower;

    fn apply_batch(&mut self, input: &Batch<Stores>) -> Result<Option<Self::Produces>, Error>;

    fn finish(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

pub trait ApplyAsync<Stores>: Clone + Send + 'static {
    type Produces;
    type Requires: Lower;

    async fn apply_batch(&mut self, input: &Batch<Stores>)
    -> Result<Option<Self::Produces>, Error>;

    async fn finish(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
