pub mod emit;
pub mod execute;

pub use emit::Emit;

use crate::set::Set;

pub type Error = ();

pub trait Apply: Clone + Send + 'static {
    type Input;
    type Output;
    type Provides: Set;
    type Requires: Set;

    fn apply<Wants: Set>(
        &mut self,
        input: Self::Input,
        out: &mut Emit<Self::Output, Wants>,
    ) -> Result<(), Error>;

    fn finish<Wants: Set>(&mut self, out: &mut Emit<Self::Output, Wants>) -> Result<(), Error> {
        let _ = out;
        Ok(())
    }
}

pub trait ApplyAsync: Clone + Send + 'static {
    type Input;
    type Output;
    type Provides: Set;
    type Requires: Set;

    async fn apply<Wants: Set>(
        &mut self,
        input: Self::Input,
        out: &mut Emit<Self::Output, Wants>,
    ) -> Result<(), Error>;

    async fn finish<Wants: Set>(
        &mut self,
        out: &mut Emit<Self::Output, Wants>,
    ) -> Result<(), Error> {
        let _ = out;
        Ok(())
    }
}
