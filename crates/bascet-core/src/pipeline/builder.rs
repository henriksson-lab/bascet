use std::marker::PhantomData;

use crate::apply::execute::Work;
use crate::set::{Set, Subset, Union};

pub struct PipelineBuilder<Chain> {
    pub(crate) chain: Chain,
}

pub struct Source<A, M> {
    pub(crate) apply: A,
    pub(crate) _mode: PhantomData<M>,
}

pub struct Pipe<A, M, Tail> {
    pub(crate) apply: A,
    pub(crate) tail: Tail,
    pub(crate) _mode: PhantomData<M>,
}

pub struct Pipeline<Chain> {
    pub(crate) chain: Chain,
}

pub trait Head {
    type Output;
    type Provides: Set;
}

impl<A, M: 'static> Head for Source<A, M>
where
    A: Work<M>,
{
    type Output = A::Output;
    type Provides = A::Provides;
}

impl<A, M: 'static, Tail> Head for Pipe<A, M, Tail>
where
    A: Work<M>,
{
    type Output = A::Output;
    type Provides = A::Provides;
}

pub type Wanted<A, M, W> = Union<<A as Work<M>>::Requires, W>;

impl Pipeline<()> {
    pub fn builder() -> PipelineBuilder<()> {
        PipelineBuilder { chain: () }
    }
}

impl PipelineBuilder<()> {
    pub fn source<A, M: 'static>(self, apply: A) -> PipelineBuilder<Source<A, M>>
    where
        A: Work<M, Input = ()>,
    {
        PipelineBuilder {
            chain: Source {
                apply,
                _mode: PhantomData,
            },
        }
    }
}

impl<Chain: Head> PipelineBuilder<Chain> {
    pub fn layer<A, M: 'static>(self, apply: A) -> PipelineBuilder<Pipe<A, M, Chain>>
    where
        A: Work<M, Input = Chain::Output>,
        A::Requires: Subset<Chain::Provides>,
    {
        PipelineBuilder {
            chain: Pipe {
                apply,
                tail: self.chain,
                _mode: PhantomData,
            },
        }
    }

    pub fn sink<A, M: 'static>(self, apply: A) -> Pipeline<Pipe<A, M, Chain>>
    where
        A: Work<M, Input = Chain::Output, Output = ()>,
        A::Requires: Subset<Chain::Provides>,
    {
        Pipeline {
            chain: Pipe {
                apply,
                tail: self.chain,
                _mode: PhantomData,
            },
        }
    }
}
