use std::marker::PhantomData;

use crate::apply::Apply;
use crate::set::{Lower, Union};

pub struct PipelineBuilder<Chain> {
    pub(crate) chain: Chain,
}

pub struct Source<A> {
    pub(crate) apply: A,
}

pub struct Pipe<A, Stores, Tail> {
    pub(crate) apply: A,
    pub(crate) tail: Tail,
    pub(crate) _stores: PhantomData<fn() -> Stores>,
}

pub struct Pipeline<Chain> {
    pub(crate) chain: Chain,
}

pub type Wanted<A, Stores, W> =
    <<<A as Apply<Stores>>::Requires as Lower>::Out as Union<W>>::Output;

impl Pipeline<()> {
    pub fn builder() -> PipelineBuilder<()> {
        PipelineBuilder { chain: () }
    }
}

impl PipelineBuilder<()> {
    pub fn source<A>(self, apply: A) -> PipelineBuilder<Source<A>> {
        PipelineBuilder {
            chain: Source { apply },
        }
    }
}

impl<Chain> PipelineBuilder<Chain> {
    pub fn layer<A, Stores>(self, apply: A) -> PipelineBuilder<Pipe<A, Stores, Chain>> {
        PipelineBuilder {
            chain: Pipe {
                apply,
                tail: self.chain,
                _stores: PhantomData,
            },
        }
    }

    pub fn sink<A, Stores>(self, apply: A) -> Pipeline<Pipe<A, Stores, Chain>> {
        Pipeline {
            chain: Pipe {
                apply,
                tail: self.chain,
                _stores: PhantomData,
            },
        }
    }
}
