use std::marker::PhantomData;

use crate::layer::Layer;
use crate::owned::Owned;
use crate::pipe::Pipe;
use crate::set::{Set, Subset, Union};
use crate::execute::Executable;
use crate::schedule::Schedule;
use crate::traits;

pub struct PipelineBuilder<Provides, Producer, Applies, Resources>
where
    Provides: Set,
{
    pub producer: Producer,
    pub applies: Applies,
    pub resources: Resources,
    _provides: PhantomData<Provides>,
}

impl PipelineBuilder<(), (), (), ()> {
    pub fn new() -> Self {
        PipelineBuilder {
            producer: (),
            applies: (),
            resources: (),
            _provides: PhantomData,
        }
    }
}

impl<Provides, Applies, Resources> PipelineBuilder<Provides, (), Applies, Resources>
where
    Provides: Set,
{
    pub fn layer<A>(
        self,
        apply: A,
    ) -> PipelineBuilder<<Provides as Union<A::Provides>>::Output, Layer<A>, Applies, Resources>
    where
        A: traits::Apply<Input = ()>,
        A::Requires: Subset<Provides>,
        Resources: Owned<A::Resources>,
        Provides: Union<A::Provides>,
        <Provides as Union<A::Provides>>::Output: Set,
    {
        self.layer_scheduled(apply, <A::Runtime as Executable>::default_schedule())
    }

    fn layer_scheduled<A>(
        self,
        apply: A,
        schedule: Schedule,
    ) -> PipelineBuilder<<Provides as Union<A::Provides>>::Output, Layer<A>, Applies, Resources>
    where
        A: traits::Apply<Input = ()>,
        A::Requires: Subset<Provides>,
        Resources: Owned<A::Resources>,
        Provides: Union<A::Provides>,
        <Provides as Union<A::Provides>>::Output: Set,
    {
        PipelineBuilder {
            producer: Layer::new(apply, schedule),
            applies: self.applies,
            resources: self.resources,
            _provides: PhantomData,
        }
    }
}

impl<Provides, Producer, Applies> PipelineBuilder<Provides, Producer, Applies, ()>
where
    Provides: Set,
{
    pub fn resource<Resources>(
        self,
        resources: Resources,
    ) -> PipelineBuilder<Provides, Producer, Applies, Resources> {
        PipelineBuilder {
            producer: self.producer,
            applies: self.applies,
            resources,
            _provides: PhantomData,
        }
    }
}

impl<Provides, Producer, Applies, Resources> PipelineBuilder<Provides, Producer, Applies, Resources>
where
    Provides: Set,
    Producer: traits::Apply<Input = ()>,
{
    pub fn layer<A>(
        self,
        apply: A,
    ) -> PipelineBuilder<
        <Provides as Union<A::Provides>>::Output,
        Producer,
        Pipe<Layer<A>, Applies>,
        Resources,
    >
    where
        A: traits::Apply,
        A::Requires: Subset<Provides>,
        Resources: Owned<A::Resources>,
        Provides: Union<A::Provides>,
        <Provides as Union<A::Provides>>::Output: Set,
    {
        self.layer_scheduled(apply, <A::Runtime as Executable>::default_schedule())
    }

    fn layer_scheduled<A>(
        self,
        apply: A,
        schedule: Schedule,
    ) -> PipelineBuilder<
        <Provides as Union<A::Provides>>::Output,
        Producer,
        Pipe<Layer<A>, Applies>,
        Resources,
    >
    where
        A: traits::Apply,
        A::Requires: Subset<Provides>,
        Resources: Owned<A::Resources>,
        Provides: Union<A::Provides>,
        <Provides as Union<A::Provides>>::Output: Set,
    {
        PipelineBuilder {
            producer: self.producer,
            applies: Pipe(Layer::new(apply, schedule), self.applies),
            resources: self.resources,
            _provides: PhantomData,
        }
    }
}
