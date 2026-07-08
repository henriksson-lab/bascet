use super::runtime::{self, Dispatch};
use crate::apply::{Apply, Scheduled};
use crate::coordinate::Coordinate;
use crate::execute::{Async, Executable};
use crate::layer::{Control, Layer};
use crate::pipe::Pipe;
use crate::set::Set;

use crate::pipeline::consts::REQ_DEPTH_MAX;
use crate::pipeline::edge::{Downstream, Edge, Upstream};
use crate::pipeline::runtime::Runtime;
use crate::pipeline::scheduler::Slot;

pub(crate) struct Drain;

impl<S> Layer<S>
where
    S: Apply,
{
    pub(crate) fn register<W>(
        self,
        runtime: Runtime,
        input: Upstream<S::Input>,
        output: Option<Downstream<S::Output>>,
    ) where
        Self: Scheduled<Input = S::Input, Output = S::Output>,
        W: Set + 'static,
        S: Clone + Send + 'static,
        S::Input: Send + 'static,
        S::Output: Send + 'static,
        <Self as Apply>::Runtime: Dispatch<Self, W>,
        S::Coordinate: Coordinate<S, W>,
    {
        S::Coordinate::stage(runtime, self, input, output);
    }

    pub(crate) fn register_source<W>(self, runtime: Runtime, output: Downstream<S::Output>)
    where
        Self: Scheduled<Input = (), Output = S::Output>,
        W: Set + 'static,
        S: Clone + Send + 'static,
        S::Output: Send + 'static,
        <Self as Apply>::Runtime: Dispatch<Self, W>,
        S::Coordinate: Coordinate<S, W>,
    {
        S::Coordinate::source(runtime, self, output);
    }
}

impl Drain {
    pub(crate) fn register<T>(runtime: Runtime, input: Upstream<T>)
    where
        T: Send + 'static,
    {
        let petitioner = runtime.petitioner().clone();
        let input = input.edge();
        let spawn =
            Box::new(move |slot: Slot| runtime::Drain::spawn(runtime.clone(), input.clone(), slot));
        let control = Control::new(
            <Async as Executable>::default_schedule(),
            spawn,
            petitioner.clone(),
        );
        petitioner.register(control);
    }
}

pub(crate) trait Connect<W: Set, Input: Send + 'static> {
    type Output: Send + 'static;

    fn connect(self, input: Upstream<Input>, runtime: Runtime) -> Upstream<Self::Output>;
}

impl<W: Set, Input: Send + 'static> Connect<W, Input> for () {
    type Output = Input;

    fn connect(self, input: Upstream<Input>, _runtime: Runtime) -> Upstream<Self::Output> {
        input
    }
}

impl<S, Tail, W, Input> Connect<W, Input> for Pipe<Layer<S>, Tail>
where
    S: Apply + Clone + Send + 'static,
    S::Coordinate: Coordinate<S, W>,
    Layer<S>: Scheduled<Input = S::Input, Output = S::Output>,
    S::Input: Send + 'static,
    S::Output: Send + 'static,
    Tail: Connect<W, Input, Output = S::Input> + Send + 'static,
    W: Set + 'static,
    Input: Send + 'static,
    <Layer<S> as Apply>::Runtime: Dispatch<Layer<S>, W>,
{
    type Output = S::Output;

    fn connect(self, input: Upstream<Input>, runtime: Runtime) -> Upstream<Self::Output> {
        let stage_input = self.1.connect(input, runtime.clone());
        let (stage_output_upstream, stage_output_downstream) =
            Edge::<S::Output>::new(REQ_DEPTH_MAX);
        self.0
            .register::<W>(runtime, stage_input, Some(stage_output_downstream));
        stage_output_upstream
    }
}
