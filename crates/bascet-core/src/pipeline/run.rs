use super::builder::PipelineBuilder;
use super::consts::REQ_DEPTH_MAX;
use super::edge::Edge;
use super::pipeline::{Pipeline, Runner};
use super::runtime::Runtime;
use super::watchdog::Watchdog;
use super::worker::{Connect, Dispatch, Drain};
use crate::coordinate::Coordinate;
use crate::layer::Layer;
use crate::set::{Set, Subset};
use crate::apply::Scheduled;

pub trait Run<W> {
    fn run(self, runtime: Runtime) -> Runner;
}

impl<W, Provides, Producer, Applies, Resources> Run<W>
    for PipelineBuilder<Provides, Layer<Producer>, Applies, Resources>
where
    W: Set + Subset<Provides> + 'static,
    Provides: Set,
    Producer: crate::apply::Apply + Clone + Send + 'static,
    Producer::Coordinate: Coordinate<Producer, W>,
    Layer<Producer>: Scheduled<Input = (), Output = Producer::Output>,
    Producer::Output: Send + 'static,
    <Layer<Producer> as crate::apply::Apply>::Runtime: Dispatch<Layer<Producer>, W>,
    Applies: Connect<W, Producer::Output> + Send + 'static,
    <Applies as Connect<W, Producer::Output>>::Output: Send + 'static,
{
    fn run(self, runtime: Runtime) -> Runner {
        let pipeline = Pipeline::default();
        Watchdog::spawn(runtime.clone());

        let (source_upstream, source_downstream) = Edge::<Producer::Output>::new(REQ_DEPTH_MAX);
        self.producer
            .register_source::<W>(runtime.clone(), source_downstream);

        let output = self.applies.connect(source_upstream, runtime.clone());
        Drain::register(runtime.clone(), output);

        Runner::new(
            pipeline,
            runtime.petitioner().clone(),
            runtime.clone(),
            runtime.shutdown().clone(),
            runtime.metrics(),
        )
    }
}
