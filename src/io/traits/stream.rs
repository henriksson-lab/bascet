use crate::command::countsketch::CountsketchStream;
use crate::command::shardify::ShardifyStream;


#[enum_dispatch::enum_dispatch]
pub trait BascetStream<Cell>: Sized
where
    Cell: crate::io::traits::BascetCell + 'static,
    Cell::Builder: crate::io::traits::BascetCellBuilder<Cell = Cell>,
{
    fn next_cell(&mut self) -> Result<Option<Cell>, crate::runtime::Error>;

    fn set_reader_threads(self, _: usize) -> Self {
        self
    }
}