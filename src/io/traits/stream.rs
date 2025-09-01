use crate::command::countsketch::CountsketchStream;
use crate::command::shardify::ShardifyStream;

#[enum_dispatch::enum_dispatch]
pub trait BascetCellStream<C>: Sized
{
    fn next_cell(&mut self) -> Result<Option<C>, crate::runtime::Error>;

    fn set_reader_threads(self, _: usize) -> Self {
        self
    }
}
