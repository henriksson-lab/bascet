use crate::command::shardify::ShardifyWriter;

#[enum_dispatch::enum_dispatch]
pub trait BascetWrite<W>: Sized
where
    W: std::io::Write,
{
    fn set_writer(self, _: W) -> Self {
        self
    }
    fn write_cell<T>(&mut self, token: T)
    where
        T: crate::io::traits::BascetCell;
}
