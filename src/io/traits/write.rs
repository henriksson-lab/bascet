use crate::command::countsketch::CountsketchOutput;
use crate::command::countsketch::CountsketchWriter;

use crate::command::shardify::ShardifyWriter;

#[enum_dispatch::enum_dispatch]
pub trait BascetWrite<W>: Sized
where
    W: std::io::Write,
{
    fn set_writer(self, _: W) -> Self;
    fn get_writer(self) -> Option<W>;

    fn write_cell<Cell>(&mut self, cell: &Cell) -> Result<(), crate::runtime::Error>
    where
        Cell: crate::io::traits::BascetCell,
    {
        unimplemented!()
    }

    fn write_countsketch<Cell>(
        &mut self,
        cell: &Cell,
        countsketch: &crate::kmer::kmc_counter::CountSketch,
    ) -> Result<(), crate::runtime::Error>
    where
        Cell: crate::io::traits::CellIdAccessor,
    {
        unimplemented!()
    }
}
