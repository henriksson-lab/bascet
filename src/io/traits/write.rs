use crate::command::countsketch::CountsketchWriter;
// use crate::command::countsketch::CountsketchOutput;

// use crate::command::shardify::ShardifyWriter;

#[enum_dispatch::enum_dispatch]
pub trait BascetWrite<W>: Sized
where
    W: std::io::Write,
{
    fn set_writer(self, _: W) -> Self;
    fn get_writer(self) -> Option<W>;

    fn write_cell<C>(&mut self, cell: &C) -> Result<(), crate::runtime::Error>
    where
        C: crate::io::traits::BascetCell,
    {
        todo!()
    }

    fn write_countsketch<C>(
        &mut self,
        cell: &C,
        countsketch: &crate::kmer::kmc_counter::CountSketch,
    ) -> Result<(), crate::runtime::Error>
    where
        C: crate::io::traits::BascetCell,
    {
        todo!()
    }
}
