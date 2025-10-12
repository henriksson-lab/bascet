use std::io::Write;

use crate::command::countsketch::CountsketchWriter;

use crate::command::shardify::ShardifyWriter;

use crate::command::getraw::DebarcodeHistWriter;
use crate::command::getraw::DebarcodeMergeWriter;

#[enum_dispatch::enum_dispatch]

#[allow(unused_variables)]
pub trait BascetWrite<W>: Sized
where
    W: std::io::Write,
{
    fn set_writer(self, _: W) -> Self;
    fn get_writer(self) -> Option<W>;

    fn write_hist<H, K, V>(&mut self, counts: H) -> Result<(), crate::runtime::Error>
    where
        H: IntoIterator<Item = (K, V)>,
        K: AsRef<[u8]>,
        V: std::fmt::Display,
    {
        todo!()
    }

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
