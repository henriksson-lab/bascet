use crate::command::countsketch::CountsketchStream;
// use crate::command::shardify::ShardifyStream;
use crate::log_debug;

#[enum_dispatch::enum_dispatch]
pub trait BascetStream<T>: Sized
where
    T: BascetCell + 'static,
    for<'page> T::Builder<'page>: BascetCellBuilder<'page, Token = T>,
{
    fn next_cell(&mut self) -> Result<Option<T>, crate::runtime::Error>;
    fn set_reader_threads(self, _: usize) -> Self {
        self
    }
}

pub trait BascetCellGuard {}

// 'page represents the lifetime of buffer page data
// Cells can hold references to page data with this lifetime
pub trait BascetCell: Send + Sized {
    type Builder<'page>: BascetCellBuilder<'page, Token = Self>;
    fn builder<'page>() -> Self::Builder<'page>;

    fn get_cell(&self) -> Option<&[u8]> {
        None
    }
    fn get_reads(&self) -> Option<&[(&[u8], &[u8])]> {
        None
    }
    fn get_qualities(&self) -> Option<&[(&[u8], &[u8])]> {
        None
    }
    fn get_umis(&self) -> Option<&[&[u8]]> {
        None
    }
}

// 'page represents the lifetime of buffer page data that the builder references
pub trait BascetCellBuilder<'page>: Sized {
    type Token: BascetCell;

    // Core methods all builders must support
    fn build(self) -> Self::Token;

    // Optional methods with default implementations

    fn add_sentinel_tracking(self, buffer_page_ptr: *mut crate::io::format::tirp::alloc::PageBuffer, buffer_bounds: (*const u8, *const u8)) -> Self {
        log_debug!("Method 'add_buffer_info' called on a BascetCellBuilder implementation that does not implement this method. Data will be ignored.");
        self
    }

    // Methods that take slices from buffer pages (with 'page lifetime)
    fn add_cell_id_slice(self, id: &'static [u8]) -> Self {
        log_debug!("Method 'add_cell_id_slice' called on a BascetCellBuilder implementation that does not implement this method. Data will be ignored.");
        self
    }

    fn add_rp_slice(self, r1: &'static [u8], r2: &'static [u8]) -> Self {
        log_debug!("Method 'add_rp_slice' called on a BascetCellBuilder implementation that does not implement this method. Data will be ignored.");
        self
    }

    fn add_qp_slice(self, q1: &'static [u8], q2: &'static [u8]) -> Self {
        log_debug!("Method 'add_qp_slice' called on a BascetCellBuilder implementation that does not implement this method. Data will be ignored.");
        self
    }

    fn add_sequence_slice(self, sequence: &'static [u8]) -> Self {
        log_debug!("Method 'add_sequence_slice' called on a BascetCellBuilder implementation that does not implement this method. Data will be ignored.");
        self
    }

    fn add_quality_slice(self, qualities: &'static [u8]) -> Self {
        log_debug!("Method 'add_quality_slice' called on a BascetCellBuilder implementation that does not implement this method. Data will be ignored.");
        self
    }

    fn add_umi_slice(self, umi: &'static [u8]) -> Self {
        log_debug!("Method 'add_umi_slice' called on a BascetCellBuilder implementation that does not implement this method. Data will be ignored.");
        self
    }

    // Lower performance, since these would often require copies in some way.
    fn add_cell_id_owned(self, id: Vec<u8>) -> Self {
        log_debug!("Method 'add_cell_id_owned' called on a BascetCellBuilder implementation that does not implement this method. Data will be ignored.");
        self
    }

    fn add_sequence_owned(self, sequence: Vec<u8>) -> Self {
        log_debug!("Method 'add_sequence_owned' called on a BascetCellBuilder implementation that does not implement this method. Data will be ignored.");
        self
    }

    fn add_rp_owned(self, rp: (Vec<u8>, Vec<u8>)) -> Self {
        log_debug!("Method 'add_rp_owned' called on a BascetCellBuilder implementation that does not implement this method. Data will be ignored.");
        self
    }

    fn add_quality_owned(self, scores: Vec<u8>) -> Self {
        log_debug!("Method 'add_quality_owned' called on a BascetCellBuilder implementation that does not implement this method. Data will be ignored.");
        self
    }

    fn add_umi_owned(self, umi: Vec<u8>) -> Self {
        log_debug!("Method 'add_umi_owned' called on a BascetCellBuilder implementation that does not implement this method. Data will be ignored.");
        self
    }

    fn add_metadata_owned(self, meta: Vec<u8>) -> Self {
        log_debug!("Method 'add_metadata_owned' called on a BascetCellBuilder implementation that does not implement this method. Data will be ignored.");
        self
    }

    fn add_metadata_slice(self, meta: &'static [u8]) -> Self {
        log_debug!("Method 'add_metadata_slice' called on a BascetCellBuilder implementation that does not implement this method. Data will be ignored.");
        self
    }
}
