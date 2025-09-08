use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Subcommand};

use crate::{
    common, io::traits::{BascetCell, BascetCellBuilder, BascetStream}, log_warning, support_which_stream, support_which_writer
};

support_which_stream! {
    TrimExperimentalInput => TrimExperimentalStream<T: BascetCell>
    for formats [fastq_gz]
}
support_which_writer! {
    TrimExperimentalOutput => TrimExperimentalWriter<W: std::io::Write>
    for formats [tirp_bgzf]
}

#[derive(Args)]
pub struct TrimExperimentalCMD {
    #[command(subcommand)]
    pub chemistry: Chemistry,

    // Input bascets (comma separated; ok with PathBuf???)
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_in: Vec<PathBuf>,

    // Output bascets
    #[arg(short = 'o', value_parser= clap::value_parser!(PathBuf), num_args = 1.., value_delimiter = ',')]
    pub path_out: Vec<PathBuf>,

    #[arg(short = '@', value_parser = clap::value_parser!(usize), default_value_t = 16)]
    threads_total: usize,
    #[arg(short = 'r', value_parser = clap::value_parser!(usize), default_value_t = 16)]
    threads_read: usize,
    #[arg(short = 'w', value_parser = clap::value_parser!(usize), default_value_t = 0)]
    threads_work: usize,

    // Stream buffer configuration
    #[arg(long = "buffer-size", value_parser = clap::value_parser!(usize), default_value_t = 8196)]
    pub buffer_size_mb: usize,
    #[arg(long = "page-size", value_parser = clap::value_parser!(usize), default_value_t = 8)]
    pub page_size_mb: usize,
}

#[derive(Subcommand)]
pub enum Chemistry {
    Atrandi(AtrandiArgs),
}

#[derive(Args)]
pub struct AtrandiArgs {}

impl TrimExperimentalCMD {
    pub fn try_execute(&mut self) -> Result<()> {
        let paths_in = &self.path_in;
        for path_in in paths_in {
            let input = TrimExperimentalInput::try_from_path(path_in).unwrap();
            let mut stream =
                TrimExperimentalStream::<TrimExperimentalCell>::try_from_input(input).unwrap();
            stream = stream.set_pagebuffer_config(8196, 8);

            match &self.chemistry {
                Chemistry::Atrandi(_args) => {
                    for token in stream {
                        println!("Got token!");
                        println!("{:?}", String::from_utf8_lossy(token.unwrap().cell))
                    }
                }
                _ => todo!(),
            }
        }

        Ok(())
    }
}

struct TrimExperimentalCell {
    cell: &'static [u8],
    read: &'static [u8],
    quality: &'static [u8],

    _page_refs: smallvec::SmallVec<[common::UnsafeMutPtr<common::PageBuffer>; 2]>,
    _owned: Vec<Vec<u8>>,
}
impl Drop for TrimExperimentalCell {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe {
            for page_ptr in &self._page_refs {
                (*page_ptr.mut_ptr()).dec_ref();
            }
        }
    }
}

impl BascetCell for TrimExperimentalCell {
    type Builder = TrimExperimentalCellBuilder;
    fn builder() -> Self::Builder {
        Self::Builder::new()
    }
}
struct TrimExperimentalCellBuilder {
    cell: Option<&'static [u8]>,
    read: Option<&'static [u8]>,
    quality: Option<&'static [u8]>,

    page_refs: smallvec::SmallVec<[common::UnsafeMutPtr<common::PageBuffer>; 2]>,
    owned: Vec<Vec<u8>>,
}

impl TrimExperimentalCellBuilder {
    fn new() -> Self {
        Self {
            cell: None,
            read: None,
            quality: None,

            page_refs: smallvec::SmallVec::new(),
            owned: Vec::new(),
        }
    }
}

impl BascetCellBuilder for TrimExperimentalCellBuilder {
    type Token = TrimExperimentalCell;

    #[inline(always)]
    fn add_page_ref(mut self, page_ptr: common::UnsafeMutPtr<common::PageBuffer>) -> Self {
        unsafe {
            (*page_ptr.mut_ptr()).inc_ref();
        }
        self.page_refs.push(page_ptr);
        self
    }

    // NOTE: Here the idea is that for as long as the stream tokens are alive the underlying memory will be kept alive
    // by Arcs. For as long as these are valid the memory can be considered static even if it technically is not
    // this is a bit of a hack to make the underlying trait easier to use.
    // has the benefit of being much faster and more memory efficient since there is no copy overhead
    #[inline(always)]
    fn add_cell_id_slice(mut self, slice: &'static [u8]) -> Self {
        if self.cell.is_some() {
            log_warning!("Cell ID already set, overwriting");
        }
        self.cell = Some(slice);
        self
    }

    #[inline(always)]
    fn add_sequence_slice(mut self, slice: &'static [u8]) -> Self {
        if self.read.is_some() {
            log_warning!("Sequence already set, overwriting");
        }
        self.read = Some(slice);
        self
    }
    #[inline(always)]
    fn add_quality_slice(mut self, slice: &'static [u8]) -> Self {
        if self.quality.is_some() {
            log_warning!("Quality already set, overwriting");
        }
        self.quality = Some(slice);
        self
    }

    #[inline(always)]
    fn build(self) -> TrimExperimentalCell {
        TrimExperimentalCell {
            cell: self.cell.expect("cell is required"),
            read: self.read.expect("read is required"),
            quality: self.quality.expect("quality is required"),

            _page_refs: self.page_refs,
            _owned: self.owned,
        }
    }
}

// convenience iterator over stream
impl<T> Iterator for TrimExperimentalStream<T>
where
    T: BascetCell,
{
    type Item = Result<T, crate::runtime::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_cell().transpose()
    }
}
