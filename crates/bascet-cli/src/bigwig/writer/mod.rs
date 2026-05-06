/*!
A small, write-only BigWig library vendored from bigtools.

The original file format specification for bigWig and bigBed files is defined in this paper: <https://doi.org/10.1093/bioinformatics/btq351>

## Writing

To begin, a [`BigWigWrite`] can be created using [`BigWigWrite::create_file`].

Generally, bigWig writing is done per chromosome, with compression and io being
done on an async Runtime.

The source for data to be written to bigWigs comes from the [`BBIDataSource`]
trait. It abstracts over processing the data
for a bbi file. It is a lower-level API that can be useful for custom value
generation or scheduling logic. Generally though, users should not need to
implement this directly, but rather use [`BedParserStreamingIterator`].

Given some implementation of [`BBIDataSource`], a bigWig can be created using
[`BigWigWrite::write`].
*/

mod bbi;
pub mod utils;

pub use bbi::*;
