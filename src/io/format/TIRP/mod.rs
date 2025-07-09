pub mod extract;
pub mod file;
pub mod reader;
pub mod stream;
pub mod writer;

pub use extract::*;
pub use file::*;
pub use reader::*;
pub use stream::*;
pub use writer::*;

/// TIRP file abstraction. Preferred import:
///
/// use crate::io::format::TIRP::File;
/// let f = File::new("foo.tirp.gz");
pub use file::File;
