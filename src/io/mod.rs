pub mod format;
pub mod stream;
pub mod traits;

pub use format::*;
pub use stream::*;
pub use traits::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    TIRP,
    Unknown,
}
