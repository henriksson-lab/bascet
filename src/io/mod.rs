pub mod format;
pub mod traits;

pub use format::*;
pub use traits::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    TIRP,
    Unknown,
}
