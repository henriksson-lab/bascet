pub mod tirp;
pub mod zip;

pub use tirp::*;
pub use zip::*;

crate::support_which_files! {
    AutoBascetFile
    for formats [tirp, zip]
}
