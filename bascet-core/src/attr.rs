// NOTE: mod macros and macros use must come BEFORE anything using the macros?
#[macro_use]
mod macros;
mod traits;
#[rustfmt::skip]
mod attrs;

pub use attrs::*;
pub use traits::*;