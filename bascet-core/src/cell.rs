//! Cell API for building and accessing structured data.
//!
//! # Example
//!
//! ```
//! use bascet_core::cell::*;
//! use bascet_provide::*;
//!
//! #[cell(Id, Read)]
//! pub struct TestCell {
//!     id: Vec<u8>,
//!     read: Vec<Vec<u8>>,
//! }
//!
//! let mut cell = TestCell::builder()
//!     .with::<Id>(b"cell_test".to_vec())
//!     .with::<Read>(vec![b"ATGC".to_vec()])
//!     .build();
//!
//! let id: &Vec<u8> = cell.get_ref::<Id>();
//! assert_eq!(id, b"cell_test");
//!
//! let id_mut: &mut Vec<u8> = cell.get_mut::<Id>();
//! id_mut.extend_from_slice(b"_modified");
//!
//! let (id, read) = cell.get_ref::<(Id, Read)>();
//! assert_eq!(id, b"cell_test_modified");
//! ```

#[macro_use]
pub(crate) mod macros;
pub mod attr;
pub mod core;
mod test;

pub use attr::*;
pub use core::*;
