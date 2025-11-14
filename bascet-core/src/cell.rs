//! Cell API for building and accessing structured data.
//!
//! # Example
//! ```
//! use bascet_core::*;
//! use bascet_derive::*;
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
//! In addition, several flags exist to modify builder and provider:
//! ```compile_fail
//! use bascet_core::*;
//! use bascet_derive::*;
//!
//! #[cell(Id, Read, Metadata(nobuild: &'static str))]
//! pub struct TestCell {
//!     #[with(|mut builder: TestCellBuilder, value: Vec<u8>| { builder.id = value.into_iter().map(|v| v * 2).collect(); builder })]
//!     id: Vec<u8>,
//!
//!     #[default(|| vec![b"ATGCATCC".to_vec()])]
//!     read: Vec<Vec<u8>>,
//! }
//! let mut cell = TestCell::builder()
//!     .with::<Id>(vec![1, 2, 3])
//!     .with::<Metadata>("This will be ignored")
//!     .build();
//! let (id, read) = cell.get_ref::<(Id, Read)>();
//! assert_eq!(id, &vec![2, 4, 6]);
//! assert_eq!(read, &vec![b"ATGCATCC".to_vec()]);
//! // This fails to compile
//! let metadata = cell.get_ref::<Metadata>();
//! ```
pub mod traits;
pub use traits::*;
