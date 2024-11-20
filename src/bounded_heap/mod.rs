mod bounded_heap;

pub(crate) use delegate::delegate;
pub(crate) use min_max_heap;

pub use bounded_heap::BoundedMinHeap as BoundedMinHeap;
pub use bounded_heap::BoundedMaxHeap as BoundedMaxHeap;

pub mod prelude {
    pub use super::bounded_heap::BoundedHeapBehaviour;
}
pub use prelude::*;