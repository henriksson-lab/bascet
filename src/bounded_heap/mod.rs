mod bounded_heap;
mod bounded_heap_strategy;

pub(crate) use delegate::delegate;
pub(crate) use min_max_heap;

pub use bounded_heap::BoundedHeap as BoundedHeap;
pub use bounded_heap_strategy::MaxStrategy as MaxStrategy;
pub use bounded_heap_strategy::MinStrategy as MinStrategy;

pub mod prelude {
    pub use super::bounded_heap::BoundedHeapBehaviour;
    pub use super::bounded_heap_strategy::BoundedHeapStrategy;
}
pub use prelude::*;