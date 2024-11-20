use std::cmp::Reverse;

use min_max_heap::MinMaxHeap;

pub enum HeapError {}

pub trait BoundedHeap<T, C = usize>
where
    T: Ord,
    C: Into<usize> + Copy,
{
    fn peek_border(&self)   -> Option<&T>;
    fn push(&self)          -> Result<(), HeapError>;
    fn pop(&self)           -> Result<(), HeapError>;
}

pub struct BoundedMinHeap<T, C = usize>
where
    T: Ord,
    C: Into<usize> + Copy,
{
    data: MinMaxHeap<T>,
    capacity: C
}

impl BoundedHeap<T> for BoundedMinHeap<T, C>
where
    T: Ord,
    C: Into<usize> + Copy,
{
    
}

// pub fn with_capacity(capacity: C) -> Self {
//     return Self {
//         data: MinMaxHeap::<T>::with_capacity(capacity.into()),
//         capacity: capacity,
//     };
// }
// pub fn push(&mut self, feature: T) -> Result<(), HeapError> {
//     if let Some(min) = self.data.peek_min() {
//         if &feature < min {
//             return Ok(());
//         }
//     }

//     if self.data.len() >= self.capacity.into() {
//         self.data.push_pop_max(feature);
//         return Ok(());
//     }
    
//     self.data.push(feature);
//     Ok(())
// }