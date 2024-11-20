pub enum HeapError {}

pub trait BoundedHeapBehaviour<T>
where
    T: Ord,
{
    fn push(&mut self, value: T) -> Result<(), HeapError>;
}

pub struct BoundedHeap<T, S>
where
    T: Ord,
    S: super::BoundedHeapStrategy<T>,
{
    mimxheap: super::min_max_heap::MinMaxHeap<T>,
    capacity: usize,
    _phantom: std::marker::PhantomData<S>,
}

impl<T, S> BoundedHeap<T, S>
where
    T: Ord,
    S: super::BoundedHeapStrategy<T>,
{
    super::delegate! {
        to self.mimxheap {
            //NOTE: #[inline] pub fn capacity(&self) -> usize; implemented on self
            #[inline] pub fn clear(&mut self);

            #[inline] pub fn drain(&mut self)       -> min_max_heap::Drain<'_, T>;
            #[inline] pub fn drain_asc(&mut self)   -> min_max_heap::DrainAsc<'_, T>;
            #[inline] pub fn drain_desc(&mut self)  -> min_max_heap::DrainDesc<'_, T>;

            #[inline] pub fn into_vec(self)         -> Vec<T>;
            #[inline] pub fn into_vec_asc(self)     -> Vec<T>;
            #[inline] pub fn into_vec_desc(self)    -> Vec<T>;

            #[inline] pub fn is_empty(&self)        -> bool;

            #[inline] pub fn iter(&self)            -> min_max_heap::Iter<'_, T>;

            #[inline] pub fn len(&self)             -> usize;
            //NOTE: #[inline] pub fn new() -> Self;
            //      not implemented, BoundedHeap requires a capacity to be passed

            //NOTE: min/max refer to the min and max of the heap, NOT the last element.
            //      The last element depends on the BoundedHeapStrategy; peek_first/last(_mut) is implemented by BoundedHeapStrategy
            #[inline] pub fn peek_max(&self)            -> Option<&T>;
            #[inline] pub fn peek_max_mut(&mut self)    -> Option<min_max_heap::PeekMaxMut<'_,T> >;
            #[inline] pub fn peek_min(&self)            -> Option<&T>;
            #[inline] pub fn peek_min_mut(&mut self)    -> Option<min_max_heap::PeekMinMut<'_,T> >;

             //NOTE: min/max refer to the min and max of the heap, NOT the last element.
            //      The last element depends on the BoundedHeapStrategy; pop_first/last is implemented by BoundedHeapStrategy
            #[inline] pub fn pop_max(&mut self) -> Option<T>;
            #[inline] pub fn pop_min(&mut self) -> Option<T>;

            //NOTE: #[inline] pub fn push(&mut self, element: T);
            //      not delegated, BoundedHeapBehaviour implements this

            //NOTE: min/max refer to the min and max of the heap, NOT the last element.
            //      The last element depends on the BoundedHeapStrategy; push_pop_first/last is implemented by BoundedHeapStrategy
            #[inline] pub fn push_pop_max(&mut self, element: T) -> T;
            #[inline] pub fn push_pop_min(&mut self, element: T) -> T;

            //NOTE: min/max refer to the min and max of the heap, NOT the last element.
            //      The last element depends on the BoundedHeapStrategy
            #[inline] pub fn replace_max(&mut self, element: T) -> Option<T>;
            #[inline] pub fn replace_min(&mut self, element: T) -> Option<T>;

            //NOTE: #[inline] pub fn reserve(&mut self, additional: usize);
            //      not implemented, BoundedHeap has a fixed capacity
            //NOTE: #[inline] pub fn reserve_exact(&mut self, additional: usize);
            //      not implemented, BoundedHeap has a fixed capacity
            //NOTE: #[inline] pub fn shrink_to_fit(&mut self);
            //      not implemented, BoundedHeap has a fixed capacity
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        return self.capacity;
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            mimxheap: super::min_max_heap::MinMaxHeap::with_capacity(capacity),
            capacity: capacity,

            _phantom: std::marker::PhantomData,
        }
    }
}
impl<T, S> BoundedHeapBehaviour<T> for BoundedHeap<T, S>
where
    T: Ord,
    S: super::BoundedHeapStrategy<T>,
{
    fn push(&mut self, value: T) -> Result<(), HeapError> {
        if self.len() == self.capacity {
            // if the element would be lower priority than the last element, dont push
            if let Some(last) = S::peek_last(&self.mimxheap) {
                if last.cmp(&value) == S::ORDERING {
                    return Ok(());
                }
            }

            S::push_pop_last(&mut self.mimxheap, value);
            return Ok(());
        }

        self.mimxheap.push(value);
        return Ok(());
    }
}