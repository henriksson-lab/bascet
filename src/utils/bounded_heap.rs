pub enum HeapError {}

pub trait BoundedHeap<T>
where
    T: Ord,
{
    const ORDERING: std::cmp::Ordering;

    fn push(&mut self, value: T) -> Result<(), HeapError>;
    fn peek_last(&self) -> Option<&T>;
    fn push_pop_last(&mut self, value: T) -> T;
}

pub struct BoundedMinHeap<T>
where
    T: Ord,
{
    mimxheap: min_max_heap::MinMaxHeap<T>,
    capacity: usize,
}

impl<T> BoundedMinHeap<T>
where
    T: Ord,
{
    #[inline]
    pub fn capacity(&self) -> usize {
        return self.capacity;
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            mimxheap: min_max_heap::MinMaxHeap::with_capacity(capacity),
            capacity: capacity,
        }
    }

    delegate::delegate! {
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
}
impl<T> BoundedHeap<T> for BoundedMinHeap<T>
where
    T: Ord,
{
    const ORDERING: std::cmp::Ordering = std::cmp::Ordering::Greater;

    #[inline]
    fn peek_last(&self) -> Option<&T> {
        self.mimxheap.peek_max()
    }

    #[inline]
    fn push_pop_last(&mut self, value: T) -> T {
        self.mimxheap.push_pop_max(value)
    }

    fn push(&mut self, value: T) -> Result<(), HeapError> {
        if self.len() == self.capacity {
            // if the element would be lower priority than the last element, dont push
            if let Some(last) = self.peek_last() {
                if last.cmp(&value) == Self::ORDERING {
                    return Ok(());
                }
            }

            self.push_pop_last(value);
            return Ok(());
        }

        self.mimxheap.push(value);
        return Ok(());
    }
}

pub struct BoundedMaxHeap<T>
where
    T: Ord,
{
    mimxheap: min_max_heap::MinMaxHeap<T>,
    capacity: usize,
}

impl<T> BoundedMaxHeap<T>
where
    T: Ord,
{
    #[inline]
    pub fn capacity(&self) -> usize {
        return self.capacity;
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            mimxheap: min_max_heap::MinMaxHeap::with_capacity(capacity),
            capacity: capacity,
        }
    }

    delegate::delegate! {
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
}

// NOTE: need to call push to guarantee order
impl<T> Extend<T> for BoundedMinHeap<T>
where
    T: Ord,
{
    #[inline]
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for value in iter {
            let _ = self.push(value);
        }
    }
}

impl<T> BoundedHeap<T> for BoundedMaxHeap<T>
where
    T: Ord,
{
    const ORDERING: std::cmp::Ordering = std::cmp::Ordering::Less;

    #[inline]
    fn peek_last(&self) -> Option<&T> {
        self.mimxheap.peek_min()
    }

    #[inline]
    fn push_pop_last(&mut self, value: T) -> T {
        self.mimxheap.push_pop_min(value)
    }

    fn push(&mut self, value: T) -> Result<(), HeapError> {
        if self.len() == self.capacity {
            // if the element would be lower priority than the last element, dont push
            if let Some(last) = self.peek_last() {
                if last.cmp(&value) == Self::ORDERING {
                    return Ok(());
                }
            }

            self.push_pop_last(value);
            return Ok(());
        }

        self.mimxheap.push(value);
        return Ok(());
    }
}

// NOTE: need to call push to guarantee order
impl<T> Extend<T> for BoundedMaxHeap<T>
where
    T: Ord,
{
    #[inline]
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for value in iter {
            let _ = self.push(value);
        }
    }
}
