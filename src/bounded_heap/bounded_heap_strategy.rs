pub enum PeekMut<'this, T>
where
    T: Ord,
{
    Min(super::min_max_heap::PeekMinMut<'this, T>),
    Max(super::min_max_heap::PeekMaxMut<'this, T>),
}

pub trait BoundedHeapStrategy<T>
where
    T: Ord,
{
    const ORDERING: std::cmp::Ordering;

    fn peek_first(heap: &super::min_max_heap::MinMaxHeap<T>) -> Option<&T>;
    fn peek_last(heap: &super::min_max_heap::MinMaxHeap<T>) -> Option<&T>;
    fn peek_first_mut(heap: &mut super::min_max_heap::MinMaxHeap<T>) -> Option<PeekMut<'_, T>>;
    fn peek_last_mut(heap: &mut super::min_max_heap::MinMaxHeap<T>) -> Option<PeekMut<'_, T>>;

    fn pop_first(heap: &mut super::min_max_heap::MinMaxHeap<T>) -> Option<T>;
    fn pop_last(heap: &mut super::min_max_heap::MinMaxHeap<T>) -> Option<T>;
    fn push_pop_first(heap: &mut super::min_max_heap::MinMaxHeap<T>, value: T) -> T;
    fn push_pop_last(heap: &mut super::min_max_heap::MinMaxHeap<T>, value: T) -> T;
}

pub struct MinStrategy;
impl<T> BoundedHeapStrategy<T> for MinStrategy
where
    T: Ord,
{
    // For a min strategy, we want to keep smaller elements and discard larger ones
    // When comparing last.cmp(&value), if last is smaller than value, we want to keep value
    // Therefore ORDERING should be Less
    const ORDERING: std::cmp::Ordering = std::cmp::Ordering::Less;

    #[inline]
    fn peek_first(heap: &super::min_max_heap::MinMaxHeap<T>) -> Option<&T> {
        heap.peek_min()
    }

    #[inline]
    fn peek_last(heap: &super::min_max_heap::MinMaxHeap<T>) -> Option<&T> {
        heap.peek_max()
    }

    #[inline]
    fn peek_first_mut(heap: &mut super::min_max_heap::MinMaxHeap<T>) -> Option<PeekMut<'_, T>> {
        heap.peek_min_mut().map(PeekMut::Min)
    }

    #[inline]
    fn peek_last_mut(heap: &mut super::min_max_heap::MinMaxHeap<T>) -> Option<PeekMut<'_, T>> {
        heap.peek_max_mut().map(PeekMut::Max)
    }

    #[inline]
    fn pop_first(heap: &mut super::min_max_heap::MinMaxHeap<T>) -> Option<T> {
        heap.pop_min()
    }

    #[inline]
    fn pop_last(heap: &mut super::min_max_heap::MinMaxHeap<T>) -> Option<T> {
        heap.pop_max()
    }

    #[inline]
    fn push_pop_first(heap: &mut super::min_max_heap::MinMaxHeap<T>, value: T) -> T {
        heap.push_pop_min(value)
    }

    #[inline]
    fn push_pop_last(heap: &mut super::min_max_heap::MinMaxHeap<T>, value: T) -> T {
        heap.push_pop_max(value)
    }
}

pub struct MaxStrategy;
impl<T> BoundedHeapStrategy<T> for MaxStrategy
where
    T: Ord,
{
    // For a min strategy, we want to keep larger elements and discard smaller ones
    // When comparing last.cmp(&value), if last is greater than value, we want to keep value
    // Therefore ORDERING should be Greater
    const ORDERING: std::cmp::Ordering = std::cmp::Ordering::Greater;

    #[inline]
    fn peek_first(heap: &super::min_max_heap::MinMaxHeap<T>) -> Option<&T> {
        heap.peek_max()
    }

    #[inline]
    fn peek_last(heap: &super::min_max_heap::MinMaxHeap<T>) -> Option<&T> {
        heap.peek_min()
    }

    #[inline]
    fn peek_first_mut(heap: &mut super::min_max_heap::MinMaxHeap<T>) -> Option<PeekMut<'_, T>> {
        heap.peek_max_mut().map(PeekMut::Max)
    }

    #[inline]
    fn peek_last_mut(heap: &mut super::min_max_heap::MinMaxHeap<T>) -> Option<PeekMut<'_, T>> {
        heap.peek_min_mut().map(PeekMut::Min)
    }

    #[inline]
    fn pop_first(heap: &mut super::min_max_heap::MinMaxHeap<T>) -> Option<T> {
        heap.pop_max()
    }

    #[inline]
    fn pop_last(heap: &mut super::min_max_heap::MinMaxHeap<T>) -> Option<T> {
        heap.pop_min()
    }

    #[inline]
    fn push_pop_first(heap: &mut super::min_max_heap::MinMaxHeap<T>, value: T) -> T {
        heap.push_pop_max(value)
    }

    #[inline]
    fn push_pop_last(heap: &mut super::min_max_heap::MinMaxHeap<T>, value: T) -> T {
        heap.push_pop_min(value)
    }
}
