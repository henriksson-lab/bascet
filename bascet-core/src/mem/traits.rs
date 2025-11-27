/// Trait for types that support manual reference counting.
///
/// Types implementing this trait can be wrapped in `Managed<T>` for
/// automatic reference release on drop.
///
/// # Safety
/// Implementors must ensure that `retain()` and `release()` correctly
/// manage the reference count and that the underlying resource is only
/// freed when the count reaches zero.
pub unsafe trait Shared {
    fn retain(&self);
    fn release(&self);
}
