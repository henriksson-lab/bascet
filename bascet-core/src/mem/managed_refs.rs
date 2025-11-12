pub unsafe trait ManagedRef {}

unsafe impl<T> ManagedRef for std::rc::Rc<T> {}
unsafe impl<T> ManagedRef for std::sync::Arc<T> {}
