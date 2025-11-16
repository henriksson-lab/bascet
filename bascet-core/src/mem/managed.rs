use std::{rc::Rc, sync::Arc};

unsafe impl<T> crate::ManuallyManaged for Arc<T> {
    fn inc_ref(&mut self) {
        unsafe {
            Arc::increment_strong_count(Arc::as_ptr(self));
        }
    }

    fn dec_ref(&mut self) {
        unsafe {
            Arc::decrement_strong_count(Arc::as_ptr(self));
        }
    }
}

unsafe impl<T> crate::ManuallyManaged for Rc<T> {
    fn inc_ref(&mut self) {
        unsafe {
            Rc::increment_strong_count(Rc::as_ptr(self));
        }
    }

    fn dec_ref(&mut self) {
        unsafe {
            Rc::decrement_strong_count(Rc::as_ptr(self));
        }
    }
}
