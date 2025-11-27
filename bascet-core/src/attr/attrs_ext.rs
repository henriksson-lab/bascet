// use crate::{Get, RefCount, Shared};

// pub trait RefCounted: Get<RefCount>
// where
//     <Self as Get<RefCount>>::Value: Shared,
// {
//     #[inline(always)]
//     fn retain(&mut self) {
//         self.attr_mut().retain();
//     }

//     #[inline(always)]
//     fn release(&mut self) {
//         self.attr_mut().release();
//     }
// }

// impl<T> RefCounted for T
// where
//     T: Get<RefCount>,
//     <T as Get<RefCount>>::Value: Shared,
// {

// }

// pub trait BytesAttr<'a, A>: Get<A>
// where
//     <Self as Get<A>>::Value: AsRef<[u8]> + 'a,
// {
//     #[inline(always)]
//     fn bytes(&'a self) -> &'a [u8] {
//         self.attr().as_ref()
//     }
// }

// impl<'a, T, A> BytesAttr<'a, A> for T
// where
//     T: Get<A>,
//     <T as Get<A>>::Value: AsRef<[u8]> + 'a,
// {

// }
