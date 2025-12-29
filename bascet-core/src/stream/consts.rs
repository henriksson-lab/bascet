use bounded_integer::BoundedUsize;

pub const DEFAULT_MIN_COUNTOF_BUFFERS: usize = 2;
pub const DEFAULT_COUNTOF_BUFFERS: BoundedUsize<2, { usize::MAX }> =
    BoundedUsize::const_new::<1024>();
