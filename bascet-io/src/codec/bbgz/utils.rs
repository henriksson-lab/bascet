use bounded_integer::BoundedI32;

#[repr(transparent)]
#[derive(Debug, Clone, Copy)]
pub struct Compression(BoundedI32<0, 12>);

impl Compression {
    pub const fn new<const N: i32>() -> Self {
        Self(BoundedI32::const_new::<N>())
    }
    // FIXME:   itd be better to get these values from libdeflater consts (MIN_COMPRESSION_LVL, MAX_COMPRESSION_LVL) but this is currently impossible
    pub const fn best() -> Self {
        Self(BoundedI32::const_new::<12>())
    }

    // NOTE:    6 is the default compression level
    pub const fn balanced() -> Self {
        Self(BoundedI32::const_new::<6>())
    }

    // FIXME:   itd be better to get these values from libdeflater consts (MIN_COMPRESSION_LVL + 1, MAX_COMPRESSION_LVL) but this is currently impossible
    pub const fn fastest() -> Self {
        Self(BoundedI32::const_new::<1>())
    }
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.get())
    }
}

impl From<BoundedI32<0, 12>> for Compression {
    fn from(level: BoundedI32<0, 12>) -> Self {
        Self(level)
    }
}

impl From<Compression> for libdeflater::CompressionLvl {
    fn from(compression: Compression) -> Self {
        libdeflater::CompressionLvl::new(compression.0.get()).unwrap()
    }
}
