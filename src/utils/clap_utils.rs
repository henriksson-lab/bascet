#[macro_export]
macro_rules! bounded_parser {
    (BoundedU64<$min:tt, $max:tt>) => {
        bounded_parser!(@impl u64, BoundedU64, $min, $max)
    };
    (@impl $prim:ty, $bounded:ident, $min:tt, $max:tt) => {
        clap::builder::TypedValueParser::try_map(
            clap::value_parser!($prim).range($min..=$max),
            |n| bounded_integer::$bounded::<$min, $max>::new(n).ok_or("Value out of bounds")
        )
    };
}

#[macro_export]
macro_rules! bbgz_compression_parser {
    () => {
        bbgz_compression_parser!(bascet_io::Compression)
    };
    ($compression_type:ty) => {
        clap::builder::TypedValueParser::try_map(clap::value_parser!(i32).range(0..=12), |n| {
            bounded_integer::BoundedI32::<0, 12>::new(n)
                .map(<$compression_type>::from)
                .ok_or("Compression level out of bounds")
        })
    };
}
