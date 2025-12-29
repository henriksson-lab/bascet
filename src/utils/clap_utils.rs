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
