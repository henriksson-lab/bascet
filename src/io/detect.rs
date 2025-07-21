use enum_dispatch::enum_dispatch;
use paste::paste;

use crate::io::traits::{BascetFile, BascetRead, BascetStream, BascetStreamToken, BascetWrite};

// detect file
macro_rules! file_valid_formats {
    ($path_expr:expr; $($module:ident),*) => {
        {
            $(
                if let Ok(_) = crate::io::$module::File::file_validate($path_expr) {
                    paste! {
                        return Some(AutoFile::[<$module:camel>](crate::io::format::$module::File::new($path_expr).unwrap()));
                    }
                }
            )*
            None
        }
    };
}
#[derive(Debug)]
pub enum AutoFile {
    Tirp(crate::io::format::tirp::File),
    Zip(crate::io::format::zip::File),
}

pub fn which_file<P: AsRef<std::path::Path>>(path: P) -> Option<AutoFile> {
    let path = path.as_ref().to_path_buf();
    file_valid_formats!(&path; tirp, zip)
}

// detect stream
macro_rules! stream_valid_formats {
    ($file_expr:expr; $enum_ty:ty, $T:ty, $I:ty, $P:ty; $($module:ident),*) => {
        match $file_expr {
            $(
                paste! { AutoFile::[<$module:camel>](inner) } => Some(
                    paste! { <$enum_ty>::[<$module:camel>] }(
                        crate::io::format::$module::Stream::new(&inner)
                    )
                ),
            )*
            _ => None
        }
    };
}

#[enum_dispatch(BascetStream<T, I, P>)]
pub enum AutoCountSketchStream<T, I, P>
where
    T: BascetStreamToken<I, P> + Send + 'static,
    I: From<Vec<u8>>,
    P: From<Vec<Vec<u8>>>,
{
    Tirp(crate::io::tirp::Stream<T, I, P>),
}

pub fn which_countsketch_stream<T, I, P>(file: AutoFile) -> Option<AutoCountSketchStream<T, I, P>>
where
    T: BascetStreamToken<I, P> + Send + 'static,
    I: From<Vec<u8>>,
    P: From<Vec<Vec<u8>>>,
{
    stream_valid_formats!(file; AutoCountSketchStream<T, I, P>, T, I, P; tirp)
}