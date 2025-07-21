use enum_dispatch::enum_dispatch;
use paste::paste;

use crate::io::traits::{BascetFile, BascetRead, BascetStream, BascetStreamToken, BascetWrite};

// detect file
macro_rules! file_valid_formats {
    ($path_expr:expr; $($module:ident),*) => {
        {
            $(
                if let Ok(inner) = crate::io::$module::File::new($path_expr) {
                    paste! {
                        return Some(AutoFile::[<$module:camel>](inner));
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
    ($file_expr:expr; $enum_ty:ty, $T:ty; $($module:ident),*) => {
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

#[enum_dispatch(BascetStream<T>)]
pub enum AutoCountSketchStream<T>
where
    T: BascetStreamToken + Send + 'static,
{
    Tirp(crate::io::tirp::Stream<T>),
    Zip(crate::io::zip::Stream<T>)
}

pub fn which_countsketch_stream<T>(file: AutoFile) -> Option<AutoCountSketchStream<T>>
where
    T: BascetStreamToken + Send + 'static,
{
    stream_valid_formats!(file; AutoCountSketchStream<T>, T; tirp, zip)
}
