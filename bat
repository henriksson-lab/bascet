use enum_dispatch::enum_dispatch;
use paste::paste;

use crate::io::traits::{BascetFile, BascetRead, BascetStream, BascetStreamToken, BascetWrite};

// detect file
macro_rules! file_valid_formats {
    ($path_expr:expr; $($module:ident),*) => {
        {
            $(
                if let Ok(inner) = crate::io::format::$module::File::new($path_expr) {
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
        {
            $(
                paste! {
                    if let AutoFile::[<$module:camel>](ref file_inner) = $file_expr {
                        return Some(<$enum_ty>::[<$module:camel>](
                            crate::io::format::$module::Stream::<$T>::new(file_inner)
                        ));
                    }
                }
            )*
            None
        }
    };
}

#[enum_dispatch(BascetStream<T>)]
pub enum AutoCountSketchStream<T>
where
    T: BascetStreamToken + Send + 'static,
{
    Tirp(crate::io::format::tirp::Stream<T>),
    Zip(crate::io::format::zip::Stream<T>)
}

pub fn which_countsketch_stream<T>(file: AutoFile) -> Option<AutoCountSketchStream<T>>
where
    T: BascetStreamToken + Send + 'static,
{
    stream_valid_formats!(file; AutoCountSketchStream<T>, T; tirp, zip)
}
