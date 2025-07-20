use enum_dispatch::enum_dispatch;

use crate::io::{
    traits::{BascetFile, BascetRead, BascetStream, BascetStreamToken, BascetWrite}
};

// detect file
macro_rules! file_valid_formats {
    ($ext_expr:expr, $path_expr:expr; $($module:ident),*) => {
        match $ext_expr {
            $(
                crate::io::$module::File::file_validate($path_expr).map_or(None, ext_expr) => AutoFile::$module(crate::io::format::$module::File::new($path_expr).unwrap()),
            )*
            _ => panic!()
        }
    };
}

#[allow(non_camel_case_types)]
pub enum AutoFile {
    tirp(crate::io::format::tirp::File),
    zip(crate::io::format::zip::File),
}

pub fn which_file<P: AsRef<std::path::Path>>(path: P) -> AutoFile {
    let path = path.as_ref().to_path_buf();
    let ext = path.extension().and_then(|s| s.to_str());
    file_valid_formats!(ext, path; tirp, zip)
}

// detect stream
macro_rules! stream_valid_formats {
    ($file_expr:expr; $($module:ident),*) => {
        match $file_expr {
            $(
                AutoFile::$module(inner) => AutoStream::$module(crate::io::format::$module::Stream::new(&inner)),
            )*
            _ => panic!()
        }
    };
}

#[enum_dispatch(BascetStream)]
#[allow(non_camel_case_types)]
pub enum AutoStream {
    tirp(crate::io::tirp::Stream),
}

pub fn which_stream(file: AutoFile) -> AutoStream {
    stream_valid_formats!(file; tirp)
}

#[enum_dispatch(BascetStreamToken)]
#[allow(non_camel_case_types)]
pub enum AutoToken {
    tirp(crate::io::tirp::StreamToken),
}
