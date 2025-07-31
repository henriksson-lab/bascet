#[macro_export]
macro_rules! support_which_files {
    (
        $enum_name:ident
        for formats [$($format:ident),*]
    ) => {
        paste::paste! {
            pub enum $enum_name {
                $(
                    [<$format:camel>](crate::io::format::$format::File),
                )*
            }

            impl $enum_name {
                pub fn try_from_path<P: AsRef<std::path::Path>>(path: P) -> Result<Self, crate::runtime::Error> {
                    let path = path.as_ref().to_path_buf();
                    $(
                        if let Ok(inner) = crate::io::format::$format::File::new(&path) {
                            return Ok($enum_name::[<$format:camel>](inner));
                        }
                    )*
                    Err(crate::runtime::Error::file_not_valid(
                        "try_from_file",
                        Some("Unsupported file format!")
                    ))
                }
            }
        }
    };
}

#[macro_export]
macro_rules! support_which_stream {
    (
        $enum_name:ident<$generic:ident: $trait_bound:path>
        for formats [$($format:ident),*]
    ) => {
        paste::paste! {
            #[enum_dispatch::enum_dispatch(BascetStream<$generic>)]
            pub enum $enum_name<$generic>
            where
                $generic: $trait_bound + 'static,
            {
                $(
                    [<$format:camel>](crate::io::format::$format::Stream<$generic>),
                )*
            }

            impl<$generic> $enum_name<$generic>
            where
                $generic: $trait_bound + 'static,
            {
                pub fn try_from_file(file: crate::io::format::AutoBascetFile) -> Result<Self, crate::runtime::Error> {
                    $(
                        if let crate::io::format::AutoBascetFile::[<$format:camel>](ref file_inner) = file {
                            return Ok($enum_name::[<$format:camel>](
                                crate::io::format::$format::Stream::<$generic>::new(file_inner)?
                            ));
                        }
                    )*
                    Err(crate::runtime::Error::file_not_valid(
                        "try_from_file",
                        Some("Unsupported file format!")
                    ))
                }
            }
        }
    };
}

#[macro_export]
macro_rules! support_which_writer {
    (
        $enum_name:ident<$generic:ident: $trait_bound:path>
        for formats [$($format:ident),*]
    ) => {
        paste::paste! {
            #[enum_dispatch::enum_dispatch(BascetWriter<$generic>)]
            pub enum $enum_name<$generic>
            where
                $generic: $trait_bound + 'static,
            {
                $(
                    [<$format:camel>](crate::io::format::$format::Writer<$generic>),
                )*
            }

            impl<$generic> $enum_name<$generic>
            where
                $generic: $trait_bound + 'static,
            {
                pub fn try_from_file(file: crate::io::format::AutoBascetFile) -> Result<Self, crate::runtime::Error> {
                    $(
                        if let crate::io::format::AutoBascetFile::[<$format:camel>](_) = file {
                            return Ok($enum_name::[<$format:camel>](
                                crate::io::format::$format::Writer::<$generic>::new()?
                            ));
                        }
                    )*
                    Err(crate::runtime::Error::file_not_valid(
                        "try_from_file",
                        Some("Unsupported file format!")
                    ))
                }
            }
        }
    };
}
