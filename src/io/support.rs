#[macro_export]
macro_rules! support_which_stream {
    (
        $input_enum:ident => $stream_enum:ident<$generic:ident: $trait_bound:path>
        for formats [$($format:ident),* $(,)?]
    ) => {
        $crate::__generate_input_enum! {
            $input_enum for formats [$($format),*]
        }

        $crate::__generate_stream_enum! {
            $input_enum => $stream_enum<$generic: $trait_bound> for formats [$($format),*]
        }
    };
}
#[macro_export]
macro_rules! support_which_writer {
    (
        $output_enum:ident => $writer_enum:ident<$generic:ident: $trait_bound:path>
        for formats [$($format:ident),* $(,)?]
    ) => {
        $crate::__generate_output_enum! {
            $output_enum for formats [$($format),*]
        }

        $crate::__generate_writer_enum! {
            $output_enum => $writer_enum<$generic: $trait_bound> for formats [$($format),*]
        }
    };
}
#[macro_export]
macro_rules! support_which_reader {
    (
        $input_enum:ident => $reader_enum:ident<$generic:ident: $trait_bound:path>
        for formats [$($format:ident),* $(,)?]
    ) => {
        $crate::__generate_input_enum! {
            $input_enum for formats [$($format),*]
        }

        $crate::__generate_reader_enum! {
            $input_enum => $reader_enum<$generic: $trait_bound> for formats [$($format),*]
        }
    };
}
#[macro_export]
macro_rules! support_which_temp {
    (
        $enum_name:ident
        for formats [$($format:ident),* $(,)?]
    ) => {
        $crate::__generate_temp_enum! {
            $enum_name for formats [$($format),*]
        }
    };
}

#[macro_export]
macro_rules! __generate_input_enum {
    ($enum_name:ident for formats [$($format:ident),* $(,)?]) => {
        paste::paste! {
            #[enum_dispatch::enum_dispatch(BascetFile)]
            pub enum $enum_name {
                $([<$format:camel>](crate::io::format::$format::Input),)*
            }

            impl $enum_name {
                #[allow(unused_assignments)]
                pub fn try_from_path<P: AsRef<std::path::Path>>(path: P) -> Result<Self, crate::runtime::Error> {
                    let path = path.as_ref();
                    let mut last_error: Option<crate::runtime::Error> = None;
                    $(
                        match crate::io::format::$format::Input::new(path) {
                            Ok(inner) => {
                                crate::log_info!("Detected input format: {}", stringify!($format));
                                return Ok(Self::[<$format:camel>](inner));
                            }
                            Err(e) => {
                                last_error = Some(e);
                            }
                        }
                    )*
                    Err(last_error.unwrap_or_else(||
                        crate::runtime::Error::file_not_valid(path, Some("No supported input format could handle this file"))
                    ))
                }
            }
        }
    };
}
#[macro_export]
macro_rules! __generate_output_enum {
    ($enum_name:ident for formats [$($format:ident),* $(,)?]) => {
        paste::paste! {
            #[enum_dispatch::enum_dispatch(BascetFile)]
            pub enum $enum_name {
                $([<$format:camel>](crate::io::format::$format::Output),)*
            }

            impl $enum_name {
                #[allow(unused_assignments)]
                pub fn try_from_path<P: AsRef<std::path::Path>>(path: P) -> Result<Self, crate::runtime::Error> {
                    let path = path.as_ref();
                    let mut last_error: Option<crate::runtime::Error> = None;
                    $(
                        match crate::io::format::$format::Output::new(path) {
                            Ok(inner) => {
                                crate::log_info!("Detected output format: {}", stringify!($format));
                                return Ok(Self::[<$format:camel>](inner));
                            }
                            Err(e) => {
                                last_error = Some(e);
                            }
                        }
                    )*
                    Err(last_error.unwrap_or_else(||
                        crate::runtime::Error::file_not_valid(path, Some("No supported output format could handle this file"))
                    ))
                }
            }
        }
    };
}
// #[macro_export]
// macro_rules! __generate_temp_enum {
//     ($enum_name:ident for formats [$($format:ident),* $(,)?]) => {
//         paste::paste! {
//             #[enum_dispatch::enum_dispatch(BascetFile, BascetTempFile)]
//             pub enum $enum_name {
//                 $([<$format:camel>](crate::io::format::$format::Temp),)*
//             }

//             impl $enum_name {
//                 pub fn with_unique_name(extension: &str) -> Result<Self, crate::runtime::Error> {
//                     $(
//                         if let Ok(inner) = crate::io::format::$format::Temp::with_unique_name(extension) {
//                             crate::log_info!("Using temp format: {}", stringify!($format));
//                             return Ok(Self::[<$format:camel>](inner));
//                         }
//                     )*
//                     Err(crate::runtime::Error::file_not_valid("temp", Some("No supported temp format could create file".to_string())))
//                 }

//                 pub fn with_unique_name_in<P: AsRef<std::path::Path>>(dir: P, extension: &str) -> Result<Self, crate::runtime::Error> {
//                     $(
//                         if let Ok(inner) = crate::io::format::$format::Temp::with_unique_name_in(&dir, extension) {
//                             crate::log_info!("Using temp format: {}", stringify!($format));
//                             return Ok(Self::[<$format:camel>](inner));
//                         }
//                     )*
//                     Err(crate::runtime::Error::file_not_valid("temp", Some("No supported temp format could create file".to_string())))
//                 }
//             }
//         }
//     };
// }
#[macro_export]
macro_rules! __generate_stream_enum {
    (
        $input_enum:ident => $stream_enum:ident<$generic:ident: $trait_bound:path>
        for formats [$($format:ident),* $(,)?]
    ) => {
        paste::paste! {
            #[enum_dispatch::enum_dispatch(BascetStream<$generic>)]
            pub enum $stream_enum<$generic>
            where
                $generic: $trait_bound + 'static,
            {
                $([<$format:camel>](crate::io::format::$format::Stream<$generic>),)*
            }

            impl<$generic> $stream_enum<$generic>
            where
                $generic: $trait_bound + 'static,
            {
                #[allow(irrefutable_let_patterns)]
                pub fn try_from_input(input: &$input_enum) -> Result<Self, crate::runtime::Error> {
                    $(
                        if let $input_enum::[<$format:camel>](file) = input {
                            return Ok(Self::[<$format:camel>](
                                crate::io::format::$format::Stream::<$generic>::new(&file)?
                            ));
                        }
                    )*
                    unreachable!()
                }
            }
        }
    };
}
#[macro_export]
macro_rules! __generate_writer_enum {
    (
        $output_enum:ident => $writer_enum:ident<$generic:ident: $trait_bound:path>
        for formats [$($format:ident),* $(,)?]
    ) => {
        paste::paste! {
            #[enum_dispatch::enum_dispatch(BascetWrite<$generic>)]
            pub enum $writer_enum<$generic>
            where
                $generic: $trait_bound + 'static,
            {
                $([<$format:camel>](crate::io::format::$format::Writer<$generic>),)*
            }

            impl<$generic> $writer_enum<$generic>
            where
                $generic: $trait_bound + 'static,
            {
                #[allow(irrefutable_let_patterns)]
                pub fn try_from_output(output: &$output_enum) -> Result<Self, crate::runtime::Error> {
                    $(
                        if let $output_enum::[<$format:camel>](_) = output {
                            return Ok(Self::[<$format:camel>](
                                crate::io::format::$format::Writer::<$generic>::new()?
                            ));
                        }
                    )*
                    unreachable!()
                }
            }
        }
    };
}
#[macro_export]
macro_rules! __generate_reader_enum {
    (
        $input_enum:ident => $reader_enum:ident<$generic:ident: $trait_bound:path>
        for formats [$($format:ident),* $(,)?]
    ) => {
        paste::paste! {
            #[enum_dispatch::enum_dispatch(BascetRead<$generic>)]
            pub enum $reader_enum<$generic>
            where
                $generic: $trait_bound + 'static,
            {
                $([<$format:camel>](crate::io::format::$format::Reader<$generic>),)*
            }

            impl<$generic> $reader_enum<$generic>
            where
                $generic: $trait_bound + 'static,
            {
                #[allow(irrefutable_let_patterns)]
                pub fn try_from_input(input: $input_enum) -> Result<Self, crate::runtime::Error> {
                    $(
                        if let $input_enum::[<$format:camel>](file) = input {
                            return Ok(Self::[<$format:camel>](
                                crate::io::format::$format::Reader::<$generic>::new(&file)?
                            ));
                        }
                    )*
                    unreachable!()
                }
            }
        }
    };
}
