#[macro_export]
macro_rules! support_which_input {
    (
        $enum_name:ident
        for formats [$($format:ident),* $(,)?]
    ) => {
        paste::paste! {
            #[enum_dispatch::enum_dispatch(BascetFile)]
            pub enum $enum_name {
                $(
                    [<$format:camel>](crate::io::format::$format::Input),
                )*
            }

            impl $enum_name {
                pub fn try_from_path<P: AsRef<std::path::Path>>(path: P) -> Result<Self, crate::runtime::Error> {
                    let path = path.as_ref();
                    $(
                        let res = crate::io::format::$format::Input::new(path);
                        if let Ok(inner) = res {
                            crate::log_info!("Detected format: {}", stringify!($format));
                            return Ok($enum_name::[<$format:camel>](inner));
                        } else if let Err(e) = res {
                            return Err(crate::runtime::Error::file_not_valid(
                                path,
                                Some(format!("No supported input format could handle this file ({})", e))
                            ));
                        }
                    )*
                    unreachable!();
                }
            }
        }
    };
}

#[macro_export]
macro_rules! support_which_output {
    (
        $enum_name:ident
        for formats [$($format:ident),* $(,)?]
    ) => {
        paste::paste! {
            #[enum_dispatch::enum_dispatch(BascetFile)]
            pub enum $enum_name {
                $(
                    [<$format:camel>](crate::io::format::$format::Output),
                )*
            }

            impl $enum_name {
                pub fn try_from_path<P: AsRef<std::path::Path>>(path: P) -> Result<Self, crate::runtime::Error> {
                    let path = path.as_ref();
                    $(
                        let res = crate::io::format::$format::Output::new(path);
                        if let Ok(inner) = res {
                            crate::log_info!("Detected format: {}", stringify!($format));
                            return Ok($enum_name::[<$format:camel>](inner));
                        }
                        if let Err(e) = res {
                            return Err(crate::runtime::Error::file_not_valid(
                                path,
                                Some(format!("No supported input format could handle this file ({})", e))
                            ));
                        }
                        unreachable!();
                    )*
                }
            }
        }
    };
}

#[macro_export]
macro_rules! support_which_temp {
    (
        $enum_name:ident
        for formats [$($format:ident),* $(,)?]
    ) => {
        paste::paste! {
            #[enum_dispatch::enum_dispatch(BascetFile, BascetTempFile)]
            pub enum $enum_name {
                $(
                    [<$format:camel>](crate::io::format::$format::Temp),
                )*
            }

            impl $enum_name {
                pub fn with_unique_name(extension: &str) -> Result<Self, crate::runtime::Error> {
                    $(
                        let res = crate::io::format::$format::Temp::with_unique_name(extension);
                        if let Ok(inner) = res {
                            crate::log_info!("Detected format: {}", stringify!($format));
                            return Ok($enum_name::[<$format:camel>](inner));
                        } 
                        if let Err(e) = res {
                            return Err(crate::runtime::Error::file_not_valid(
                                path,
                                Some(format!("No supported input format could handle this file ({})", e))
                            ));
                        }
                        unreachable!();
                    )*
                }

                pub fn with_unique_name_in<P: AsRef<std::path::Path>>(dir: P, extension: &str) -> Result<Self, crate::runtime::Error> {
                    $(
                        let res = crate::io::format::$format::::Temp::with_unique_name_in(&dir, extension);
                        if let Ok(inner) = res {
                            crate::log_info!("Detected format: {}", stringify!($format));
                            return Ok($enum_name::[<$format:camel>](inner));
                        } 
                        if let Err(e) = res {
                            return Err(crate::runtime::Error::file_not_valid(
                                path,
                                Some(format!("No supported input format could handle this file ({})", e))
                            ));
                        }
                        unreachable!();
                    )*
                }
            }
        }
    };
}

#[macro_export]
macro_rules! support_which_stream {
    (
        $input_enum:ident => $enum_name:ident<$generic:ident: $trait_bound:path>
        for formats [$($format:ident),* $(,)?]
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
                pub fn try_from_input(input: $input_enum) -> Result<Self, crate::runtime::Error> {
                    $(
                        if let $input_enum::[<$format:camel>](file) = input {
                            return Ok($enum_name::[<$format:camel>](
                                crate::io::format::$format::Stream::<$generic>::new(&file)?
                            ));
                        }
                    )*
                    unreachable!();
                }
            }
        }
    };
}

#[macro_export]
macro_rules! support_which_writer {
    (
        $output_enum:ident => $enum_name:ident<$generic:ident: $trait_bound:path>
        for formats [$($format:ident),* $(,)?]
    ) => {
        paste::paste! {
            #[enum_dispatch::enum_dispatch(BascetWrite<$generic>)]
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
                pub fn try_from_output(output: $output_enum) -> Result<Self, crate::runtime::Error> {
                    $(
                        if let $output_enum::[<$format:camel>](_) = output {
                            return Ok($enum_name::[<$format:camel>](
                                crate::io::format::$format::Writer::<$generic>::new()?
                            ));
                        }
                    )*
                    unreachable!();
                }
            }
        }
    };
}
