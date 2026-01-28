
#[macro_export]
macro_rules! log_filter_parser {
    ($filter_type:ty) => {
        clap::builder::TypedValueParser::try_map(
            clap::builder::NonEmptyStringValueParser::new(),
            |s| <$filter_type>::try_new(s),
        )
    };
}

#[macro_export]
macro_rules! log_strictness_parser {
    ($strictness_type:ty) => {
        clap::builder::TypedValueParser::try_map(
            clap::builder::NonEmptyStringValueParser::new(),
            |s| -> Result<$strictness_type, String> {
                match s.to_lowercase().as_str() {
                    "ignore" => Ok(<$strictness_type>::Ignore),
                    "strict" => Ok(<$strictness_type>::Strict),
                    other => other
                        .parse::<u64>()
                        .map(<$strictness_type>::Lenient)
                        .map_err(|_| {
                            format!("Expected 'ignore', 'strict', or a number, got '{}'", s)
                        }),
                }
            },
        )
    };
}

#[macro_export]
macro_rules! log_mode_parser {
    ($mode_type:ty) => {
        paste::paste! {
            clap::builder::TypedValueParser::try_map(
                clap::builder::NonEmptyStringValueParser::new(),
                |s| -> Result<$mode_type, String> {
                    match s.as_str() {
                        "terminal" => Ok([<$mode_type>] { terminal: true, file: None }),
                        "discard" => Ok([<$mode_type>] { terminal: false, file: None }),
                        path => {
                            let file = clio::OutputPath::new(path)
                                .map_err(|e| format!("Invalid path '{}': {}", path, e))?;
                            Ok([<$mode_type>] { terminal: true, file: Some(file) })
                        }
                    }
                },
            )
        }
    };
}

#[macro_export]
macro_rules! log_ordered_parser {
    ($ordered_type:ty) => {
        clap::builder::TypedValueParser::try_map(
            clap::builder::NonEmptyStringValueParser::new(),
            |s| -> Result<$ordered_type, String> {
                let mut result = <$ordered_type>::empty();

                for part in s.split(',') {
                    match part.trim() {
                        "terminal" => result |= <$ordered_type>::TERMINAL,
                        "file" => result |= <$ordered_type>::FILE,
                        "none" => {}
                        other => {
                            return Err(format!("Unknown '{}'. Expected: terminal, file, none", other))
                        }
                    }
                }

                Ok(result)
            },
        )
    };
}
