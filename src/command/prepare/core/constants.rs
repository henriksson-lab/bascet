pub static CB_PATTERN: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| regex::Regex::new(r"[A-Z]+[0-9]+-[A-Z]+[0-9]+-[A-Z]+[0-9]+").unwrap());