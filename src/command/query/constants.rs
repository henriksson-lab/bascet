pub const QUERY_DEFAULT_PATH_IN: &str = "-";
pub const QUERY_DEFAULT_PATH_TEMP: &str = "tmp";
pub const QUERY_DEFAULT_PATH_OUT: &str = "features.mm";
pub const QUERY_DEFAULT_FEATURES_REF_MIN: usize = 100_0000;
pub const QUERY_DEFAULT_FEATURES_REF_MAX: usize = 10_000;
pub const QUERY_DEFAULT_FEATURES_QUERY_MIN: usize = 10_0000;
pub const QUERY_DEFAULT_FEATURES_QUERY_MAX: usize = 1_000;
// \t[0-9]{10}\n
// (4 294 967 296) is max value for kmer counts, thats 10 digits :)
pub const OVLP_DIGITS: usize = 12;
