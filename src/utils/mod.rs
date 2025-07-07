mod detect_software;
mod expand_and_resolve_path;
mod merge_archives;
mod umi_dedup;

pub use merge_archives::merge_archives;
pub use merge_archives::merge_archives_and_delete;

pub use detect_software::check_bgzip;
pub use detect_software::check_kmc_tools;
pub use detect_software::check_samtools;
pub use detect_software::check_tabix;

pub use umi_dedup::dedup_umi;

pub use expand_and_resolve_path::expand_and_resolve_path;
