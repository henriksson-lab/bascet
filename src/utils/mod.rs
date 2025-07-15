mod merge_archives;
mod detect_software;

pub use merge_archives::merge_archives;
pub use merge_archives::merge_archives_and_delete;

pub use detect_software::check_bgzip;
pub use detect_software::check_tabix;
pub use detect_software::check_samtools;
pub use detect_software::check_kmc_tools;
pub use detect_software::get_bascet_datadir;

