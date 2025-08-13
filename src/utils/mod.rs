mod command_to_string;
mod detect_software;
mod merge_archives;
mod path_utils;
<<<<<<< HEAD
mod umi_dedup;
=======
>>>>>>> main

pub use merge_archives::merge_archives;
pub use merge_archives::merge_archives_and_delete;

pub use detect_software::check_bgzip;
pub use detect_software::check_kmc_tools;
pub use detect_software::check_samtools;
pub use detect_software::check_tabix;

<<<<<<< HEAD
pub use umi_dedup::dedup_umi;

=======
>>>>>>> main
pub use path_utils::expand_and_resolve;

pub use command_to_string::command_to_string;
pub use detect_software::get_bascet_datadir;
<<<<<<< HEAD
=======

>>>>>>> main
