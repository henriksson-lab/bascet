mod clap_utils;
mod command_to_string;
mod detect_software;
mod fs_utils;
mod merge_archives;
mod path_utils;
mod tabix_bed;

pub use merge_archives::merge_archives;
pub use merge_archives::merge_archives_and_delete;

pub use detect_software::check_kmc_tools;

pub use fs_utils::{
    atomic_temp_path, atomic_temp_path_in_dir, publish_atomic_output,
    rename_or_copy_across_filesystems,
};
pub use path_utils::expand_and_resolve;
pub use tabix_bed::BedTabixIndexer;

pub use command_to_string::command_to_string;
pub use detect_software::get_bascet_datadir;
