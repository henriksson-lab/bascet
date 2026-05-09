use std::{
    fs, io,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

/// Return a hidden sibling temp path for writing a final output.
///
/// Writing in the destination directory keeps the final publish step atomic on
/// the common path, because the temp file and destination live on the same
/// filesystem.
pub fn atomic_temp_path(final_path: impl AsRef<Path>) -> PathBuf {
    let final_path = final_path.as_ref();
    let parent = final_path.parent().unwrap_or_else(|| Path::new("."));
    atomic_temp_path_in_dir(final_path, parent)
}

/// Return a hidden temp path under `temp_dir` for writing `final_path`.
///
/// This is useful for commands where per-job temporary output must stay out of
/// the final output directory until publish time.
pub fn atomic_temp_path_in_dir(
    final_path: impl AsRef<Path>,
    temp_dir: impl AsRef<Path>,
) -> PathBuf {
    let final_path = final_path.as_ref();
    let temp_dir = temp_dir.as_ref();
    let file_name = final_path
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("output"))
        .to_string_lossy();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);

    temp_dir.join(format!(
        ".{file_name}.{}.{}.tmp",
        std::process::id(),
        timestamp
    ))
}

pub fn publish_atomic_output(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    rename_or_copy_across_filesystems(from, to)
}

/// Move a completed file into place.
///
/// This uses an atomic rename when source and destination are on the same
/// filesystem. If the paths cross filesystem boundaries, it falls back to
/// copy-and-delete because POSIX rename returns EXDEV in that case.
pub fn rename_or_copy_across_filesystems(
    from: impl AsRef<Path>,
    to: impl AsRef<Path>,
) -> io::Result<()> {
    let from = from.as_ref();
    let to = to.as_ref();

    match fs::rename(from, to) {
        Ok(()) => Ok(()),
        Err(err) if is_cross_device_link(&err) => {
            copy_to_temp_then_rename(from, to)?;
            fs::remove_file(from)
        }
        Err(err) => Err(err),
    }
}

fn copy_to_temp_then_rename(from: &Path, to: &Path) -> io::Result<()> {
    let copy_temp = copy_temp_path(to);
    if copy_temp.exists() {
        fs::remove_file(&copy_temp)?;
    }

    fs::copy(from, &copy_temp)?;
    fs::rename(&copy_temp, to)
}

fn copy_temp_path(to: &Path) -> PathBuf {
    let mut extension = to.extension().unwrap_or_default().to_os_string();
    if extension.is_empty() {
        extension.push("tmp");
    } else {
        extension.push(".tmp");
    }
    to.with_extension(extension)
}

fn is_cross_device_link(err: &io::Error) -> bool {
    err.raw_os_error() == Some(18)
}
