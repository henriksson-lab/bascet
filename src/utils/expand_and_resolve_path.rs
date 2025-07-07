use anyhow::{Context, Result};
use shellexpand;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

/// Expands ~ and env vars if possible (only for UTF-8 paths), and always returns an absolute PathBuf.
/// Prints a warning if the path is not valid UTF-8 or expansion fails, but still makes the path absolute.
/// Does NOT fail if the file does not exist.
pub fn expand_and_resolve_path<P: AsRef<Path>>(input: P) -> Result<PathBuf> {
    let input = input.as_ref();
    let expanded: PathBuf = match input.to_str() {
        Some(s) => {
            if let Ok(expanded) = shellexpand::full(s) {
                PathBuf::from(expanded.as_ref())
            } else {
                eprintln!(
                    "Warning: Failed to expand path {:?}. Using original path.",
                    input
                );
                input.to_path_buf()
            }
        }
        None => {
            eprintln!(
                "Warning: Path {:?} is not valid UTF-8. Skipping path expansion.",
                input
            );
            input.to_path_buf()
        }
    };

    // Try canonicalize, else make absolute
    if let Ok(absolute) = fs::canonicalize(&expanded) {
        return Ok(absolute);
    }
    let abs = if expanded.is_absolute() {
        expanded
    } else {
        env::current_dir()
            .context("Failed to get current directory")?
            .join(expanded)
    };
    Ok(abs)
}
