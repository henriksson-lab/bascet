use anyhow::Result;
use clap::Args;

#[derive(Args)]
pub struct SysinfoCMD {
    #[arg(long = "info")]
    pub info: String,
}
impl SysinfoCMD {
    pub fn try_execute(&self) -> Result<()> {
        print!("{}", get_info(&self.info)?);
        Ok(())
    }
}

#[cfg(not(target_os = "macos"))]
fn get_info(info: &str) -> Result<String> {
    use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

    if info == "cpu" {
        let s = System::new_with_specifics(
            RefreshKind::nothing().with_cpu(CpuRefreshKind::everything()),
        );
        Ok(s.cpus()
            .first()
            .map(|cpu| cpu.brand().to_string())
            .unwrap_or_default())
    } else if info == "totalmem" {
        let s = System::new_with_specifics(
            RefreshKind::nothing().with_memory(MemoryRefreshKind::everything()),
        );
        Ok(s.total_memory().to_string()) // bytes
    } else {
        Ok("Invalid".to_string())
    }
}

#[cfg(target_os = "macos")]
fn get_info(info: &str) -> Result<String> {
    match info {
        "cpu" => sysctl_string("machdep.cpu.brand_string"),
        "totalmem" => sysctl_u64("hw.memsize").map(|value| value.to_string()),
        _ => Ok("Invalid".to_string()),
    }
}

#[cfg(target_os = "macos")]
fn sysctl_string(name: &str) -> Result<String> {
    let bytes = sysctl_bytes(name)?;
    let text = bytes.split(|byte| *byte == 0).next().unwrap_or(&bytes);
    Ok(String::from_utf8_lossy(text).into_owned())
}

#[cfg(target_os = "macos")]
fn sysctl_u64(name: &str) -> Result<u64> {
    use anyhow::Context;

    let bytes = sysctl_bytes(name)?;
    let mut value = [0u8; std::mem::size_of::<u64>()];
    let bytes = bytes
        .get(..value.len())
        .context("sysctl value is shorter than expected")?;
    value.copy_from_slice(bytes);
    Ok(u64::from_ne_bytes(value))
}

#[cfg(target_os = "macos")]
fn sysctl_bytes(name: &str) -> Result<Vec<u8>> {
    use anyhow::Context;
    use std::ffi::CString;

    let name = CString::new(name).context("sysctl name contains an interior NUL byte")?;
    let mut len = 0usize;
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            std::ptr::null_mut(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error()).context("failed to query sysctl value length");
    }

    let mut bytes = vec![0u8; len];
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            bytes.as_mut_ptr().cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error()).context("failed to query sysctl value");
    }
    bytes.truncate(len);
    Ok(bytes)
}
