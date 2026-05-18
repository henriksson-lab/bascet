use bytesize::ByteSize;

pub fn current_rss_bytes() -> Option<u64> {
    memory_stats::memory_stats().map(|memory| memory.physical_mem as u64)
}

pub fn current_rss_display() -> String {
    bytes_display(current_rss_bytes())
}

pub fn max_rss_bytes() -> Option<u64> {
    max_rss_bytes_platform()
}

pub fn max_rss_display() -> String {
    bytes_display(max_rss_bytes())
}

pub fn process_cpu_seconds() -> Option<f64> {
    process_cpu_seconds_platform()
}

pub fn thread_cpu_seconds() -> Option<f64> {
    thread_cpu_seconds_platform()
}

fn bytes_display(bytes: Option<u64>) -> String {
    bytes
        .map(|bytes| ByteSize(bytes).to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

#[cfg(unix)]
fn max_rss_bytes_platform() -> Option<u64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let usage = unsafe { usage.assume_init() };
    Some(max_rss_to_bytes(usage.ru_maxrss))
}

#[cfg(not(unix))]
fn max_rss_bytes_platform() -> Option<u64> {
    None
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn max_rss_to_bytes(max_rss: libc::c_long) -> u64 {
    (max_rss as u64).saturating_mul(1024)
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
fn max_rss_to_bytes(max_rss: libc::c_long) -> u64 {
    max_rss as u64
}

#[cfg(unix)]
fn process_cpu_seconds_platform() -> Option<f64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let rc = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let usage = unsafe { usage.assume_init() };
    Some(timeval_seconds(usage.ru_utime) + timeval_seconds(usage.ru_stime))
}

#[cfg(not(unix))]
fn process_cpu_seconds_platform() -> Option<f64> {
    None
}

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
fn thread_cpu_seconds_platform() -> Option<f64> {
    let mut time = std::mem::MaybeUninit::<libc::timespec>::uninit();
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, time.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let time = unsafe { time.assume_init() };
    Some(time.tv_sec as f64 + time.tv_nsec as f64 / 1_000_000_000.0)
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
)))]
fn thread_cpu_seconds_platform() -> Option<f64> {
    None
}

#[cfg(unix)]
fn timeval_seconds(value: libc::timeval) -> f64 {
    value.tv_sec as f64 + value.tv_usec as f64 / 1_000_000.0
}
