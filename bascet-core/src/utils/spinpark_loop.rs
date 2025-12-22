use crate::likely_unlikely;

pub const SPINPARK_PARK_MICROS_TIMEOUT: core::time::Duration = std::time::Duration::from_micros(50);
pub const SPINPARK_PARKS_BEFORE_WARN: usize = (std::time::Duration::from_secs(15).as_micros()
    / SPINPARK_PARK_MICROS_TIMEOUT.as_micros())
    as usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpinPark {
    Spin,
    Park,
}

#[inline(always)]
pub fn spinpark_loop<const MAX_SPINS: usize>(spinpark_counter: &mut usize) -> SpinPark {
    spinpark_loop_warn::<MAX_SPINS, 0>(spinpark_counter, "")
}

#[inline(always)]
pub fn spinpark_loop_warn<const MAX_SPINS: usize, const PARKS_BEFORE_MSG: usize>(
    spinpark_counter: &mut usize,
    msg: &str,
) -> SpinPark {
    if *spinpark_counter < MAX_SPINS {
        *spinpark_counter += 1;
        std::hint::spin_loop();
        SpinPark::Spin
    } else {
        *spinpark_counter += 1;
        spinpark_loop_slow::<PARKS_BEFORE_MSG>(*spinpark_counter - MAX_SPINS, msg);
        SpinPark::Park
    }
}

#[cold]
fn spinpark_loop_slow<const PARKS_BEFORE_MSG: usize>(park_count: usize, msg: &str) {
    if likely_unlikely::unlikely(PARKS_BEFORE_MSG > 0 && park_count % PARKS_BEFORE_MSG == 0) {
        eprintln!(
            "[SPINPARK WARNING] Parked {} times ({}ms): {}",
            PARKS_BEFORE_MSG,
            (PARKS_BEFORE_MSG as u128 * SPINPARK_PARK_MICROS_TIMEOUT.as_micros()) / 1000,
            msg
        );
    }
    std::thread::park_timeout(SPINPARK_PARK_MICROS_TIMEOUT);
}
