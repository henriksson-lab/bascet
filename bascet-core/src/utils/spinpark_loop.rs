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
    if *spinpark_counter < MAX_SPINS {
        *spinpark_counter += 1;
        std::hint::spin_loop();
        SpinPark::Spin
    } else {
        // *spinpark_counter = 0;
        spinpark_loop_slow();
        SpinPark::Park
    }
}

#[cold]
fn spinpark_loop_slow() {
    std::thread::park_timeout(SPINPARK_PARK_MICROS_TIMEOUT);
}
