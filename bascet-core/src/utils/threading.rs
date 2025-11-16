#[inline(always)]
pub fn spinpark_loop<const MAX_SPINS: usize>(spinpark_counter: &mut usize) {
    if *spinpark_counter < MAX_SPINS {
        *spinpark_counter += 1;
        std::hint::spin_loop();
    } else {
        // yield CPU for a few µs
        *spinpark_counter = 0;
        spinpark_loop_slow();
    }
}

#[cold]
fn spinpark_loop_slow() {
    std::thread::park_timeout(std::time::Duration::from_micros(50));
}
