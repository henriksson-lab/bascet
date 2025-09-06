#[inline(always)]
pub fn spin_or_park(spin_counter: &mut usize, max_spins: usize) {
    if *spin_counter < max_spins {
        *spin_counter += 1;
        std::hint::spin_loop();
    } else {
        // yield CPU for a few us
        // *spin_counter = 0;
        std::thread::park_timeout(std::time::Duration::from_micros(50));
    }
}
