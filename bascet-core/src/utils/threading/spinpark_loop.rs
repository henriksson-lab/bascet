pub const SPINPARK_PARK_DURATION: core::time::Duration = std::time::Duration::from_micros(50);
pub const SPINPARK_COUNTOF_PARKS_BEFORE_WARN: usize = (std::time::Duration::from_secs(15).as_micros()
    / SPINPARK_PARK_DURATION.as_micros())
    as usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpinPark {
    Spun,
    Parked,
    Warn,
}

#[inline(always)]
pub fn spinpark_loop<const MAX_SPINS: usize, const PARKS_BEFORE_WARN: usize>(
    spinpark_counter: &mut usize,
) -> SpinPark {
    match *spinpark_counter < MAX_SPINS {
        true => {
            *spinpark_counter += 1;
            std::hint::spin_loop();
            SpinPark::Spun
        }
        false => {
            *spinpark_counter += 1;
            spinpark_loop_slow::<MAX_SPINS, PARKS_BEFORE_WARN>(*spinpark_counter)
        }
    }
}

#[cold]
fn spinpark_loop_slow<const MAX_SPINS: usize, const PARKS_BEFORE_WARN: usize>(
    spinpark_counter: usize,
) -> SpinPark {
    let countof_parks = spinpark_counter / MAX_SPINS;
    let countof_warns = countof_parks / PARKS_BEFORE_WARN;

    let duration = match countof_warns {
        0 => SPINPARK_PARK_DURATION.mul_f64(1.0 + (countof_parks as f64 * 0.1)),
        1.. => {
            let exp_multiplier = (2.0 as f64).powi(countof_warns as i32).min(PARKS_BEFORE_WARN as f64);
            SPINPARK_PARK_DURATION.mul_f64(exp_multiplier)
        }
    };

    std::thread::park_timeout(duration);

    match spinpark_counter % PARKS_BEFORE_WARN == 0 {
        true => SpinPark::Warn,
        false => SpinPark::Parked,
    }
}
