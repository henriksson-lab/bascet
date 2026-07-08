pub const SPINPARK_PARK_DURATION: core::time::Duration = std::time::Duration::from_micros(50);
pub const SPINPARK_COUNTOF_PARKS_BEFORE_WARN: usize =
    (std::time::Duration::from_secs(15).as_micros() / SPINPARK_PARK_DURATION.as_micros()) as usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpinPark {
    Spun,
    Parked,
    Warn,
}

impl SpinPark {
    #[inline(always)]
    pub fn run<const MAX_SPINS: usize, const PARKS_BEFORE_WARN: usize>(
        spinpark_counter: &mut usize,
    ) -> Self {
        match *spinpark_counter < MAX_SPINS {
            true => {
                *spinpark_counter += 1;
                std::hint::spin_loop();
                Self::Spun
            }
            false => {
                *spinpark_counter += 1;
                Self::slow::<MAX_SPINS, PARKS_BEFORE_WARN>(*spinpark_counter)
            }
        }
    }

    #[cold]
    fn slow<const MAX_SPINS: usize, const PARKS_BEFORE_WARN: usize>(
        spinpark_counter: usize,
    ) -> Self {
        let countof_parks = spinpark_counter / MAX_SPINS;
        let countof_warns = countof_parks / PARKS_BEFORE_WARN;

        let duration = match countof_warns {
            0 => SPINPARK_PARK_DURATION,
            1.. => {
                let exp_multiplier = (2.0_f64)
                    .powi(countof_warns as i32)
                    .min(PARKS_BEFORE_WARN as f64);
                SPINPARK_PARK_DURATION.mul_f64(exp_multiplier)
            }
        };

        std::thread::park_timeout(duration);

        match spinpark_counter % PARKS_BEFORE_WARN == 0 {
            true => Self::Warn,
            false => Self::Parked,
        }
    }
}
