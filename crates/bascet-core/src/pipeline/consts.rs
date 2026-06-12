use std::time::Duration;

pub(crate) const RES_QUEUE_MAX: usize = 32;
pub(crate) const REQ_DEPTH_MAX: usize = 32;

pub(crate) const STALL_WARN: Duration = Duration::from_millis(512);
pub(crate) const STALL_HARD: Duration = Duration::from_secs(8);
pub(crate) const TASK_IDLE_TIMEOUT: Duration = Duration::from_millis(100);

pub(crate) const BURN_PATIENCE_INITIAL: u32 = 512;
pub(crate) const BURN_PATIENCE_GROWTH: u32 = 2;
pub(crate) const BURN_PATIENCE_DECAY: u32 = 1;
pub(crate) const BURN_PATIENCE_MAX: u32 = 1024;

pub(crate) const JOB_PATIENCE_INITIAL: u32 = 32;
pub(crate) const JOB_PATIENCE_GROWTH: u32 = 2;
pub(crate) const JOB_PATIENCE_DECAY: u32 = 1;
pub(crate) const JOB_PATIENCE_MAX: u32 = 256;

pub(crate) const PRESSURE_INITIAL: u32 = 0;
pub(crate) const PRESSURE_MIN: u32 = 0;
pub(crate) const PRESSURE_STRAIN: u32 = 1024;
pub(crate) const PRESSURE_GROWTH: u32 = 2;
pub(crate) const PRESSURE_DECAY: u32 = 1;
pub(crate) const PRESSURE_GROWTH_MIN: u32 = 1;
pub(crate) const PRESSURE_GROWTH_MAX: u32 = 8;
pub(crate) const PRESSURE_DECAY_MIN: u32 = 1;
pub(crate) const PRESSURE_DECAY_MAX: u32 = 512;

pub(crate) const DEMAND_INITIAL: u32 = 0;
pub(crate) const DEMAND_MIN: u32 = 0;
pub(crate) const DEMAND_STRAIN: u32 = 1024;
pub(crate) const DEMAND_GROWTH: u32 = 2;
pub(crate) const DEMAND_DECAY: u32 = 1;
pub(crate) const DEMAND_GROWTH_MIN: u32 = 1;
pub(crate) const DEMAND_GROWTH_MAX: u32 = 8;
pub(crate) const DEMAND_DECAY_MIN: u32 = 1;
pub(crate) const DEMAND_DECAY_MAX: u32 = 512;

pub(crate) const SENSITIVITY_UP: (u32, u32) = (11, 10);
pub(crate) const SENSITIVITY_DOWN: (u32, u32) = (10, 11);
