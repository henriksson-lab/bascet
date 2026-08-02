use tracing::Level;

use crate::exception::Raise;

#[derive(Debug, thiserror::Error)]
pub enum Exception {
    #[error("cpu topology unavailable; distributing over logical cores only")]
    HWUnavailableTopology,
    #[error("cpu affinity unavailable; workers run unpinned")]
    HWUnavailableAffinity,
    #[error("cpu core kinds unavailable; treating each core as logical")]
    HWUnavailableCores,
    #[error("cpu core clock speeds unavailable; placing workers unranked")]
    HWUnavailableClock,

    #[error("cpu binding failed; thread runs unpinned")]
    HWFailureSetAffinity,

    #[error("insufficient physical cores; using logical cores")]
    HWFailureInsufficientCoresPhysical,

    #[error("insufficient logical cores; truncating to available cores")]
    HWFailureInsufficientCoresLogical,

    #[error("insufficient parallelism")]
    HWFailureInsufficientParallelism,
}

impl Raise for Exception {
    fn level(&self) -> Level {
        match self {
            Exception::HWUnavailableTopology
            | Exception::HWUnavailableAffinity
            | Exception::HWUnavailableClock
            | Exception::HWUnavailableCores
            | Exception::HWFailureSetAffinity
            | Exception::HWFailureInsufficientCoresPhysical
            | Exception::HWFailureInsufficientCoresLogical => Level::WARN,
            Exception::HWFailureInsufficientParallelism => Level::ERROR,
        }
    }
}
