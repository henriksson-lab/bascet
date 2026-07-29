pub(crate) enum Exception {
    HWUnavailableTopology,
    HWUnavailableAffinity,
    HWUnavailableCores,
    HWUnavailableKinds,
    NoWorkers,
    HWFailureSetAffinity,
    BurnExceedsCores { burn: usize, cores: usize },
    BurnExceedsLogical { burn: usize, logical: usize },
}

impl Exception {
    pub(crate) fn log(self) {
        match self {
            Exception::HWUnavailableTopology => {
                tracing::warn!(
                    "hardware topology unavailable; distributing over logical core count only"
                )
            }
            Exception::HWUnavailableAffinity => {
                tracing::warn!(
                    "cpu binding unsupported on this platform; workers will run unpinned"
                )
            }
            Exception::HWUnavailableCores => {
                tracing::warn!("no physical cores reported; treating each logical core as its own")
            }
            Exception::HWUnavailableKinds => {
                tracing::debug!("cpu kinds unavailable; burn placed without performance ranking")
            }
            Exception::NoWorkers => {
                tracing::warn!("no workers configured; the pipeline will not make progress")
            }
            Exception::HWFailureSetAffinity => {
                tracing::warn!("a thread failed to pin to its cores")
            }
            Exception::BurnExceedsCores { burn, cores } => {
                tracing::warn!(
                    "{burn} burn workers exceed {cores} physical cores; pinning to logical cores"
                )
            }
            Exception::BurnExceedsLogical { burn, logical } => {
                tracing::warn!(
                    "cannot pin all burn workers: {burn} requested but only {logical} logical cores"
                )
            }
        }
    }
}
