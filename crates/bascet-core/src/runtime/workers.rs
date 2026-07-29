use std::sync::Arc;
use std::thread::JoinHandle;

use hwlocality::Topology;
use hwlocality::cpu::binding::CpuBindingFlags;
use hwlocality::cpu::cpuset::CpuSet;

use crate::runtime::Tier;
use crate::runtime::allocation::Allocation;
use crate::runtime::exception::Exception;

pub(crate) struct Workers {
    handles: Vec<JoinHandle<()>>,
}

impl Workers {
    pub(crate) fn spawn<F>(
        topology: Option<Arc<Topology>>,
        allocation: Allocation,
        mut run: impl FnMut(Tier) -> F,
    ) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        let mut handles = Vec::new();

        for (index, logical) in allocation.burn.into_iter().enumerate() {
            let topology = topology.clone();
            let run = run(Tier::Burn);
            handles.push(
                std::thread::Builder::new()
                    .name(format!("bascet-burn-{index}"))
                    .spawn(move || {
                        Self::pin(topology.as_deref(), &logical);
                        run();
                    })
                    .expect("spawn burn thread"),
            );
        }

        for index in 0..allocation.jobs + allocation.tasks {
            let topology = topology.clone();
            let float = allocation.float.clone();
            let run = run(Tier::Job);
            handles.push(
                std::thread::Builder::new()
                    .name(format!("bascet-job-{index}"))
                    .spawn(move || {
                        Self::pin(topology.as_deref(), &float);
                        run();
                    })
                    .expect("spawn job thread"),
            );
        }

        Self { handles }
    }

    pub(crate) fn join(&mut self) {
        for handle in self.handles.drain(..) {
            handle.join().ok();
        }
    }

    fn pin(topology: Option<&Topology>, logical: &[usize]) {
        let Some(topology) = topology else { return };
        if logical.is_empty() {
            return;
        }
        let mut set = CpuSet::new();
        for &id in logical {
            set.set(id);
        }
        if topology.bind_cpu(&set, CpuBindingFlags::THREAD).is_err() {
            Exception::HWFailureSetAffinity.log();
        }
    }
}
