use crate::runtime::Tier;

pub type Job = Box<dyn FnOnce() + Send>;

pub struct Pool {
    burn: Box<[kanal::Sender<Job>]>,
    job: Box<[kanal::Sender<Job>]>,
    handles: Vec<std::thread::JoinHandle<()>>,
}

impl Pool {
    pub fn spawn(burn: usize, jobs: usize, _tasks: usize) -> Self {
        let cores = core_affinity::get_core_ids().unwrap_or_default();
        let mut handles = Vec::new();
        let burn = (0..burn)
            .map(|i| {
                let (work_tx, work_rx) = kanal::unbounded::<Job>();
                let core = cores.get(i).copied();
                handles.push(
                    std::thread::Builder::new()
                        .name(format!("bascet-burn-{i}"))
                        .spawn(move || {
                            if let Some(core) = core {
                                core_affinity::set_for_current(core);
                            }
                            while let Ok(job) = work_rx.recv() {
                                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
                            }
                        })
                        .expect("spawn burn thread"),
                );
                work_tx
            })
            .collect();
        let job = (0..jobs)
            .map(|i| {
                let (work_tx, work_rx) = kanal::unbounded::<Job>();
                handles.push(
                    std::thread::Builder::new()
                        .name(format!("bascet-job-{i}"))
                        .spawn(move || {
                            while let Ok(job) = work_rx.recv() {
                                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
                            }
                        })
                        .expect("spawn job thread"),
                );
                work_tx
            })
            .collect();
        Self { burn, job, handles }
    }

    pub fn broadcast(&self, mut job: impl FnMut(Tier) -> Job) {
        for burn_tx in self.burn.iter() {
            burn_tx.send(job(Tier::Burn)).ok();
        }
        for job_tx in self.job.iter() {
            job_tx.send(job(Tier::Job)).ok();
        }
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.burn = Box::new([]);
        self.job = Box::new([]);
        for handle in self.handles.drain(..) {
            handle.join().ok();
        }
    }
}
