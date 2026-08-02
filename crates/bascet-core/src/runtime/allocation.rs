use std::collections::HashSet;
use std::num::NonZero;

use crate::exception::Raise;
use crate::runtime::exception::Exception;
use crate::runtime::machine::{Core, Machine};

#[derive(Clone, Copy, Debug, Default)]
pub enum Pinning {
    #[default]
    Physical,
    Virtual,
}

pub(crate) struct Allocation {
    pub(crate) burn: Vec<Vec<usize>>,
    pub(crate) float: Vec<usize>,
    pub(crate) jobs: u64,
    pub(crate) tasks: u64,
}

impl Allocation {
    const RESERVE: [(u64, u64); 9] = [
        (0, 0),
        (1, 0),
        (1, 1),
        (2, 1),
        (3, 1),
        (3, 1),
        (3, 1),
        (3, 1),
        (3, 1),
    ];

    pub(crate) fn plan(
        machine: &Machine,
        total: Option<NonZero<u64>>,
        jobs: Option<u64>,
        tasks: Option<u64>,
        burn: Option<u64>,
        pinning: Pinning,
    ) -> Result<Self, Exception> {
        let physical = machine.cores.len() as u64;
        let logical: u64 = machine
            .cores
            .iter()
            .map(|core| core.logical.len() as u64)
            .sum();
        let threads = (logical / physical.max(1)).max(1);
        let pinned = machine.binds && matches!(pinning, Pinning::Physical);

        let budget = total.map(NonZero::get).unwrap_or(logical).min(logical);
        let (reserved_jobs, reserved_tasks) = Self::reserve(budget);
        let tasks = tasks.unwrap_or(reserved_tasks);

        let (mut burn, jobs) = match (burn, jobs) {
            (None, given) => {
                let jobs = given.unwrap_or(reserved_jobs);
                let cost = if pinned { threads } else { 1 };
                (budget.saturating_sub(jobs + tasks) / cost, jobs)
            }
            (Some(burn), None) => {
                let cost = if pinned && burn <= physical {
                    threads
                } else {
                    1
                };
                (burn, budget.saturating_sub(burn * cost + tasks))
            }
            (Some(burn), Some(jobs)) => (burn, jobs),
        };

        let cost = if pinned && burn <= physical {
            threads
        } else {
            1
        };

        if pinned && burn > physical {
            let _span = tracing::warn_span!("cpu pinning", requested = burn, physical).entered();
            Exception::HWFailureInsufficientCoresPhysical
                .annotate()
                .fixed("placing burn workers on logical cpus instead of whole cores")
                .suggestion(format!("lower burn to at most {physical} to keep dedicated pinning"))
                .raise()?;
        }
        if burn * cost + jobs + tasks > budget {
            let _span = tracing::warn_span!("cpu pinning", requested = burn, logical).entered();
            burn = budget.saturating_sub(jobs + tasks) / cost;
            Exception::HWFailureInsufficientCoresLogical
                .annotate()
                .fixed(format!("truncated burn to {burn}"))
                .suggestion(format!("lower jobs and tasks to fit {budget} logical cpus"))
                .raise()?;
        }
        if burn.max(jobs) + tasks < 2 {
            Exception::HWFailureInsufficientParallelism
                .annotate()
                .suggestion(format!(
                    "burn {burn}, jobs {jobs}, tasks {tasks} yields fewer than two workers; raise one of them"
                ))
                .raise()?;
        }

        let cores = &machine.cores[..budget.min(physical) as usize];
        Ok(Self::place(cores, burn, jobs, tasks, pinning))
    }

    fn reserve(cores: u64) -> (u64, u64) {
        if cores < Self::RESERVE.len() as u64 {
            return Self::RESERVE[cores as usize];
        }
        let root = cores.isqrt();
        let jobs = if root * root == cores { root } else { root + 1 };
        (jobs, jobs.div_ceil(2))
    }

    fn place(cores: &[Core], burn: u64, jobs: u64, tasks: u64, pinning: Pinning) -> Self {
        let pinning = match pinning {
            Pinning::Physical if burn <= cores.len() as u64 => Pinning::Physical,
            _ => Pinning::Virtual,
        };
        let burn = match pinning {
            Pinning::Physical => Self::snap(cores, burn as usize),
            Pinning::Virtual => burn as usize,
        };
        let burn: Vec<Vec<usize>> = cores
            .iter()
            .flat_map(|core| match pinning {
                Pinning::Physical => vec![core.logical.clone()],
                Pinning::Virtual => core.logical.iter().map(|&id| vec![id]).collect(),
            })
            .take(burn)
            .collect();
        let held: HashSet<usize> = burn.iter().flatten().copied().collect();
        let float = cores
            .iter()
            .flat_map(|core| core.logical.iter().copied())
            .filter(|id| !held.contains(id))
            .collect();
        Self {
            burn,
            float,
            jobs,
            tasks,
        }
    }

    fn snap(cores: &[Core], burn: usize) -> usize {
        if burn == 0 || burn >= cores.len() {
            return burn;
        }
        if cores[burn].cluster != cores[burn - 1].cluster {
            return burn;
        }
        let cut = cores[burn].cluster;
        match cores[..burn].iter().rposition(|core| core.cluster != cut) {
            Some(index) => index + 1,
            None => burn,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::machine::{Cluster, Kind, Node};

    fn machine(cores: usize, smt: usize) -> Machine {
        let cores = (0..cores)
            .map(|core| Core {
                logical: (0..smt).map(|lane| core * smt + lane).collect(),
                kind: Kind(0),
                cluster: Cluster(0),
                node: Node(0),
            })
            .collect();
        Machine { cores, binds: true }
    }

    #[test]
    fn reserve_leaves_room_for_burn() {
        for cores in 1u64..=4096 {
            let (jobs, tasks) = Allocation::reserve(cores);
            assert!(tasks <= jobs, "cores {cores}: tasks {tasks} > jobs {jobs}");
            assert!(
                jobs + tasks <= cores,
                "cores {cores}: reserve exceeds cores"
            );
        }
    }

    #[test]
    fn auto_split_conserves_cores() {
        let allocation =
            Allocation::plan(&machine(16, 1), None, None, None, None, Pinning::Physical).unwrap();
        assert_eq!(
            (allocation.burn.len(), allocation.jobs, allocation.tasks),
            (10, 4, 2)
        );
        assert_eq!(allocation.float.len(), 6);
    }

    #[test]
    fn total_caps_the_core_budget() {
        let allocation = Allocation::plan(
            &machine(16, 1),
            NonZero::new(8),
            None,
            None,
            None,
            Pinning::Physical,
        )
        .unwrap();
        assert_eq!(
            (allocation.burn.len(), allocation.jobs, allocation.tasks),
            (4, 3, 1)
        );
        assert_eq!(allocation.float.len(), 4);
    }

    #[test]
    fn explicit_burn_fills_the_rest_from_cores() {
        let allocation = Allocation::plan(
            &machine(16, 1),
            None,
            None,
            None,
            Some(6),
            Pinning::Physical,
        )
        .unwrap();
        assert_eq!(
            (allocation.burn.len(), allocation.jobs, allocation.tasks),
            (6, 8, 2)
        );
    }

    #[test]
    fn burn_takes_the_performant_kind_first() {
        let mut machine = machine(8, 1);
        machine
            .cores
            .iter_mut()
            .skip(4)
            .for_each(|core| core.kind = Kind(1));
        machine
            .cores
            .sort_by_key(|core| (core.kind, core.cluster, core.node, core.logical[0]));
        let allocation =
            Allocation::plan(&machine, None, Some(3), Some(1), Some(4), Pinning::Physical).unwrap();
        let held: HashSet<usize> = allocation.burn.iter().flatten().copied().collect();
        assert!(held.iter().all(|&id| id < 4));
    }

    #[test]
    fn burn_snaps_to_whole_clusters() {
        let mut machine = machine(16, 1);
        machine
            .cores
            .iter_mut()
            .skip(8)
            .for_each(|core| core.cluster = Cluster(1));
        machine
            .cores
            .sort_by_key(|core| (core.kind, core.cluster, core.node, core.logical[0]));
        let allocation = Allocation::plan(
            &machine,
            None,
            Some(4),
            Some(2),
            Some(10),
            Pinning::Physical,
        )
        .unwrap();
        assert_eq!(allocation.burn.len(), 8);
        let held: HashSet<usize> = allocation.burn.iter().flatten().copied().collect();
        assert!(held.iter().all(|&id| id < 8));
    }

    #[test]
    fn virtual_pinning_gives_burn_single_cores() {
        let physical = Allocation::plan(
            &machine(8, 2),
            None,
            Some(2),
            Some(0),
            Some(4),
            Pinning::Physical,
        )
        .unwrap();
        let virtual_burn = Allocation::plan(
            &machine(8, 2),
            None,
            Some(2),
            Some(0),
            Some(4),
            Pinning::Virtual,
        )
        .unwrap();
        assert!(physical.burn.iter().all(|logical| logical.len() == 2));
        assert!(virtual_burn.burn.iter().all(|logical| logical.len() == 1));
    }

    #[test]
    fn burn_overflow_spills_onto_siblings() {
        let allocation = Allocation::plan(
            &machine(12, 2),
            None,
            Some(0),
            Some(0),
            Some(18),
            Pinning::Physical,
        )
        .unwrap();
        assert_eq!(allocation.burn.len(), 18);
        assert!(allocation.burn.iter().all(|logical| logical.len() == 1));
    }
}
