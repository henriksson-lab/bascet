use std::collections::HashSet;

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
    pub(crate) jobs: usize,
    pub(crate) tasks: usize,
}

impl Allocation {
    const RESERVE: [(usize, usize); 9] = [
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
        total: Option<usize>,
        burn: Option<usize>,
        jobs: Option<usize>,
        tasks: Option<usize>,
        pinning: Pinning,
    ) -> Self {
        let available = total
            .unwrap_or(machine.cores.len())
            .min(machine.cores.len());

        let cores = &machine.cores[..available];
        let logical: usize = cores.iter().map(|core| core.logical.len()).sum();

        let (reserved_jobs, reserved_tasks) = Self::reserve(available);
        let tasks = tasks.unwrap_or(reserved_tasks);
        let (burn, jobs) = match (burn, jobs) {
            (None, given) => {
                let jobs = given.unwrap_or(reserved_jobs);
                (available.saturating_sub(jobs + tasks), jobs)
            }
            (Some(burn), None) => (burn, available.saturating_sub(burn + tasks)),
            (Some(burn), Some(jobs)) => (burn, jobs),
        };

        if burn + jobs + tasks == 0 {
            Exception::NoWorkers.log();
        }
        if matches!(pinning, Pinning::Physical) && burn > available && burn <= logical {
            Exception::BurnExceedsCores {
                burn,
                cores: available,
            }
            .log();
        }
        if burn > logical {
            Exception::BurnExceedsLogical { burn, logical }.log();
        }

        Self::place(cores, burn, jobs, tasks, pinning)
    }

    fn reserve(cores: usize) -> (usize, usize) {
        if cores < Self::RESERVE.len() {
            return Self::RESERVE[cores];
        }
        let root = cores.isqrt();
        let jobs = if root * root == cores { root } else { root + 1 };
        (jobs, jobs.div_ceil(2))
    }

    fn place(cores: &[Core], burn: usize, jobs: usize, tasks: usize, pinning: Pinning) -> Self {
        let pinning = match pinning {
            Pinning::Physical if burn <= cores.len() => Pinning::Physical,
            _ => Pinning::Virtual,
        };
        let burn = match pinning {
            Pinning::Physical => Self::snap(cores, burn),
            Pinning::Virtual => burn,
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
        for cores in 1..=4096 {
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
            Allocation::plan(&machine(16, 1), None, None, None, None, Pinning::Physical);
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
            Some(8),
            None,
            None,
            None,
            Pinning::Physical,
        );
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
            Some(6),
            None,
            None,
            Pinning::Physical,
        );
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
            Allocation::plan(&machine, None, Some(4), Some(3), Some(1), Pinning::Physical);
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
            Some(10),
            Some(4),
            Some(2),
            Pinning::Physical,
        );
        assert_eq!(allocation.burn.len(), 8);
        let held: HashSet<usize> = allocation.burn.iter().flatten().copied().collect();
        assert!(held.iter().all(|&id| id < 8));
    }

    #[test]
    fn virtual_pinning_gives_burn_single_cores() {
        let physical = Allocation::plan(
            &machine(8, 2),
            None,
            Some(4),
            Some(2),
            Some(0),
            Pinning::Physical,
        );
        let virtual_burn = Allocation::plan(
            &machine(8, 2),
            None,
            Some(4),
            Some(2),
            Some(0),
            Pinning::Virtual,
        );
        assert!(physical.burn.iter().all(|logical| logical.len() == 2));
        assert!(virtual_burn.burn.iter().all(|logical| logical.len() == 1));
    }

    #[test]
    fn burn_overflow_spills_onto_siblings() {
        let allocation = Allocation::plan(
            &machine(12, 2),
            None,
            Some(18),
            Some(0),
            Some(0),
            Pinning::Physical,
        );
        assert_eq!(allocation.burn.len(), 18);
        assert!(allocation.burn.iter().all(|logical| logical.len() == 1));
    }
}
