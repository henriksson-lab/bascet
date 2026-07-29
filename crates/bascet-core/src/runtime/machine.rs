use std::collections::HashMap;
use std::num::NonZeroUsize;

use hwlocality::Topology;
use hwlocality::object::types::ObjectType;

use crate::runtime::exception::Exception;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Kind(pub(crate) usize);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Cluster(pub(crate) usize);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Node(pub(crate) usize);

pub(crate) struct Core {
    pub(crate) logical: Vec<usize>,
    pub(crate) kind: Kind,
    pub(crate) cluster: Cluster,
    pub(crate) node: Node,
}

pub(crate) struct Machine {
    pub(crate) cores: Vec<Core>,
    pub(crate) binds: bool,
}

impl Machine {
    pub(crate) fn probe(topology: &Topology) -> Self {
        let binds = topology
            .feature_support()
            .cpu_binding()
            .is_some_and(|support| support.set_thread());

        let mut rank: HashMap<usize, usize> = HashMap::new();
        match topology.cpu_kinds() {
            Ok(kinds) => {
                let mut kinds: Vec<_> = kinds.collect();
                kinds.sort_by(|a, b| b.efficiency.cmp(&a.efficiency));
                for (index, kind) in kinds.iter().enumerate() {
                    for id in kind.cpuset.iter_set() {
                        rank.insert(usize::from(id), index);
                    }
                }
            }
            Err(_) => Exception::HWUnavailableKinds.log(),
        }

        let mut cores: Vec<Core> = topology
            .objects_with_type(ObjectType::Core)
            .filter_map(|core| {
                let logical: Vec<usize> = core.cpuset()?.iter_set().map(usize::from).collect();
                let &first = logical.first()?;
                let index = |ty| core.first_ancestor_with_type(ty).map(|o| o.logical_index());
                Some(Core {
                    kind: Kind(rank.get(&first).copied().unwrap_or(0)),
                    cluster: Cluster(index(ObjectType::L3Cache).unwrap_or(0)),
                    node: Node(index(ObjectType::NUMANode).unwrap_or(0)),
                    logical,
                })
            })
            .collect();

        if cores.is_empty() {
            Exception::HWUnavailableCores.log();
            cores = topology
                .cpuset()
                .iter_set()
                .map(|id| Core {
                    logical: vec![usize::from(id)],
                    kind: Kind(0),
                    cluster: Cluster(0),
                    node: Node(0),
                })
                .collect();
        }

        cores.sort_by_key(|core| (core.kind, core.cluster, core.node, core.logical[0]));
        Machine { cores, binds }
    }

    pub(crate) fn fallback() -> Self {
        let count = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        let cores = (0..count)
            .map(|id| Core {
                logical: vec![id],
                kind: Kind(0),
                cluster: Cluster(0),
                node: Node(0),
            })
            .collect();
        Machine {
            cores,
            binds: false,
        }
    }
}
