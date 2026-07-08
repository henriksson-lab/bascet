use std::num::NonZeroU32;

use crate::apply::Apply;
use crate::coordinate::{Coordinate, Demotion, Promotion};
use crate::layer::{Feedback, Layer};
use crate::pipeline::scheduler::{Motivation, Signal};
use crate::pipeline::scheduler::Id;
use crate::schedule::Strategy;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Auto;

impl<A: Apply> Coordinate<A> for Auto {
    fn on_promote(layer: &Layer<A>, signal: &Signal) -> Promotion {
        let current = signal.pressure().level();
        let birth = signal.level().get();

        let raw_desired = if layer.manual_parallelism() {
            layer
                .desired_for(signal)
                .min(layer.parallelism.value().max().get() as usize)
        } else {
            layer.desired_for(signal)
        };
        let useful_width = layer.useful_width();
        let desired = raw_desired.min(useful_width);
        let active_for = layer.active_for(signal, Strategy::Task);
        let width_deficit = desired.saturating_sub(active_for);

        if current == 0 {
            layer.learn(signal, Feedback::Eager);
            return Promotion::Idle;
        }

        let feedback = if current < birth {
            Feedback::Eager
        } else if active_for > 0 && width_deficit > 0 {
            Feedback::Late
        } else {
            Feedback::Stable
        };
        if feedback != Feedback::Stable {
            layer.learn(signal, feedback);
        }

        if layer.manual_parallelism() {
            return Promotion::Idle;
        }
        if NonZeroU32::new(current).is_none() {
            return Promotion::Idle;
        }

        if layer.strategy.is_manual() {
            let pinned = *layer.strategy.value();
            if layer.headroom() > 0 && layer.active_for(signal, pinned) < desired {
                return Promotion::Scale { strategy: pinned };
            }
            return Promotion::Idle;
        }

        if let Some(id) = layer.candidate_for_signal(Strategy::Job, signal) {
            return Promotion::Upgrade {
                id,
                to: Strategy::Burn,
            };
        }
        if let Some(id) = layer.candidate_for_signal(Strategy::Task, signal) {
            return Promotion::Upgrade {
                id,
                to: Strategy::Job,
            };
        }

        let coverage = match signal.motivation() {
            Motivation::Demand => layer.active_for(signal, Strategy::Task),
            Motivation::Pressure => layer.active_for(signal, Strategy::Job),
        };
        if layer.headroom() > 0 && coverage < desired {
            return Promotion::Scale {
                strategy: Strategy::Burn,
            };
        }

        Promotion::Idle
    }

    fn on_demote(layer: &Layer<A>, id: Id) -> Demotion {
        if layer.surplus() == 0 {
            return Demotion::Idle;
        }
        let signal = layer.get(id).and_then(|handle| handle.signal.as_ref()).cloned();
        if signal.as_ref().is_some_and(|signal| !layer.recovered(signal)) {
            return Demotion::Idle;
        }
        Demotion::Release { id }
    }
}
