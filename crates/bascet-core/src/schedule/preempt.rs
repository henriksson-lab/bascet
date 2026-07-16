#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Preempt {
    Continue = 0,
    Yield = 1,
    Halt = 2,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Action {
    Promote,
    Demote,
    Acquire,
    Released,
    Yield,
}

pub type Receipt = kanal::Sender<()>;
