pub(crate) mod synchronous;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum State {
    New = 0,
    Running = 1,
    Released = 2,
    Finished = 3,
    Panicked = 4,
    Failed = 5,
    Starved = 6,
    Blocked = 7,
    Yielded = 8,
    Halted = 9,
}
