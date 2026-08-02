#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Preempt {
    Continue = 0,
    Yield = 1,
    Halt = 2,
}
