#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Cooperate {
    Continue = 0,
    Yield = 1,
    Halt = 2,
    Shutdown = 3,
}

impl From<u8> for Cooperate {
    fn from(bits: u8) -> Self {
        match bits {
            0 => Cooperate::Continue,
            1 => Cooperate::Yield,
            2 => Cooperate::Halt,
            _ => Cooperate::Shutdown,
        }
    }
}
