use std::ops::Range;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Pull {
    Next,
    Read(Range<u64>),
}
