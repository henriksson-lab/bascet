#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Preempt {
    Continue = 0,
    Yield = 1,
    Halt = 2,
}
/* 1)[1] I'm not 100% sure because im not super familiar with this
kind of rust. if this (including the split to map_batch also)
is the rustiest way of achieving this thats cool. otherwise
please think more on how to solve this problem in general [2]
sealing is a great idea! [3] I'm confused here. Why cant we use
the Store trait? why are we abstracting this so hard? [4] im not
so sure I love BoxError as a type? why do we need it like that?
cant we just use the underling type instead of hiding it behind
an opaque type? (2) why do we need a new thing for this? why
cant we integrate this with existing stuff? (3) [5] */
