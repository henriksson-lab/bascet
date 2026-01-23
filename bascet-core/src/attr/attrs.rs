pub mod sequence {
    bascet_derive::define_attr!(R0, R1, R2);
}

pub mod quality {
    bascet_derive::define_attr!(Q0, Q1, Q2);
}

pub mod meta {
    bascet_derive::define_attr!(Id, Umi, Depth, Countsketch);
}

pub mod block {
    bascet_derive::define_attr!(Offset, Header, Compressed, Trailer);
}