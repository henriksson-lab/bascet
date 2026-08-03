use crate::attr::Attr;
use crate::set::chain::Chain;

pub trait Lower {
    type Out;
}

impl Lower for () {
    type Out = ();
}

impl<A: Attr> Lower for A {
    type Out = (A, ());
}

bascet_variadic::variadic!(N = 1..=16, for N in N => {
    impl<@N[A~#: Attr](sep=",")> Lower for (@N[A~#](sep=","),) {
        type Out = @N[<(A~#, ()) as Chain<] () @N[>>::Output];
    }
});
