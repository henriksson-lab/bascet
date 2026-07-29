pub trait Chain<Tail> {
    type Output;
    fn chain(self, tail: Tail) -> Self::Output;
}

impl<Tail> Chain<Tail> for () {
    type Output = Tail;
    fn chain(self, tail: Tail) -> Tail {
        tail
    }
}

impl<S, Rest, Tail> Chain<Tail> for (S, Rest)
where
    Rest: Chain<Tail>,
{
    type Output = (S, <Rest as Chain<Tail>>::Output);
    fn chain(self, tail: Tail) -> Self::Output {
        (self.0, self.1.chain(tail))
    }
}
