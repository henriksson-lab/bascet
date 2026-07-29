use crate::attr::Attr;
use crate::set::ops::chain::Chain;
use crate::set::ops::member::In;
use crate::set::ops::select::Select;

pub trait Keep<R> {
    type Output;
}

impl<A: Attr, R> Keep<R> for A
where
    A: In<R>,
    A: Select<<A as In<R>>::Result>,
{
    type Output = <A as Select<<A as In<R>>::Result>>::Output;
}

pub trait Intersect<R> {
    type Output;
}

impl<R> Intersect<R> for () {
    type Output = ();
}

impl<R, H: Attr, Rest> Intersect<R> for (H, Rest)
where
    H: Keep<R>,
    Rest: Intersect<R>,
    <H as Keep<R>>::Output: Chain<<Rest as Intersect<R>>::Output>,
{
    type Output = <<H as Keep<R>>::Output as Chain<<Rest as Intersect<R>>::Output>>::Output;
}
