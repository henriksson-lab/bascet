pub trait Backing {}

pub trait TakeBacking<B: Backing>
where
    Self: crate::Get<B>,
{
    fn take_backing(self) -> <Self as crate::Get<B>>::Value;
}

pub trait PushBacking<S, B: Backing>
where
    Self: crate::Get<B>,
    S: crate::Get<B>,
{
    fn push_backing(&mut self, backing: <S as crate::Get<B>>::Value);
}
