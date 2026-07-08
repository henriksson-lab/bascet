mod atomic;
mod patience;

pub use atomic::AtomicPatience;
pub use patience::Patience;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Temper<T> {
    Eager(T),
    Patient(T),
}
