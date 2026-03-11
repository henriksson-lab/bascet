pub trait Threads<T> {
    type Value;

    fn threads(&self) -> &Self::Value;

    fn spawn<F, R>(&self, offset: u64, f: F) -> std::thread::JoinHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static;
}

pub trait Memory<T> {
    type Value;
    fn mem(&self) -> &Self::Value;
}
