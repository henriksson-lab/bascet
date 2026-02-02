
pub enum ParseResult<T> {
    Full(T),
    Partial,
    Error(anyhow::Error),
    Finished,
}