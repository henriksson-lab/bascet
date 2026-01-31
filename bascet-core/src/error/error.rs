use tracing::Span;

pub type BascetResult<T> = Result<T, BascetError>;

#[derive(Debug, thiserror::Error)]
#[error("{inner}")]
pub struct BascetError {
    #[source]
    pub inner: anyhow::Error,
    pub span: Span,
}

#[bon::bon]
impl BascetError {
    #[builder]
    pub fn new(
        with_error: impl Into<anyhow::Error>,
        with_span: tracing::Span
    ) -> Self {
        BascetError {
            inner: with_error.into(),
            span: with_span,
        }
    }

    pub fn is_fatal(&self) -> bool {
        self.span
            .metadata()
            .map(|m| m.level() <= &tracing::Level::ERROR)
            .unwrap_or(true)
    }
}