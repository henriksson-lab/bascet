use bascet_core::*;

define_parser!(
    TIRPMarker,
    AsRecord = crate::tirp::TIRPRecord,
    AsCell = crate::tirp::TIRPCell,
);

pub struct TIRP<M>
where
    M: TIRPMarker,
{
    pub(crate) inner_cursor: usize,
    pub(crate) inner_current: Option<M::Item>,
}

#[bon::bon]
impl<M> TIRP<M>
where
    M: TIRPMarker,
{
    #[builder]
    pub fn new() -> Result<Self, ()> {
        Ok(TIRP {
            inner_cursor: 0,
            inner_current: None,
        })
    }
}
