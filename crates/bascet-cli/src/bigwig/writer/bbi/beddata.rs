//! Input adapters for writing BigWig data.

use std::io;

use thiserror::Error;
use tokio::runtime::Runtime;

use super::{BBIDataProcessor, BBIDataSource, BBIProcessError, ProcessDataError};

pub trait StreamingBedValues {
    type Value;

    fn next(&mut self) -> Option<Result<(&str, Self::Value), BedValueError>>;
}

#[derive(Error, Debug)]
pub enum BedValueError {
    #[error("{}", .0)]
    InvalidInput(String),
    #[error("Error occurred: {}", .0)]
    IoError(#[from] io::Error),
}

pub struct BedInfallibleIteratorStream<V, I> {
    pub(crate) iter: I,
    pub(crate) curr: Option<(String, V)>,
}

impl<V: Clone, C: Into<String> + for<'a> PartialEq<&'a str>, I: Iterator<Item = (C, V)>>
    StreamingBedValues for BedInfallibleIteratorStream<V, I>
{
    type Value = V;

    fn next(&mut self) -> Option<Result<(&str, V), BedValueError>> {
        use std::ops::Deref;
        self.curr = match (self.curr.take(), self.iter.next()?) {
            (Some(c), v) => {
                if v.0 == &c.0 {
                    Some((c.0, v.1))
                } else {
                    Some((v.0.into(), v.1))
                }
            }
            (None, v) => Some((v.0.into(), v.1)),
        };
        self.curr.as_ref().map(|v| Ok((v.0.deref(), v.1.clone())))
    }
}

pub struct BedParserStreamingIterator<S: StreamingBedValues> {
    bed_data: S,
    allow_out_of_order_chroms: bool,
}

impl<S: StreamingBedValues> BedParserStreamingIterator<S> {
    pub fn new(bed_data: S, allow_out_of_order_chroms: bool) -> Self {
        BedParserStreamingIterator {
            bed_data,
            allow_out_of_order_chroms,
        }
    }
}

impl<V: Clone, C: Into<String> + for<'a> PartialEq<&'a str>, I: Iterator<Item = (C, V)>>
    BedParserStreamingIterator<BedInfallibleIteratorStream<V, I>>
{
    pub fn wrap_infallible_iter(iter: I, allow_out_of_order_chroms: bool) -> Self {
        BedParserStreamingIterator::new(
            BedInfallibleIteratorStream { iter, curr: None },
            allow_out_of_order_chroms,
        )
    }
}

impl<S: StreamingBedValues> BBIDataSource for BedParserStreamingIterator<S> {
    type Value = S::Value;
    type Error = BedValueError;

    fn process_to_bbi<
        P: BBIDataProcessor<Value = Self::Value>,
        StartProcessing: FnMut(String) -> Result<P, ProcessDataError>,
        Advance: FnMut(P),
    >(
        &mut self,
        runtime: &Runtime,
        start_processing: &mut StartProcessing,
        advance: &mut Advance,
    ) -> Result<(), BBIProcessError<Self::Error>> {
        runtime.block_on(async move {
            let first_val = self.bed_data.next();
            let (mut curr_state, mut next_val) = match first_val {
                Some(Err(e)) => return Err(BBIProcessError::SourceError(e)),
                None => {
                    return Err(BBIProcessError::SourceError(BedValueError::InvalidInput(
                        "Input bedGraph is empty.".to_string(),
                    )));
                }
                Some(Ok((chrom, val))) => {
                    let chrom = chrom.to_string();
                    let mut p = start_processing(chrom.clone())?;
                    let next_val = match self.bed_data.next() {
                        Some(Err(e)) => return Err(BBIProcessError::SourceError(e)),
                        Some(Ok(v)) => Some(v),
                        None => None,
                    };
                    let next_value = match &next_val {
                        Some(v) if v.0 == chrom => Some(&v.1),
                        _ => None,
                    };
                    p.do_process(val, next_value).await?;
                    ((chrom, p), next_val)
                }
            };

            loop {
                next_val = match (&mut curr_state, next_val) {
                    ((_, _), None) => {
                        advance(curr_state.1);
                        return Ok(());
                    }
                    ((curr_chrom, curr_state), Some((chrom, val))) if chrom == curr_chrom => {
                        let next_val = match self.bed_data.next() {
                            Some(Err(e)) => return Err(BBIProcessError::SourceError(e)),
                            Some(Ok(v)) => Some(v),
                            None => None,
                        };
                        let next_value = match &next_val {
                            Some(v) if v.0 == curr_chrom => Some(&v.1),
                            _ => None,
                        };
                        curr_state.do_process(val, next_value).await?;
                        next_val
                    }
                    (_, Some((chrom, val))) => {
                        let (prev_chrom, prev_state) = curr_state;
                        if !self.allow_out_of_order_chroms && prev_chrom.as_str() >= chrom {
                            return Err(BBIProcessError::SourceError(BedValueError::InvalidInput(
                                "Input bedGraph not sorted by chromosome. Sort with `sort -k1,1 -k2,2n`."
                                    .to_string(),
                            )));
                        }
                        advance(prev_state);

                        let chrom = chrom.to_string();
                        let mut p = start_processing(chrom.clone())?;
                        let next_val = match self.bed_data.next() {
                            Some(Err(e)) => return Err(BBIProcessError::SourceError(e)),
                            Some(Ok(v)) => Some(v),
                            None => None,
                        };
                        let next_value = match &next_val {
                            Some(v) if v.0 == chrom => Some(&v.1),
                            _ => None,
                        };

                        p.do_process(val, next_value).await?;
                        curr_state = (chrom, p);
                        next_val
                    }
                };
            }
        })
    }
}
