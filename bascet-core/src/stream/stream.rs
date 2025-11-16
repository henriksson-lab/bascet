pub struct Stream<D, P> 
where 
    D: crate::Decode,
    P: crate::Parse<D::Block> {
    inner_decoder: D,
    inner_parser: P,
}

impl<D, P> Stream<D, P>
where 
    D: crate::Decode,
    P: crate::Parse<D::Block> {
    pub fn new(decoder: D, parser: P) -> Self {
        Self { inner_decoder: decoder, inner_parser: parser }
    }

    pub fn next<C>(&mut self) -> Result<Option<C>, ()>
    where
        C: crate::Composite,
    {
        loop {
            match self.inner_decoder.decode()? {
                Some(block) => {
                    if let Some(structured) = self.inner_parser.parse::<C, C::Attrs>(block)? {
                        return Ok(Some(structured));
                    }
                }
                None => return Ok(None),
            }
        }
    }

    pub fn next_with<C, A>(&mut self) -> Result<Option<C>, ()>
    where
        C: crate::Composite,
    {
        loop {
            match self.inner_decoder.decode()? {
                Some(block) => {
                    if let Some(structured) = self.inner_parser.parse::<C, A>(block)? {
                        return Ok(Some(structured));
                    }
                }
                None => return Ok(None),
            }
        }
    }
}
