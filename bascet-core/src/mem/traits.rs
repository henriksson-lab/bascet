pub unsafe trait ManuallyManaged {
    fn inc_ref(&mut self);
    fn dec_ref(&mut self);
}
