use crate::cell::*;
use bascet_provide::*;

// nobuild flag creates a noop build with an unreachable-flagged getter
#[cell(Id, Read, Metadata(nobuild: &'static str))]
pub struct TestCell {
    // example of a build noop that does exist on the struct
    #[build_set(|builder, _| builder)]
    id: Vec<u8>,
    
    // example of default override
    #[build_default(|| vec!["ATGCATCC".into()])]
    read: Vec<Vec<u8>>,
}

fn main() {
    let mut cell = TestCell::builder()
        .with::<Id>(b"cell_test".to_vec())
        .with::<Metadata>("This will be ignored")
        .build();
}
