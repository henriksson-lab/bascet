use bascet_core::*;
use bascet_derive::*;

#[cell(Id, Read)]
pub struct BasicCell {
    id: Vec<u8>,
    read: Vec<Vec<u8>>,
}

#[cell(Id = custom_id, Read)]
pub struct RedirectCell {
    custom_id: Vec<u8>,
    read: Vec<Vec<u8>>,
}

#[cell(Id, Read, Metadata(nobuild: &'static str))]
pub struct NoBuildCell {
    id: Vec<u8>,
    #[default(|| vec![b"default".to_vec()])]
    read: Vec<Vec<u8>>,
}

#[cell(Id, Read)]
pub struct CustomSetterCell {
    #[with(|mut builder: CustomSetterCellBuilder, value: Vec<u8>| {
        builder.id = value.into_iter().map(|v| v * 2).collect();
        builder
    })]
    id: Vec<u8>,
    read: Vec<Vec<u8>>,
}

#[test]
fn test_single_attr() {
    let cell = BasicCell::builder()
        .with::<Id>(b"test_id".to_vec())
        .with::<Read>(vec![b"ATGC".to_vec()])
        .build();

    let id: &Vec<u8> = cell.get_ref::<Id>();
    assert_eq!(id, b"test_id");
}

#[test]
fn test_tuple_get() {
    let cell = BasicCell::builder()
        .with::<Id>(b"test".to_vec())
        .with::<Read>(vec![b"ATGC".to_vec()])
        .build();

    let (id, read) = cell.get_ref::<(Id, Read)>();
    assert_eq!(id, b"test");
    assert_eq!(read, &vec![b"ATGC".to_vec()]);
}

#[test]
fn test_mut() {
    let mut cell = BasicCell::builder()
        .with::<Id>(b"test".to_vec())
        .with::<Read>(vec![b"ATGC".to_vec()])
        .build();

    let id: &mut Vec<u8> = cell.get_mut::<Id>();
    id.extend_from_slice(b"_modified");

    assert_eq!(cell.get_ref::<Id>(), b"test_modified");
}

#[test]
fn test_redirect() {
    let cell = RedirectCell::builder()
        .with::<Id>(b"override".to_vec())
        .with::<Read>(vec![b"ATGC".to_vec()])
        .build();

    assert_eq!(cell.get_ref::<Id>(), b"override");
}

#[test]
fn test_nobuild() {
    let cell = NoBuildCell::builder()
        .with::<Id>(b"test".to_vec())
        .with::<Metadata>("ignored")
        .build();

    assert_eq!(cell.get_ref::<Id>(), b"test");
}

#[test]
fn test_default() {
    let cell = NoBuildCell::builder().with::<Id>(b"test".to_vec()).build();

    assert_eq!(cell.get_ref::<Read>(), &vec![b"default".to_vec()]);
}

#[test]
fn test_custom_setter() {
    let cell = CustomSetterCell::builder()
        .with::<Id>(vec![1, 2, 3])
        .with::<Read>(vec![b"ATGC".to_vec()])
        .build();

    assert_eq!(cell.get_ref::<Id>(), &vec![2, 4, 6]);
    assert_eq!(cell.get_ref::<Read>(), &vec![b"ATGC".to_vec()]);
}
