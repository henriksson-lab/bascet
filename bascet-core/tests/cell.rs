use bascet_apply::*;
use bascet_core::{
    attr::{Id, Metadata, Read},
    Composite, Get,
};
use bascet_derive::*;

#[derive(Composite, Default)]
#[attrs(Id, Metadata)]
struct NormalCell {
    id: String,
    metadata: &'static str,
}

#[derive(Composite, Default)]
#[attrs(Id = custom_id, Read)]
struct RedirectCell {
    custom_id: Vec<u8>,
    read: Vec<Vec<u8>>,
}

#[derive(Composite)]
#[attrs(Id, Metadata)]
struct DefaultCell {
    id: Vec<u8>,
    metadata: &'static str,
}

impl Default for DefaultCell {
    fn default() -> Self {
        Self {
            id: b"default".to_vec(),
            metadata: Default::default(),
        }
    }
}

#[test]
fn apply_full() {
    let mut cell = NormalCell::default();
    apply_selected!((Id, Metadata), cell, {
        Id => "test".to_string(),
        Metadata => "meta",
    });
    assert_eq!(cell.id, "test");
    assert_eq!(cell.metadata, "meta");
}

#[test]
fn apply_partial() {
    let mut cell = NormalCell::default();
    apply_selected!(Id, cell, {
        Id => "only_id".to_string(),
        Metadata => "ignored",
    });
    assert_eq!(cell.id, "only_id");
    assert_eq!(cell.metadata, <&str>::default());
}

#[test]
fn get_trait() {
    let mut cell = NormalCell::default();
    apply_selected!(Id, cell, { Id => "test".to_string() });
    assert_eq!(cell.get_ref::<Id>(), "test");
}

#[test]
fn field_override() {
    let mut cell = RedirectCell::default();
    apply_selected!(Id, cell, {
        Id => b"test".to_vec(),
        Read => b"read test".to_vec()
    });
    assert_eq!(cell.custom_id, b"test");
    assert_eq!(Get::<Id>::attr(&cell), b"test");
}

#[test]
fn filters_unselected() {
    let mut cell = NormalCell::default();
    apply_selected!(Id, cell, {
        Id => "selected".to_string(),
        Read => "not selected - generates no code",
    });
    assert_eq!(cell.id, "selected");
}

#[test]
fn default_impl() {
    let mut cell = DefaultCell::default();
    apply_selected!(Metadata, cell, {
        Metadata => "Some metadata",
    });
    assert_eq!(cell.id, b"default".to_vec());
}
