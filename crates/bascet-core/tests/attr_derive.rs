use std::collections::HashMap;

use bascet_core::attr::Phred;
use bascet_core::set::AttrId;
use bascet_core::{Attr, AttrEntry};

#[test]
fn derived_attr_has_distinct_type_ids() {
    assert_ne!(
        <<Phred<1> as Attr>::Id as AttrId>::ID,
        <<Phred<2> as Attr>::Id as AttrId>::ID,
    );
}

#[test]
fn no_two_attrs_share_an_id() {
    let mut seen: HashMap<u64, &str> = HashMap::new();
    for entry in inventory::iter::<AttrEntry>() {
        if let Some(prev) = seen.insert(entry.id, entry.name) {
            assert_eq!(prev, entry.name, "attr id collision at {:#x}", entry.id);
        }
    }
}
