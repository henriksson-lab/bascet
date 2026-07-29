//! Value-plane engine spike (throwaway). Does the by-key column lookup plus a
//! `Cols`-generic `impl Apply` compile and read well? Run: `cargo run -p bascet-sandbox`.
//! Expected output: "AC" then "TTTT".
//!
//! Shape under test:
//!  - `Holds<A>` / `Step<A, V>`: the by-key column lookup, dispatched on a `Matches` verdict.
//!    Both are public, so `Holds` (the bound layers reference) never leaks a private trait.
//!  - `Get<A>` + `Row::get`: projecting the matched column's GAT `Item` at row `i`.
//!  - `impl Apply for QualityTrim<Cols>`: the layer is generic in its input columns, so the
//!    per-attr representation bound lives on the impl block, and the body calls a real concrete
//!    method `trim(&[u8], &[u8])`.
//!  - Columns carry their own key (`Column::Key`) and own their data — there is no `Bind`
//!    wrapper holding a column, so the concrete column type never leaks through a tag.
//!  - `Matches` here relates the attr key types directly. bascet-core relates the FNV nibble
//!    ids (`A::Id: Matches<B::Id>`); adding `::Id` is mechanical and changes none of the below.
//!  - Out of scope (their own spikes): the fusion/passthrough move, and the `#[layer]` macro.
//!
//! Most likely to fail (the point of the spike): the higher-ranked GAT-equality bound
//! `for<'x> C: Column<Item<'x> = &'x [u8]>`, and whether it normalises `row.get::<Sequence>()`
//! to `&[u8]` at the `trim` call. Mis-wire checks are the commented functions at the bottom.

use std::marker::PhantomData;

// ---- verdicts ----
pub struct Hit;
pub struct Miss;

// ---- attrs (valueless keys) + the match relation (diagonal Hit, off-diagonal Miss) ----
pub trait Attr: 'static {}
pub struct Id;
pub struct Sequence;
pub struct Quality;
pub struct Trimmed;
pub struct Barcode;
impl Attr for Id {}
impl Attr for Sequence {}
impl Attr for Quality {}
impl Attr for Trimmed {}
impl Attr for Barcode {}

pub trait Matches<B> {
    type Verdict;
}
macro_rules! diagonal {
    ($($x:ty),*) => { $( impl Matches<$x> for $x { type Verdict = Hit; } )* };
}
macro_rules! off_diagonal {
    () => {};
    ($h:ty $(, $t:ty)*) => {
        $(
            impl Matches<$t> for $h { type Verdict = Miss; }
            impl Matches<$h> for $t { type Verdict = Miss; }
        )*
        off_diagonal!($($t),*);
    };
}
diagonal!(Id, Sequence, Quality, Trimmed, Barcode);
off_diagonal!(Id, Sequence, Quality, Trimmed, Barcode);

// ---- columns: each owns its data and knows its key; its item is a GAT ----
pub trait Column: 'static {
    type Key: Attr;
    type Item<'a>;
    fn get(&self, row: usize) -> Self::Item<'_>;
    fn len(&self) -> usize;
}

pub struct Names(pub Vec<Vec<u8>>);
impl Column for Names {
    type Key = Id;
    type Item<'a> = &'a [u8];
    fn get(&self, row: usize) -> &[u8] {
        &self.0[row]
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}

pub struct Reads(pub Vec<Vec<u8>>);
impl Column for Reads {
    type Key = Sequence;
    type Item<'a> = &'a [u8];
    fn get(&self, row: usize) -> &[u8] {
        &self.0[row]
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}

pub struct Quals(pub Vec<Vec<u8>>);
impl Column for Quals {
    type Key = Quality;
    type Item<'a> = &'a [u8];
    fn get(&self, row: usize) -> &[u8] {
        &self.0[row]
    }
    fn len(&self) -> usize {
        self.0.len()
    }
}

// a column whose item is *constructed*, not `&V` — this is why `Column`/`Get` are GAT'd.
pub struct Kmer<'a>(pub &'a [u8]);
pub struct Packed {
    pub bytes: Vec<u8>,
    pub spans: Vec<(usize, usize)>,
}
impl Column for Packed {
    type Key = Sequence;
    type Item<'a> = Kmer<'a>;
    fn get(&self, row: usize) -> Kmer<'_> {
        let (start, len) = self.spans[row];
        Kmer(&self.bytes[start..start + len])
    }
    fn len(&self) -> usize {
        self.spans.len()
    }
}

// ---- a named representation predicate: routes the rep bound through a labelable trait ----
#[diagnostic::on_unimplemented(
    message = "column `{Self}` is not readable as `&[u8]`",
    label = "this layer reads its attribute as `&[u8]`, but `{Self}` yields a different representation"
)]
pub trait Bytes: for<'x> Column<Item<'x> = &'x [u8]> {}
impl<C> Bytes for C where C: for<'x> Column<Item<'x> = &'x [u8]> {}

// ---- the lookup: find the column keyed to A inside a column tuple ----
#[diagnostic::on_unimplemented(
    message = "no column provides attribute `{A}`",
    label = "this batch has no `{A}` column",
    note = "a layer can only read attributes its upstream provides"
)]
pub trait Holds<A> {
    type Col: Column;
    fn col(&self) -> &Self::Col;
}
pub trait Step<A, V> {
    type Col: Column;
    fn col(&self) -> &Self::Col;
}

impl<A, C: Column, Rest> Step<A, Hit> for (C, Rest) {
    type Col = C;
    fn col(&self) -> &C {
        &self.0
    }
}
impl<A, C, Rest> Step<A, Miss> for (C, Rest)
where
    Rest: Holds<A>,
{
    type Col = <Rest as Holds<A>>::Col;
    fn col(&self) -> &Self::Col {
        self.1.col()
    }
}

impl<A, C, Rest> Holds<A> for (C, Rest)
where
    C: Column,
    C::Key: Matches<A>,
    (C, Rest): Step<A, <C::Key as Matches<A>>::Verdict>,
{
    type Col = <(C, Rest) as Step<A, <C::Key as Matches<A>>::Verdict>>::Col;
    fn col(&self) -> &Self::Col {
        <(C, Rest) as Step<A, <C::Key as Matches<A>>::Verdict>>::col(self)
    }
}

// ---- batch + row view ----
pub struct Batch<Cols> {
    pub cols: Cols,
    pub len: usize,
}
pub struct Row<'b, Cols> {
    cols: &'b Cols,
    i: usize,
}

impl<Cols> Batch<Cols> {
    fn rows(&self) -> impl Iterator<Item = Row<'_, Cols>> {
        (0..self.len).map(move |i| Row {
            cols: &self.cols,
            i,
        })
    }
}

pub trait Get<A> {
    type Value<'x>
    where
        Self: 'x;
    fn get(&self) -> Self::Value<'_>;
}
impl<'b, A, Cols> Get<A> for Row<'b, Cols>
where
    Cols: Holds<A>,
{
    type Value<'x>
        = <<Cols as Holds<A>>::Col as Column>::Item<'x>
    where
        Self: 'x;
    fn get(&self) -> Self::Value<'_> {
        <Cols as Holds<A>>::col(self.cols).get(self.i)
    }
}
impl<'b, Cols> Row<'b, Cols> {
    fn get<A>(&self) -> <Self as Get<A>>::Value<'_>
    where
        Self: Get<A>,
    {
        <Self as Get<A>>::get(self)
    }
}

// ---- a layer: generic over its input columns, rep bound on the impl block ----
pub trait Apply {
    type Input;
    type Requires;
    type Provides;
    fn run(&mut self, input: &Self::Input, out: &mut Vec<Vec<u8>>);
}

pub struct QualityTrim<Cols> {
    pub min_phred: u8,
    _c: PhantomData<Cols>,
}
impl<Cols> QualityTrim<Cols> {
    pub fn new(min_phred: u8) -> Self {
        Self {
            min_phred,
            _c: PhantomData,
        }
    }
    // the marked method the macro would keep and call — a plain, concrete function.
    fn trim(&self, seq: &[u8], qual: &[u8]) -> Vec<u8> {
        let keep = qual.iter().take_while(|&&q| q >= self.min_phred).count();
        seq[..keep].to_vec()
    }
}
impl<Cols> Apply for QualityTrim<Cols>
where
    Cols: Holds<Sequence> + Holds<Quality>,
    <Cols as Holds<Sequence>>::Col: Bytes,
    <Cols as Holds<Quality>>::Col: Bytes,
{
    type Input = Batch<Cols>;
    type Requires = (Sequence, Quality);
    type Provides = (Trimmed,);

    fn run(&mut self, batch: &Batch<Cols>, out: &mut Vec<Vec<u8>>) {
        for row in batch.rows() {
            let seq = row.get::<Sequence>();
            let qual = row.get::<Quality>();
            out.push(self.trim(seq, qual));
        }
    }
}

fn main() {
    let cols = (
        Names(vec![b"r1".to_vec(), b"r2".to_vec()]),
        (
            Reads(vec![b"ACGT".to_vec(), b"TTTT".to_vec()]),
            (Quals(vec![vec![40, 40, 10, 10], vec![40, 40, 40, 40]]), ()),
        ),
    );
    let batch = Batch { cols, len: 2 };

    let mut trim = QualityTrim::new(30);
    let mut out = Vec::new();
    trim.run(&batch, &mut out);

    for o in &out {
        println!("{:?}", std::str::from_utf8(o).unwrap());
    }
}

// ---- MIS-WIRE CHECKS (uncomment one at a time to see the error message) ----
//
// (1) Absent attr. The batch carries no Barcode column, so `Holds<Barcode>` walks off the
//     end of the tuple. Expect: "the trait bound `(): Holds<Barcode>` is not satisfied".
//
// fn miswire_absent(batch: &Batch<(Names, (Reads, (Quals, ())))>) {
//     for row in batch.rows() {
//         let _ = row.get::<Barcode>();
//     }
// }
//
// (2) Wrong representation. Sequence is stored Packed (Item = Kmer), but `run` demands &[u8].
//     Expect: the `for<'x> ...: Column<Item<'x> = &'x [u8]>` bound fails for `Packed`.
//
// fn miswire_rep() {
//     let cols = (
//         Packed {
//             bytes: vec![],
//             spans: vec![],
//         },
//         (Quals(vec![]), ()),
//     );
//     let batch = Batch { cols, len: 0 };
//     let mut trim = QualityTrim::new(30);
//     let mut out = Vec::new();
//     trim.run(&batch, &mut out);
// }
