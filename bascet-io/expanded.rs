pub mod tirp {
    use bascet_core::*;
    pub struct Tirp {
        pub(crate) inner_cursor: usize,
    }
    impl Tirp {
        #[doc(hidden)]
        #[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
        fn __orig_new() -> Result<Self, ()> {
            Ok(Tirp { inner_cursor: 0 })
        }
        #[inline(always)]
        #[allow(
            clippy::inline_always,
            clippy::use_self,
            clippy::missing_const_for_fn,
            clippy::elidable_lifetime_names
        )]
        #[allow(deprecated)]
        pub fn builder() -> TirpBuilder {
            TirpBuilder {
                __unsafe_private_phantom: ::core::marker::PhantomData,
                __unsafe_private_named: ::core::default::Default::default(),
            }
        }
    }
    #[must_use = "the builder does nothing until you call `build()` on it to finish building"]
    ///Use builder syntax to set the inputs and finish with [`build()`](Self::build()).
    #[allow(unused_parens)]
    #[allow(clippy::struct_field_names, clippy::type_complexity)]
    #[allow(deprecated)]
    pub struct TirpBuilder<S: tirp_builder::State = tirp_builder::Empty> {
        #[doc(hidden)]
        #[deprecated = "this field should not be used directly; it's an implementation detail, and if you access it directly, you may break some internal unsafe invariants; if you found yourself needing it, then you are probably doing something wrong; feel free to open an issue/discussion in our GitHub repository (https://github.com/elastio/bon) or ask for help in our Discord server (https://bon-rs.com/discord)"]
        __unsafe_private_phantom: ::core::marker::PhantomData<
            (fn() -> S, fn() -> ::core::marker::PhantomData<Tirp>),
        >,
        #[doc(hidden)]
        #[deprecated = "this field should not be used directly; it's an implementation detail, and if you access it directly, you may break some internal unsafe invariants; if you found yourself needing it, then you are probably doing something wrong; feel free to open an issue/discussion in our GitHub repository (https://github.com/elastio/bon) or ask for help in our Discord server (https://bon-rs.com/discord)"]
        __unsafe_private_named: (),
    }
    #[allow(unused_parens)]
    #[allow(dead_code)]
    #[automatically_derived]
    #[allow(deprecated)]
    impl<S: tirp_builder::State> TirpBuilder<S> {
        /// Finishes building and performs the requested action.
        #[inline(always)]
        #[allow(
            clippy::inline_always,
            clippy::future_not_send,
            clippy::missing_const_for_fn,
        )]
        pub fn build(self) -> Result<Tirp, ()>
        where
            S: tirp_builder::IsComplete,
        {
            <Tirp>::__orig_new()
        }
    }
    #[allow(unnameable_types, unreachable_pub, clippy::redundant_pub_crate)]
    /**Tools for manipulating the type state of [`TirpBuilder`].

See the [detailed guide](https://bon-rs.com/guide/typestate-api) that describes how all the pieces here fit together.*/
    #[allow(deprecated)]
    mod tirp_builder {
        #[doc(inline)]
        pub use ::bon::__::{IsSet, IsUnset};
        use ::bon::__::{Set, Unset};
        mod sealed {
            pub struct Sealed;
        }
        ///Builder's type state specifies if members are set or not (unset).
        pub trait State: ::core::marker::Sized {
            #[doc(hidden)]
            const SEALED: sealed::Sealed;
        }
        /**Marker trait that indicates that all required members are set.

In this state, you can finish building by calling the method [`TirpBuilder::build()`](super::TirpBuilder::build())*/
        pub trait IsComplete: State {
            #[doc(hidden)]
            const SEALED: sealed::Sealed;
        }
        #[doc(hidden)]
        impl<S: State> IsComplete for S {
            const SEALED: sealed::Sealed = sealed::Sealed;
        }
        #[deprecated = "this should not be used directly; it is an implementation detail; use the Set* type aliases to control the state of members instead"]
        #[doc(hidden)]
        #[allow(non_camel_case_types)]
        mod members {}
        /// Represents a [`State`] that has [`IsUnset`] implemented for all members.
        ///
        /// This is the initial state of the builder before any setters are called.
        pub struct Empty(());
        #[doc(hidden)]
        impl State for Empty {
            const SEALED: sealed::Sealed = sealed::Sealed;
        }
    }
    #[bascet(
        attrs = (Id, SequencePair, QualityPair, Umi),
        backing = ArenaBacking,
        marker = AsRecord
    )]
    pub struct Record {
        id: &'static [u8],
        sequence_pair: (&'static [u8], &'static [u8]),
        quality_pair: (&'static [u8], &'static [u8]),
        umi: &'static [u8],
        pub(crate) arena_backing: smallvec::SmallVec<[ArenaView<u8>; 2]>,
    }
    impl bascet_core::Composite for Record {
        type Attrs = (Id, SequencePair, QualityPair, Umi);
        type Single = (Id, SequencePair, QualityPair, Umi);
        type Collection = ();
        type Marker = bascet_core::AsRecord;
        type Intermediate = Self;
        type Backing = bascet_core::ArenaBacking;
    }
    impl bascet_core::Get<Id> for Record {
        type Value = &'static [u8];
        fn as_ref(&self) -> &Self::Value {
            &self.id
        }
        fn as_mut(&mut self) -> &mut Self::Value {
            &mut self.id
        }
    }
    impl bascet_core::Get<SequencePair> for Record {
        type Value = (&'static [u8], &'static [u8]);
        fn as_ref(&self) -> &Self::Value {
            &self.sequence_pair
        }
        fn as_mut(&mut self) -> &mut Self::Value {
            &mut self.sequence_pair
        }
    }
    impl bascet_core::Get<QualityPair> for Record {
        type Value = (&'static [u8], &'static [u8]);
        fn as_ref(&self) -> &Self::Value {
            &self.quality_pair
        }
        fn as_mut(&mut self) -> &mut Self::Value {
            &mut self.quality_pair
        }
    }
    impl bascet_core::Get<Umi> for Record {
        type Value = &'static [u8];
        fn as_ref(&self) -> &Self::Value {
            &self.umi
        }
        fn as_mut(&mut self) -> &mut Self::Value {
            &mut self.umi
        }
    }
    impl bascet_core::Get<bascet_core::ArenaBacking> for Record {
        type Value = smallvec::SmallVec<[ArenaView<u8>; 2]>;
        fn as_ref(&self) -> &Self::Value {
            &self.arena_backing
        }
        fn as_mut(&mut self) -> &mut Self::Value {
            &mut self.arena_backing
        }
    }
    #[automatically_derived]
    impl ::core::default::Default for Record {
        #[inline]
        fn default() -> Record {
            Record {
                id: ::core::default::Default::default(),
                sequence_pair: ::core::default::Default::default(),
                quality_pair: ::core::default::Default::default(),
                umi: ::core::default::Default::default(),
                arena_backing: ::core::default::Default::default(),
            }
        }
    }
    #[automatically_derived]
    impl ::core::clone::Clone for Record {
        #[inline]
        fn clone(&self) -> Record {
            Record {
                id: ::core::clone::Clone::clone(&self.id),
                sequence_pair: ::core::clone::Clone::clone(&self.sequence_pair),
                quality_pair: ::core::clone::Clone::clone(&self.quality_pair),
                umi: ::core::clone::Clone::clone(&self.umi),
                arena_backing: ::core::clone::Clone::clone(&self.arena_backing),
            }
        }
    }
    impl Record {
        pub unsafe fn from_raw(
            buf_record: &[u8],
            pos_tab: [usize; 7],
            arena_view: ArenaView<u8>,
        ) -> Self {
            let id = buf_record.get_unchecked(..pos_tab[0]);
            let r1 = buf_record.get_unchecked(pos_tab[2] + 1..pos_tab[3]);
            let r2 = buf_record.get_unchecked(pos_tab[3] + 1..pos_tab[4]);
            let q1 = buf_record.get_unchecked(pos_tab[4] + 1..pos_tab[5]);
            let q2 = buf_record.get_unchecked(pos_tab[5] + 1..pos_tab[6]);
            let umi = buf_record.get_unchecked(pos_tab[6] + 1..);
            if likely_unlikely::unlikely(r1.len() != q1.len()) {
                {
                    ::core::panicking::panic_fmt(
                        format_args!(
                            "r1/q1 length mismatch: {0:?} != {1:?}",
                            r1.len(),
                            q1.len(),
                        ),
                    );
                };
            }
            if likely_unlikely::unlikely(r2.len() != q2.len()) {
                {
                    ::core::panicking::panic_fmt(
                        format_args!(
                            "r1/q1 length mismatch: {0:?} != {1:?}",
                            r2.len(),
                            q2.len(),
                        ),
                    );
                };
            }
            let static_id: &'static [u8] = unsafe { std::mem::transmute(id) };
            let static_r1: &'static [u8] = unsafe { std::mem::transmute(r1) };
            let static_r2: &'static [u8] = unsafe { std::mem::transmute(r2) };
            let static_q1: &'static [u8] = unsafe { std::mem::transmute(q1) };
            let static_q2: &'static [u8] = unsafe { std::mem::transmute(q2) };
            let static_umi: &'static [u8] = unsafe { std::mem::transmute(umi) };
            Self {
                id: static_id,
                sequence_pair: (static_r1, static_r2),
                quality_pair: (static_q1, static_q2),
                umi: static_umi,
                arena_backing: {
                    let count = 0usize + 1usize;
                    let mut vec = ::smallvec::SmallVec::new();
                    if count <= vec.inline_size() {
                        vec.push(arena_view);
                        vec
                    } else {
                        ::smallvec::SmallVec::from_vec(
                            <[_]>::into_vec(::alloc::boxed::box_new([arena_view])),
                        )
                    }
                },
            }
        }
    }
    #[bascet(
        attrs = (
            Id,
            SequencePair = vec_sequence_pairs,
            QualityPair = vec_quality_pairs,
            Umi = vec_umis
        ),
        backing = ArenaBacking,
        marker = AsCell<Accumulate>,
        intermediate = Record
    )]
    pub struct Cell {
        id: &'static [u8],
        #[collection]
        vec_sequence_pairs: Vec<(&'static [u8], &'static [u8])>,
        #[collection]
        vec_quality_pairs: Vec<(&'static [u8], &'static [u8])>,
        #[collection]
        vec_umis: Vec<&'static [u8]>,
        pub(crate) arena_backing: smallvec::SmallVec<[ArenaView<u8>; 2]>,
    }
    impl bascet_core::Composite for Cell {
        type Attrs = (Id, SequencePair, QualityPair, Umi);
        type Single = (Id);
        type Collection = (SequencePair, QualityPair, Umi);
        type Marker = bascet_core::AsCell<Accumulate>;
        type Intermediate = Record;
        type Backing = bascet_core::ArenaBacking;
    }
    impl bascet_core::Get<Id> for Cell {
        type Value = &'static [u8];
        fn as_ref(&self) -> &Self::Value {
            &self.id
        }
        fn as_mut(&mut self) -> &mut Self::Value {
            &mut self.id
        }
    }
    impl bascet_core::Get<SequencePair> for Cell {
        type Value = Vec<(&'static [u8], &'static [u8])>;
        fn as_ref(&self) -> &Self::Value {
            &self.vec_sequence_pairs
        }
        fn as_mut(&mut self) -> &mut Self::Value {
            &mut self.vec_sequence_pairs
        }
    }
    impl bascet_core::Get<QualityPair> for Cell {
        type Value = Vec<(&'static [u8], &'static [u8])>;
        fn as_ref(&self) -> &Self::Value {
            &self.vec_quality_pairs
        }
        fn as_mut(&mut self) -> &mut Self::Value {
            &mut self.vec_quality_pairs
        }
    }
    impl bascet_core::Get<Umi> for Cell {
        type Value = Vec<&'static [u8]>;
        fn as_ref(&self) -> &Self::Value {
            &self.vec_umis
        }
        fn as_mut(&mut self) -> &mut Self::Value {
            &mut self.vec_umis
        }
    }
    impl bascet_core::Get<bascet_core::ArenaBacking> for Cell {
        type Value = smallvec::SmallVec<[ArenaView<u8>; 2]>;
        fn as_ref(&self) -> &Self::Value {
            &self.arena_backing
        }
        fn as_mut(&mut self) -> &mut Self::Value {
            &mut self.arena_backing
        }
    }
    #[automatically_derived]
    impl ::core::default::Default for Cell {
        #[inline]
        fn default() -> Cell {
            Cell {
                id: ::core::default::Default::default(),
                vec_sequence_pairs: ::core::default::Default::default(),
                vec_quality_pairs: ::core::default::Default::default(),
                vec_umis: ::core::default::Default::default(),
                arena_backing: ::core::default::Default::default(),
            }
        }
    }
}
