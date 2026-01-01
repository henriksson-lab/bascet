pub mod atrandi_rnaseq_barcode;
pub mod atrandi_wgs_barcode;
pub mod chemistry;
pub mod combinatorial_barcode_16bp;
pub mod combinatorial_barcode_8bp;
pub mod combinatorial_barcode_anysize;
pub mod general_barcode;
pub mod parsebio;
pub mod petriseq_barcode;
pub mod tenx;
pub mod trim_pairwise;

pub use combinatorial_barcode_8bp::CombinatorialBarcode8bp;
pub use combinatorial_barcode_8bp::CombinatorialBarcodePart8bp;

pub use combinatorial_barcode_16bp::CombinatorialBarcode16bp;
pub use combinatorial_barcode_16bp::CombinatorialBarcodePart16bp;

pub use combinatorial_barcode_anysize::CombinatorialBarcode;
pub use combinatorial_barcode_anysize::CombinatorialBarcodePart;

pub use atrandi_rnaseq_barcode::AtrandiRNAseqChemistry;
// pub use parsebio::ParseBioChemistry3;
pub use petriseq_barcode::PetriseqChemistry;
pub use tenx::TenxRNAChemistry;

pub use chemistry::Chemistry;
