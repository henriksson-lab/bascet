pub mod atrandi_rnaseq_barcode;
pub mod atrandi_wgs_barcode;
pub mod chemistry;
pub mod combinatorial_barcode;
pub mod general_barcode;
pub mod petriseq_barcode;
pub mod tenx;
pub mod trim_pairwise;

pub use combinatorial_barcode::CombinatorialBarcode;
pub use combinatorial_barcode::CombinatorialBarcodePart;

pub use atrandi_rnaseq_barcode::AtrandiRNAseqChemistry;
pub use atrandi_wgs_barcode::AtrandiWGSChemistry;
pub use general_barcode::GeneralCombinatorialBarcode;
pub use petriseq_barcode::PetriseqChemistry;
pub use tenx::TenxChemistry;

pub use chemistry::Chemistry;

// put CellID here???
