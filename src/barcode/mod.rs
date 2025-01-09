pub mod combinatorial_barcode;
pub mod chemistry;
pub mod atrandi_wgs_barcode;
pub mod atrandi_rnaseq_barcode;
pub mod general_barcode;
pub mod tenx;


pub use combinatorial_barcode::CombinatorialBarcodePart;
pub use combinatorial_barcode::CombinatorialBarcode;

pub use general_barcode::GeneralCombinatorialBarcode;
pub use atrandi_wgs_barcode::AtrandiWGSChemistry;
pub use atrandi_rnaseq_barcode::AtrandiRNAseqChemistry;
pub use tenx::TenxChemistry;


pub use chemistry::Chemistry;

// put CellID here???