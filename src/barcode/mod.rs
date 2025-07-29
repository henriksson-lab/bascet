<<<<<<< HEAD
=======
pub mod combinatorial_barcode_anysize;
pub mod combinatorial_barcode_8bp;
pub mod combinatorial_barcode_16bp;
pub mod chemistry;
pub mod atrandi_wgs_barcode;
>>>>>>> main
pub mod atrandi_rnaseq_barcode;
pub mod atrandi_wgs_barcode;
pub mod chemistry;
pub mod combinatorial_barcode;
pub mod general_barcode;
pub mod petriseq_barcode;
pub mod tenx;
pub mod trim_pairwise;
<<<<<<< HEAD

pub use combinatorial_barcode::CombinatorialBarcode;
pub use combinatorial_barcode::CombinatorialBarcodePart;
=======
pub mod petriseq_barcode;
pub mod parsebio;

pub use combinatorial_barcode_8bp::CombinatorialBarcodePart8bp;
pub use combinatorial_barcode_8bp::CombinatorialBarcode8bp;

pub use combinatorial_barcode_16bp::CombinatorialBarcodePart16bp;
pub use combinatorial_barcode_16bp::CombinatorialBarcode16bp;

pub use combinatorial_barcode_anysize::CombinatorialBarcodePart;
pub use combinatorial_barcode_anysize::CombinatorialBarcode;
>>>>>>> main

pub use atrandi_rnaseq_barcode::AtrandiRNAseqChemistry;
<<<<<<< HEAD
pub use atrandi_wgs_barcode::AtrandiWGSChemistry;
pub use general_barcode::GeneralCombinatorialBarcode;
pub use petriseq_barcode::PetriseqChemistry;
pub use tenx::TenxChemistry;

pub use chemistry::Chemistry;

// put CellID here???
=======
pub use tenx::TenxRNAChemistry;
pub use petriseq_barcode::PetriseqChemistry;
pub use parsebio::ParseBioChemistry3;

pub use chemistry::Chemistry;



// put CellID here???
>>>>>>> main
