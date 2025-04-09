use anyhow::Result;
use clap::Args;
use std::io::{self, BufRead, stdout, Write, BufWriter};

#[derive(Args)]
pub struct PipeSamAddTagsCMD {
    // No arguments are taken. This command is for piping only.
    // This function takes a SAM file on stdin, and prints a SAM file with @BC and @UMI added.
    // Thus the file will conform to the specification of certain downstream tools
}
impl PipeSamAddTagsCMD {
    pub fn try_execute(&mut self) -> Result<()> {

        let mut writer=BufWriter::new(stdout());

        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let line = line.unwrap();

            if line.starts_with("@") {
                //This is a header line
                writeln!(writer, "{}", line).unwrap();
            } else {
                //This is a read that need to be mangled
                let (cell_id, umi) = crate::fileformat::bam::readname_to_cell_umi(line.as_bytes());
                        
                writer.write_all(line.as_bytes())?;
                writer.write_all(b"\tCB:Z:")?;
                writer.write_all(cell_id)?;
                writer.write_all(b"\tUB:Z:")?;
                writer.write_all(umi)?;
                writer.write_all(b"\n")?;

                //Typical 10x read
                //A00689:440:HNTNGDRXY:1:1232:23882:9157	0	chr1	629349	3	89M1S	*	0	0	AAACTTCCTACCACTCACCCTAGCATTACTTATATGATATGTCTCCATACCCATTACAATCTCCAGCATTCCCCCTCAAACCTTAAAAAA	FFFFFFFFFFFFFF:FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF	NH:i:2	HI:i:1	AS:i:83	nM:i:2	RG:Z:lib1:0:1:HNTNGDRXY:1	RE:A:I	xf:i:0	CR:Z:ACGACTTAGTATTGTG	CY:Z:FFFFFFFFFFFFFFFF	CB:Z:ACGACTTAGTATTGTG-1	UR:Z:TAGGCAGAAGCT	UY:Z:FFFFFFFFFFFF	UB:Z:TAGGCAGAAGCT
                //Thus add this:
                //CB:Z:ACGACTTAGTATTGTG-1
                //UB:Z:TAGGCAGAAGCT

            }
        }
        Ok(())
    }
}




