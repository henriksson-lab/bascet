use std::sync::Arc;
use std::fs::File;
use std::path::PathBuf;
use std::io::BufRead;
use std::io::BufReader;
use std::collections::BTreeMap;
use hdf5::File as H5File;
use anyhow::Result;
use clap::Args;

pub const DEFAULT_PATH_TEMP: &str = "temp";




#[derive(Args)]
pub struct KrakenCMD {

    // Input bascet or gascet
    #[arg(short = 'i', value_parser= clap::value_parser!(PathBuf))]
    pub path_in: PathBuf,

    // Temp file directory
    #[arg(short = 't', value_parser= clap::value_parser!(PathBuf), default_value = DEFAULT_PATH_TEMP)]
    pub path_tmp: PathBuf,

    // Output bascet
    #[arg(short = 'o', value_parser = clap::value_parser!(PathBuf))]
    pub path_out: PathBuf,
    
}
impl KrakenCMD {

    /// Run the commandline option.
    /// This one takes a KRAKEN output-file, and outputs a taxonomy count matrix
    pub fn try_execute(&mut self) -> Result<()> {

        let params = Kraken {
            path_tmp: self.path_tmp.clone(),            
            path_input: self.path_in.clone(),            
            path_output: self.path_out.clone(),   
        };

        let _ = Kraken::run(
            &Arc::new(params)
        );

        log::info!("Kraken has finished succesfully");
        Ok(())
    }
}


/// KRAKEN count matrix constructor.
pub struct Kraken {
    pub path_input: std::path::PathBuf,
    pub path_tmp: std::path::PathBuf,
    pub path_output: std::path::PathBuf,
}
impl Kraken {

    /// Run the algorithm
    pub fn run(
        params: &Arc<Kraken>
    ) -> anyhow::Result<()> {

        //Prepare matrix that we will store into
        let mut mm = KrakenCountMatrix::new();

        //Open input file
        let file_in = File::open(&params.path_input).unwrap();
        let bufreader = BufReader::new(&file_in);

        //Counter for how many times each taxid has been seen for one cell
        let mut taxid_counter= BTreeMap::new();

        //Loop through all reads; group by cell
        let mut last_cellid = None;
        for (_index, rline) in bufreader.lines().enumerate() {  //////////// should be a plain list of features
            if let Ok(line) = rline { ////// when is this false??

                //Divide the row
                let mut splitter = line.split("\t");
                let is_categorized= splitter.next().unwrap();


                if is_categorized=="C" {
                    let readname= splitter.next().unwrap();
                    let taxid: usize= splitter.next().unwrap().parse().expect("Failed to parse taxon id");
    
                    //Figure out what cell this is
                    let mut splitter = readname.split(":");
                    let cellid = Some(splitter.next().unwrap().to_string());

                    //If this is a new cell, then store everything we have so far in the count matrix
                    if last_cellid != cellid {
                        //Store if there is a previous cell. Could skip this "if", if we read first line before starting. TODO
                        if let Some(last_cellid_s) = last_cellid {
                            //Add taxid counts for last cell
                            mm.add_taxids(&last_cellid_s, &mut taxid_counter);
                            taxid_counter.clear();
                        }
                        //Move to track the next cell
                        last_cellid = cellid;
                    }
    
                    //Count this taxon id
                    let values = taxid_counter.entry(taxid).or_insert(0);
                    *values += 1;
                }
            } else {
                anyhow::bail!("Failed to read one line of input");
            }
        }

        //Need to also add counts for the last cell
        if let Some(last_cellid_s) = last_cellid {
            mm.add_taxids(&last_cellid_s, &mut taxid_counter);
        }


//        C       BASCET_D2_F5_H7_C10::901        86661   257     0:1 1386:53 86661:6 1386:7 86661:17 1386:10 A:129


        //Save the final count matrix
        println!("Storing count table to {}", params.path_output.display());
        mm.save_to_anndata(&params.path_output).expect("Failed to save to HDF5 file");

        //TODO delete temp files
        println!("Cleaning up temp files");
        //fs::remove_dir_all(&params.path_tmp).unwrap();

        Ok(())
    }
}





/*

Example data

C       BASCET_D2_F5_H7_C10::901        86661   257     0:1 1386:53 86661:6 1386:7 86661:17 1386:10 A:129
C       BASCET_D2_F5_H7_C10::902        28384   257     0:56 1:11 0:14 28384:9 0:4 A:129
C       BASCET_D2_F5_H7_C10::902        1783272 257     0:11 2:3 1:26 2:10 1783272:6 0:16 9606:3 0:19 A:129
C       BASCET_D2_F5_H7_C10::903        2026187 257     0:29 2026187:8 86661:30 2026187:23 86661:4 A:129
C       BASCET_D2_F5_H7_C10::903        2026187 257     86661:33 2026187:4 86661:5 2026187:23 86661:29 A:129
C       BASCET_D2_F5_H7_C10::904        86661   257     86661:94 A:129
C       BASCET_D2_F5_H7_C10::904        86661   257     86661:94 A:129
C       BASCET_D2_F5_H7_C10::905        1386    257     1386:75 0:19 A:129
C       BASCET_D2_F5_H7_C10::905        1386    257     0:3 1386:76 0:15 A:129 

https://software.cqls.oregonstate.edu/updates/docs/kraken2/MANUAL.html#standard-kraken-output-format

1. "C"/"U": a one letter code indicating that the sequence was either classified or unclassified.
2. The sequence ID, obtained from the FASTA/FASTQ header.
3. The taxonomy ID Kraken 2 used to label the sequence; this is 0 if the sequence is unclassified.
4. The length of the sequence in bp. In the case of paired read data, this will be a string containing the lengths of the two sequences in bp, separated by a pipe character, e.g. "98|94".
5. A space-delimited list indicating the LCA mapping of each k-mer in the sequence(s). For example, "562:13 561:4 A:31 0:1 562:3" would indicate that:

the first 13 k-mers mapped to taxonomy ID #562
the next 4 k-mers mapped to taxonomy ID #561
the next 31 k-mers contained an ambiguous nucleotide
the next k-mer was not in the database
the last 3 k-mers mapped to taxonomy ID #562

Note that paired read data will contain a "|:|" token in this list to indicate the end of one read and the beginning of another.

*/










/// Specialized count matrix for Kraken taxid counting per cell. As taxid is numeric, there is no need to assign column names
pub struct KrakenCountMatrix {

    pub cells: Vec<String>,
    pub entries: Vec<(u32,u32,u32)>,   //row, col, count
    pub max_taxid: usize

}
impl KrakenCountMatrix {

    pub fn new() -> Self {
        Self {
            cells: Vec::new(),
            entries: Vec::new(),
            max_taxid: 0
        }
    }

    pub fn add_cell(&mut self, cell: &String) -> usize {
        let id = self.cells.len();
        self.cells.push(cell.clone());
        id as usize
    }

    pub fn add_value(
        &mut self, 
        cell: usize, 
        feature: usize, 
        value: u32
    ) {
        self.entries.push((cell as u32, feature as u32, value));

        if feature > self.max_taxid {
            self.max_taxid = feature;
        }
    }


    pub fn add_taxids(
        &mut self, 
        cell: &String,
        taxid_counter: &mut BTreeMap<usize, u32>
    ) {

        let cell_index = self.add_cell(&cell);
        for (taxid, cnt) in taxid_counter {
            self.add_value(cell_index, *taxid, *cnt);
        }

    }

    /// Save matrix as anndata-like hdf5-file
    pub fn save_to_anndata(&self, p: &PathBuf) -> anyhow::Result<()> {
        
        //Delete output file if it exists already; HDF5 library complains otherwise
        if p.exists() {
            std::fs::remove_file(&p).expect("Failed to delete previous output file");
        }
        
        let file = H5File::create(p)?; // open for writing

        //Extract separate vectors
        let csr_data: Vec<u32> = self.entries.iter().map(|(_row,_col,data)| *data).collect();
        let csr_cols: Vec<u32> = self.entries.iter().map(|(_row,col,_data)| *col).collect();
        let csr_rows: Vec<u32> = self.entries.iter().map(|(row,_col,_data)| *row).collect(); //must be compressed

        //Figure out where rows start in this list        ////////// This assumes that we added counts, cell by cell. otherwise sort the array before!!
        let mut ind_ptr:Vec<u32> = Vec::new(); 
        ind_ptr.push(0);
        for i in 1..(csr_rows.len()) {
            if csr_rows[i] != csr_rows[i-1] {
                ind_ptr.push(i as u32);
            }
        }
        //For some reason, also need an entry representing the length of data
        ind_ptr.push((csr_data.len()) as u32);



        //Store the sparse matrix here
        let group = file.create_group("X")?; 
        let builder = group.new_dataset_builder();
        let _ = builder.with_data(&csr_data.as_slice()).create("data")?;    //Data
        let builder = group.new_dataset_builder();
        let _ = builder.with_data(&csr_cols.as_slice()).create("indices")?; // Columns
        let builder = group.new_dataset_builder();
        let _ = builder.with_data(&ind_ptr.as_slice()).create("indptr")?;  // Rows
        


        //Store the matrix size
        let n_rows = self.cells.len();
        let n_cols = self.max_taxid+1;  //Note +1; because taxid 0 means unclassified. in R, all taxid will be shifted by 1!!
        let builder = group.new_dataset_builder();
        let _ = builder.with_data(&[n_rows,n_cols].as_slice()).create("shape")?;    


        //Store the names of the cells
        let list_cell_names = vec_to_h5_string(self.cells.as_slice());
        let group = file.create_group("obs")?; 
        let builder = group.new_dataset_builder();
        let _ = builder.
            with_data(list_cell_names.as_slice()).
            create("_index")?;

        //Names of features are not stored; taxid are numeric already        

        Ok(())

    }



}

/// Helper: Take a list of strings, and generate a list of HDF5-type strings
fn vec_to_h5_string(list: &[String]) -> Vec<hdf5::types::VarLenUnicode> {
    list.iter().map(|f| f.parse().unwrap()).collect()
}

