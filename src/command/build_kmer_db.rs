//use crate::fileformat::shard::{CellID, ReadPair};

use std::sync::Arc;







pub struct BuildKMERdatabaseParams {

    pub path_input: std::path::PathBuf,
    pub path_tmp: std::path::PathBuf,
    pub path_output: std::path::PathBuf,

    pub threads_work: usize,  

}



pub struct BuildKMERdatabase {

}
impl BuildKMERdatabase {


    pub fn run(
        _params: &Arc<BuildKMERdatabaseParams>
    ) -> anyhow::Result<()> {






        Ok(())
    }



    
}