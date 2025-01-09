use crate::fileformat::shard::{CellID, ReadPair};

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
        params_io: &Arc<BuildKMERdatabaseParams>
    ) -> anyhow::Result<()> {
   //     let (tx, rx) = crossbeam::channel::bounded::<Option<PathBuf>>(64);
//        let (tx, rx) = (Arc::new(tx), Arc::new(rx));

 //       thread_pool: &threadpool::ThreadPool,
 





        //// General threaded reader system; for each cell, call one funct
        //// should call a function 







        Ok(())
    }



    
}