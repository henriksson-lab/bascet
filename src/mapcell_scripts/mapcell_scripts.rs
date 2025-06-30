use std::{collections::HashMap, io::Cursor};
use std::sync::Arc;
use itertools::Itertools;

use crate::mapcell::{MapCellFunction, MapCellFunctionShellScript};



#[derive(Clone, Debug)] 
enum MapCellFunctionConstuctor {
    ShellScriptConstructor(Vec<u8>),
    OtherConstructor(Arc<Box<dyn MapCellFunction>>)
}
impl MapCellFunctionConstuctor {
    fn construct(&self) -> Arc<Box<dyn MapCellFunction>> {
        match self {
            MapCellFunctionConstuctor::ShellScriptConstructor(content) => {
                let mut read_content = Cursor::new(content.as_slice());
                let script = MapCellFunctionShellScript::new_from_reader(&mut read_content).expect("Failed to instanciate script");
                Arc::new(Box::new(script))
            },
            MapCellFunctionConstuctor::OtherConstructor(dat) => Arc::clone(dat)
        }
    }
}


fn get_preset_scripts() -> HashMap<String,MapCellFunctionConstuctor> {

    let mut map: HashMap<String, MapCellFunctionConstuctor> = HashMap::new(); 

    //Add all BASH scripts
    map.insert("abricate".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(include_bytes!("abricate.sh").to_vec()));
    map.insert("amrfinder".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(include_bytes!("amrfinder.sh").to_vec()));
    map.insert("ariba".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(include_bytes!("ariba.sh").to_vec()));
    map.insert("bakta".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(include_bytes!("bakta.sh").to_vec()));
    map.insert("checkm".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(include_bytes!("checkm.sh").to_vec()));
    map.insert("fastqc".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(include_bytes!("fastqc.sh").to_vec()));
    map.insert("gecco".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(include_bytes!("gecco.sh").to_vec()));
    map.insert("gtdbtk".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(include_bytes!("gtdbtk.sh").to_vec()));
    map.insert("kmc_process_reads".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(include_bytes!("kmc_process_reads.sh").to_vec()));
    map.insert("kmc_process_contigs".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(include_bytes!("kmc_process_contigs.sh").to_vec()));
    map.insert("prokka".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(include_bytes!("prokka.sh").to_vec()));
    map.insert("quast".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(include_bytes!("quast.sh").to_vec()));
    map.insert("skesa".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(include_bytes!("skesa.sh").to_vec()));
    map.insert("spades".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(include_bytes!("spades.sh").to_vec()));

    //Add all Rust scripts
    // map.insert("minhash_kmc".to_string(), MapCellFunctionConstuctor::OtherConstructor(Arc::new(Box::new(super::minhash_kmc::MapCellMinHashKMC{}))));
    // map.insert("minhash_fq".to_string(), MapCellFunctionConstuctor::OtherConstructor(Arc::new(Box::new(super::minhash_fq::MapCellMinHashFQ{}))));
    map.insert("countsketch_fq".to_string(), MapCellFunctionConstuctor::OtherConstructor(Arc::new(Box::new(super::countsketch_fq::MapCellCountSketchFQ{}))));

    map
}



pub fn get_preset_script(preset_name: impl Into<String>) -> Option<Arc<Box<dyn MapCellFunction>>> {
    let map_scripts = get_preset_scripts();
    let script = map_scripts.get(&preset_name.into());
    if let Some(script) = script {
        Some(script.construct())//.cloned()
    }else {
        None
    }
    

}

pub fn get_preset_script_names() -> Vec<String> {
    let map= get_preset_scripts();
    let names: Vec<String> =map.keys().sorted().cloned().collect();
    names
}

