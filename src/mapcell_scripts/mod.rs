pub mod minhash;


use std::{collections::HashMap, io::Cursor};
use std::sync::Arc;
use itertools::Itertools;

use crate::mapcell::{MapCellFunction, MapCellFunctionShellScript};



const PRESET_SCRIPT_TEST: &[u8] = include_bytes!("test_script.sh");
const PRESET_SCRIPT_QUAST: &[u8] = include_bytes!("quast.sh");
const PRESET_SCRIPT_SKESA: &[u8] = include_bytes!("skesa.sh");
const PRESET_SCRIPT_SPADES: &[u8] = include_bytes!("spades.sh");

const PRESET_SCRIPT_KMC_PROCESS_READS: &[u8] = include_bytes!("kmc_process_reads.sh");
const PRESET_SCRIPT_KMC_PROCESS_CONTIGS: &[u8] = include_bytes!("kmc_process_contigs.sh");





enum MapCellFunctionConstuctor {
    ShellScriptConstructor(Vec<u8>),
    OtherConstructor(Arc<Box<dyn MapCellFunction>>)
}
impl MapCellFunctionConstuctor {
    fn construct(&self) -> Arc<Box<dyn MapCellFunction>> {
        match self {
            MapCellFunctionConstuctor::ShellScriptConstructor(content) => {
                let mut read_content = Cursor::new(content.as_slice());
                let script = MapCellFunctionShellScript::new_from_reader(&mut read_content).unwrap();
                Arc::new(Box::new(script))
            },
            MapCellFunctionConstuctor::OtherConstructor(dat) => Arc::clone(dat)
        }
    }
}


fn get_preset_scripts() -> HashMap<String,MapCellFunctionConstuctor> {

    let mut map: HashMap<String, MapCellFunctionConstuctor> = HashMap::new(); 

    //Add all BASH scripts
    map.insert("testing".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(PRESET_SCRIPT_TEST.to_vec()));
    map.insert("quast".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(PRESET_SCRIPT_QUAST.to_vec()));
    map.insert("skesa".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(PRESET_SCRIPT_SKESA.to_vec()));
    map.insert("spades".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(PRESET_SCRIPT_SPADES.to_vec()));

    map.insert("kmc_process_reads".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(PRESET_SCRIPT_KMC_PROCESS_READS.to_vec()));
    map.insert("kmc_process_contigs".to_string(), MapCellFunctionConstuctor::ShellScriptConstructor(PRESET_SCRIPT_KMC_PROCESS_CONTIGS.to_vec()));


    //Add all Rust scripts
    map.insert("minhash".to_string(), MapCellFunctionConstuctor::OtherConstructor(Arc::new(Box::new(minhash::MapCellKmcMinHash{}))));

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

