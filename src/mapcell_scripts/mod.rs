pub mod kmc_minhash;


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











pub fn get_preset_scripts() -> HashMap<String,Arc<Box<dyn MapCellFunction>>> {

    let mut map: HashMap<String, Arc<Box<dyn MapCellFunction>>> = HashMap::new(); 

    //Add all BASH scripts
    for (name, content) in get_preset_scripts_bash() {
        let mut read_content = Cursor::new(content.as_slice());
        let script = MapCellFunctionShellScript::new_from_reader(&mut read_content).unwrap();
        map.insert(name, Arc::new(Box::new(script)));
    }

    //Add all Rust scripts
    map.insert("kmc_minhash".to_string(), Arc::new(Box::new(kmc_minhash::MapCellKmcMinHash{})));

    map
}



fn get_preset_scripts_bash() -> HashMap<String,Vec<u8>> {
    let mut map: HashMap<String, Vec<u8>> = HashMap::new();
    map.insert("testing".to_string(), PRESET_SCRIPT_TEST.to_vec());
    map.insert("quast".to_string(), PRESET_SCRIPT_QUAST.to_vec());
    map.insert("skesa".to_string(), PRESET_SCRIPT_SKESA.to_vec());
    map.insert("spades".to_string(), PRESET_SCRIPT_SPADES.to_vec());

    map.insert("kmc_process_reads".to_string(), PRESET_SCRIPT_KMC_PROCESS_READS.to_vec());
    map.insert("kmc_process_contigs".to_string(), PRESET_SCRIPT_KMC_PROCESS_CONTIGS.to_vec());

    map
}



pub fn get_preset_script(preset_name: impl Into<String>) -> Option<Arc<Box<dyn MapCellFunction>>> {
    let map_scripts = get_preset_scripts();
    map_scripts.get(&preset_name.into()).cloned()
}

pub fn get_preset_script_names() -> Vec<String> {
    let map= get_preset_scripts();
    let names: Vec<String> =map.keys().sorted().cloned().collect();
    names
}

