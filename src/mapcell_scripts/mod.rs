use std::collections::HashMap;
use itertools::Itertools;


const PRESET_SCRIPT_TEST: &[u8] = include_bytes!("test_script.sh");
const PRESET_SCRIPT_QUAST: &[u8] = include_bytes!("quast.sh");
const PRESET_SCRIPT_SKESA: &[u8] = include_bytes!("skesa.sh");
const PRESET_SCRIPT_SPADES: &[u8] = include_bytes!("spades.sh");


pub fn get_preset_scripts() -> HashMap<String,Vec<u8>> {
    let mut map: HashMap<String, Vec<u8>> = HashMap::new();
    map.insert("testing".to_string(), PRESET_SCRIPT_TEST.to_vec());
    map.insert("quast".to_string(), PRESET_SCRIPT_QUAST.to_vec());
    map.insert("skesa".to_string(), PRESET_SCRIPT_SKESA.to_vec());
    map.insert("spades".to_string(), PRESET_SCRIPT_SPADES.to_vec());
    map
}


pub fn get_preset_script_names() -> Vec<String> {
    let map= get_preset_scripts();
    let names: Vec<String> =map.keys().sorted().cloned().collect();
    names
}

