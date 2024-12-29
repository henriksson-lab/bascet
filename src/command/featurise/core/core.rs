use std::{
    fs::{self, File},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use itertools::Itertools;
use zip::ZipArchive;

use crate::command::constants::RDB_PATH_INDEX_KMC_DBS;

use super::{params, state};

pub struct KMCProcessor {}

impl KMCProcessor {
    pub fn merge(
        params_io: &params::IO,
        params_threading: &params::Threading,
    ) -> anyhow::Result<PathBuf> {
        let file_rdb = File::open(&params_io.path_in).expect("Failed to open RDB file");
        let mut bufreader_rdb = BufReader::new(&file_rdb);
        let mut archive_rdb = ZipArchive::new(&mut bufreader_rdb).unwrap();

        let file_rdb_for_index = File::open(&params_io.path_in).expect("Failed to open RDB file");
        let mut bufreader_rdb_for_index = BufReader::new(&file_rdb_for_index);
        let mut archive_rdb_for_index = ZipArchive::new(&mut bufreader_rdb_for_index)
            .expect("Failed to create zip archive from RDB");

        let mut file_reads_index = archive_rdb_for_index
            .by_name(RDB_PATH_INDEX_KMC_DBS)
            .expect("Could not find rdb reads index file");
        let bufreader_reads_index = BufReader::new(&mut file_reads_index);

        let mut dbs_to_merge: Vec<(PathBuf, String)> = Vec::new();
        for line_reads_index in bufreader_reads_index.lines() {
            if let Ok(line_reads_index) = line_reads_index {
                let line_reads_split: Vec<&str> = line_reads_index.split(',').collect();
                {
                    let index_found = line_reads_split[0].parse::<usize>().expect(&format!(
                        "Could not parse index file at line: {}",
                        line_reads_index
                    ));

                    let mut zipfile_found = archive_rdb
                        .by_index(index_found)
                        .expect(&format!("No file at index {}", &index_found));

                    let zippath_found = zipfile_found.mangled_name();
                    match zippath_found.file_name().and_then(|ext| ext.to_str()) {
                        Some("kmc.kmc_pre") => {}
                        Some(_) => continue,
                        None => panic!("None value parsing read path"),
                    }

                    let path_barcode = zippath_found.parent().unwrap();
                    // HACK: '-' is a unary operator in kmc complex scripts
                    let str_barcode_sanitised = path_barcode.to_str().unwrap().replace("-", "_");
                    let path_barcode = Path::new(&str_barcode_sanitised);

                    let path_temp_dir = params_io.path_tmp.join(path_barcode);
                    let _ = fs::create_dir(&path_temp_dir);

                    let path_temp = path_temp_dir.join(zippath_found.file_name().unwrap());
                    let file_temp = File::create(&path_temp).unwrap();
                    let mut bufwriter_temp = BufWriter::new(&file_temp);

                    let mut bufreader_found = BufReader::new(&mut zipfile_found);
                    std::io::copy(&mut bufreader_found, &mut bufwriter_temp).unwrap();
                    dbs_to_merge.push((
                        path_temp.with_extension(""),
                        path_barcode.to_string_lossy().to_string(),
                    ));
                }
                {
                    let index_found = line_reads_split[1].parse::<usize>().expect(&format!(
                        "Could not parse index file at line: {}",
                        line_reads_index
                    ));

                    let mut zipfile_found = archive_rdb
                        .by_index(index_found)
                        .expect(&format!("No file at index {}", &index_found));

                    let zippath_found = zipfile_found.mangled_name();
                    match zippath_found.file_name().and_then(|ext| ext.to_str()) {
                        Some("kmc.kmc_suf") => {}
                        Some(_) => continue,
                        None => panic!("None value parsing read path"),
                    }

                    let path_barcode = zippath_found.parent().unwrap();
                    // HACK: '-' is a unary operator in kmc complex scripts
                    let str_barcode_sanitised = path_barcode.to_str().unwrap().replace("-", "_");
                    let path_barcode = Path::new(&str_barcode_sanitised);

                    let path_temp_dir = params_io.path_tmp.join(path_barcode);
                    let _ = fs::create_dir(&path_temp_dir);

                    let path_temp = path_temp_dir.join(zippath_found.file_name().unwrap());
                    let file_temp = File::create(&path_temp).unwrap();
                    let mut bufwriter_temp = BufWriter::new(&file_temp);

                    let mut bufreader_found = BufReader::new(&mut zipfile_found);
                    std::io::copy(&mut bufreader_found, &mut bufwriter_temp).unwrap();
                }
            }
        }

        let path_kmc_union_script = params_io.path_tmp.join("kmc_union.op");
        let file_kmc_union_script = File::create(&path_kmc_union_script).unwrap();
        let mut writer_kmc_union_script = BufWriter::new(&file_kmc_union_script);

        writeln!(writer_kmc_union_script, "INPUT:")?;
        for (path, barcode) in &dbs_to_merge {
            writeln!(
                writer_kmc_union_script,
                "{} = {}",
                barcode,
                path.to_str().unwrap()
            )
            .unwrap();
        }
        writeln!(writer_kmc_union_script, "OUTPUT:")?;

        let path_kmc_union = params_io.path_tmp.join("kmc_union");
        write!(
            writer_kmc_union_script,
            "{} = ",
            &path_kmc_union.to_str().unwrap()
        )
        .unwrap();

        write!(
            writer_kmc_union_script,
            "{}",
            dbs_to_merge.iter().map(|(_, barcode)| barcode).join(" + ")
        )
        .unwrap();

        writer_kmc_union_script.flush().unwrap();

        let kmc_union = std::process::Command::new("kmc_tools")
            .arg("complex")
            .arg(&path_kmc_union_script)
            .arg("-t")
            .arg(format!("{}", params_threading.threads_work))
            .output()?;

        if !kmc_union.status.success() {
            anyhow::bail!(
                "KMC merge failed: {}",
                String::from_utf8_lossy(&kmc_union.stderr)
            );
        }

        let path_dump = params_io.path_tmp.join("dump.txt");
        let kmc_dump = std::process::Command::new("kmc_tools")
            .arg("transform")
            .arg(&path_kmc_union)
            .arg("dump")
            .arg(&path_dump)
            .output()
            .expect("KMC dump command failed");

        if !kmc_dump.status.success() {
            anyhow::bail!(
                "KMC dump failed: {}",
                String::from_utf8_lossy(&kmc_dump.stderr)
            );
        }

        for (path, _) in &dbs_to_merge {
            fs::remove_dir_all(path.parent().unwrap()).unwrap();
        }

        Ok(path_dump)
    }
}
