use anyhow::Result;
use clap::Args;
use itertools::Itertools;
use std::path::PathBuf;
use std::io;
use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::BufRead;
use zip::read::ZipArchive;

pub const DEFAULT_PATH_TEMP: &str = "temp";


#[derive(Args)]
pub struct ExtractStreamCMD {
    #[arg(short = 'i', value_parser)]  /// Zip-file name. Note that this command takes a shard, not a full bascet (can support later!) -- this is for speed
    pub path_in: PathBuf,

}
impl ExtractStreamCMD {
    pub fn try_execute(&mut self) -> Result<()> {

        //This is an interactive terminal to navigate Bascet-ZIP content; could generalize to any container later

        //Tell version info etc. Final line is "ready" or "error"
        println!("version_major:1");
        println!("version_minor:0");
        let file = File::open(&self.path_in);
        
        if let Ok(file) = file {
            println!("ready");

            let bufreader_shard = BufReader::new(file);
            let mut zip_shard =    ZipArchive::new(bufreader_shard).unwrap();
    
    
            loop {
                let mut buffer = String::new();
                let stdin = io::stdin();
                stdin.read_line(&mut buffer)?;    
                let buffer = buffer.trim();
    
                if buffer=="help" {
                    println!("Available commands: exit ls showtext extract_to");
                    println!("Note that this system is optimized for streaming data to Zorn, and not for being user friendly to terminal users!");
                } else if buffer=="ls" {
                    let list_files = zip_shard.file_names().collect_vec();
                    println!("{}", list_files.len());
                    for f in list_files {
                        println!("{}", f);
                    }
                } else if buffer.starts_with("showtext") {
                    //let fname=&buffer[b"showtext ".len()..];
                    let mut splitter=buffer.split_whitespace();
                    splitter.next();
                    let zip_entry_name = splitter.next().expect("Did not get zip entry name");
    
    
                    let entry = zip_shard.by_name(&zip_entry_name);
                    if let Ok(entry) = entry {
                        if entry.is_file() {
    
                            //Figure out how many lines there are in this file
                            let reader = io::BufReader::new(entry);
                            let mut lines: Vec<String> = Vec::new();
                            for line in reader.lines().map_while(Result::ok) {
                                lines.push(line);
                            }
    
                            //Print each line
                            println!("{}", lines.len());
                            for line in lines {
                                println!("{}", line);
                            }
                        } else {
                            println!("error not a file");
                        }
                    } else {
                        println!("error missing -{}-", zip_entry_name);
                    }
    
                } else if buffer.starts_with("extract_to") {
                    let mut splitter=buffer.split_whitespace();
                    splitter.next();
                    let zip_entry_name = splitter.next().expect("Did not get zip entry name");
                    let path_outfile = splitter.next().expect("Did not get out file name");
    
                    //let fname=&buffer[b"showtext ".len()..];
                    let mut entry = zip_shard.by_name(&zip_entry_name);
                    if let Ok(entry) = &mut entry {
                        if entry.is_file() {
                            let file_out = File::create(&path_outfile).unwrap();
                            let mut bufwriter_out = BufWriter::new(&file_out);
                            let mut bufreader_found = BufReader::new(entry);
                            std::io::copy(&mut bufreader_found, &mut bufwriter_out).unwrap();
                            println!("done");
                        } else {
                            println!("error not a file");
                        }
                    } else {
                        println!("error missing -{}-", zip_entry_name);
                    }
    
                } else if buffer=="exit" {
                    break;
                } else {
                    println!("error unknown command -{}-", buffer);
                }
            }
            println!("exiting");            

        } else {
            println!("error no such file {}", self.path_in.display());
        }
        Ok(())
    }
}
