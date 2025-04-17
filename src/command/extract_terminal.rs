use anyhow::bail;
use anyhow::Result;
use clap::Args;
use itertools::Itertools;
use std::io;
use std::fs::File;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::BufRead;
use zip::read::ZipArchive;

pub const DEFAULT_PATH_TEMP: &str = "temp";


#[derive(Args)]
pub struct ExtractStreamCMD {
    #[arg(short = 'i', value_parser)]  /// can take a file. note that we use a string to ensure comparability with later paths
    pub path_in: Option<String>,

}
impl ExtractStreamCMD {
    pub fn try_execute(&mut self) -> Result<()> {

        //This is an interactive terminal to navigate Bascet-ZIP content; could generalize to any container later

        //Tell version info etc. Final line is "ready" or "error"
        println!("version_major:1");
        println!("version_minor:0");

        let mut stream = ExtractStream {
            curfile: None
        };

        //Open file provided as argument, if given
        if let Some(path_in) = &self.path_in {
            let res = stream.open(path_in);
            if res.is_err() {
                println!("error could not open file provided as argument");
                bail!("");
            } else {
                println!("ready");
            }
        } else {
            println!("ready");
        }

        //Start the terminal loop
        stream.run()
    
            
    }
}





pub struct ExtractStream {

    curfile: Option<(String, ZipArchive<BufReader<File>>)>

}
impl ExtractStream {


    pub fn open(&mut self, path_in: &String) -> Result<()> {
        
        let file = File::open(path_in);

        if let Ok(file) = file {
            let bufreader_shard = BufReader::new(file);
            self.curfile = Some((
                path_in.clone(),
                ZipArchive::new(bufreader_shard).expect("error could not open zip archive"))
            );
            anyhow::Ok(())    
        } else {
            bail!("error no such file {}", path_in);
        }
    }


    pub fn run(&mut self) -> Result<()> {

        loop {
            let mut buffer = String::new();
            let stdin = io::stdin();
            stdin.read_line(&mut buffer)?;    
            let buffer = buffer.trim();

            if buffer=="help" {

                /////////////////////////////// help
                println!("Available commands: exit ls showtext extract_to");
                println!("Note that this system is optimized for streaming data to Zorn, and not for being user friendly to terminal users!");
            } else if buffer=="ls" {

                /////////////////////////////// ls
                if let Some((_, zip_shard)) = &self.curfile {
                    let list_files = zip_shard.file_names().collect_vec();
                    println!("{}", list_files.len());
                    for f in list_files {
                        println!("{}", f);
                    }
                } else {
                    println!("error no file open");
                }


            } else if buffer.starts_with("open") {

                /////////////////////////////// open ///////////////////////////////
                let mut splitter=buffer.split_whitespace();
                splitter.next();
                let path_in = splitter.next().expect("Did not get file name");

                //Only open file if it is different from the currently open file
                if let Some((f,_)) = &self.curfile {
                    if f==path_in {
                        println!("ok");
                        continue;
                    }
                }

                //If a file is open, close it
                self.curfile = None;

                //Attempt to open new file
                let res = self.open(&path_in.to_string());
                if res.is_err() {
                    println!("error could not open file provided as argument");
                } else {
                    println!("ok");
                }

            } else if buffer.starts_with("showtext") {

                /////////////////////////////// showtext ///////////////////////////////
                if let Some((_, zip_shard)) = &mut self.curfile {
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
    
                            //Print each line of the file
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
                } else {
                    println!("error no file open");
                }

            } else if buffer.starts_with("extract_to") {

                /////////////////////////////// extract_to ///////////////////////////////
                if let Some((_, zip_shard)) = &mut self.curfile {

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

                } else {
                    println!("error no file open");
                }

            } else if buffer=="exit" {

                /////////////////////////////// exit ///////////////////////////////
                break;
            } else {

                /////////////////////////////// anything else ///////////////////////////////
                println!("error unknown command -{}-", buffer);
            }
        }
        println!("exiting");            

    Ok(())


    }
}
 //   pub fn try_execute(&mut self) -> Result<()> {