

// kmc has an API: https://github.com/refresh-bio/KMC/blob/master/kmc_dump/kmc_dump.cpp
// example opens kmer database for listing and print kmers within min_count and max_count



/////  '-' is a unary operator in kmc complex scripts  i.e. illegal. cannot use in barcode names. this is ok, we already banned it elsewhere!!


/////////////////// util to invoke kmc3 to merge databases


/*


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


*/