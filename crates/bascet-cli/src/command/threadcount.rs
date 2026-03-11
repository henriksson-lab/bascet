pub fn determine_thread_counts_1(total: Option<usize>) -> anyhow::Result<usize> {
    if let Some(total) = total {
        anyhow::Ok(total)
    } else {
        let total = std::thread::available_parallelism();
        if let Ok(total) = total {
            anyhow::Ok(total.get())
        } else {
            println!("Could not autodetect the number of threads available. Setting to 1, but it is better if you specify");
            anyhow::Ok(1)
        }
    }
}

pub fn determine_thread_counts_2(
    total: Option<usize>,
    threads1: Option<usize>,
    threads2: Option<usize>,
) -> anyhow::Result<(usize, usize)> {
    if let Some(total) = total {
        let threads1 = some_min1(threads1)?;
        let threads2 = min1(total - threads1);
        anyhow::Ok((threads1, threads2))
    } else {
        let total = std::thread::available_parallelism();
        if let Ok(total) = total {
            let threads1 = some_min1(threads1)?;
            let threads2 = min1(total.get() - threads1);
            anyhow::Ok((threads1, threads2))
        } else {
            //println!("Could not autodetect the number of threads available. Will assume the minimum number, but it is better if you specify");

            let threads1 = some_min1(threads1)?;
            let threads2 = some_min1(threads2)?;
            anyhow::Ok((threads1, threads2))
        }
    }
}

pub fn determine_thread_counts_3(
    total: Option<usize>,
    threads1: Option<usize>,
    threads2: Option<usize>,
    threads3: Option<usize>,
) -> anyhow::Result<(usize, usize, usize)> {
    if let Some(total) = total {
        let threads1 = some_min1(threads1)?;
        let threads2 = some_min1(threads2)?;
        let threads3 = min1(total - threads1 - threads2);
        anyhow::Ok((threads1, threads2, threads3))
    } else {
        let total = std::thread::available_parallelism();
        if let Ok(total) = total {
            let threads1 = some_min1(threads1)?;
            let threads2 = some_min1(threads2)?;
            let threads3 = min1(total.get() - threads1 - threads2);
            anyhow::Ok((threads1, threads2, threads3))
        } else {
            //println!("Could not autodetect the number of threads available. Will assume the minimum number, but it is better if you specify");

            let threads1 = some_min1(threads1)?;
            let threads2 = some_min1(threads2)?;
            let threads3 = some_min1(threads3)?;
            anyhow::Ok((threads1, threads2, threads3))
        }
    }
}

pub fn some_min1(t: Option<usize>) -> anyhow::Result<usize> {
    if let Some(t) = t {
        if t < 1 {
            anyhow::bail!("Cannot set number of threads to be negative")
        } else {
            anyhow::Ok(t)
        }
    } else {
        anyhow::Ok(1)
    }
}

pub fn min1(t: usize) -> usize {
    if t < 1 {
        println!("Thread count cannot be negative, so setting to 1");
        1
    } else {
        t
    }
}

pub fn determine_thread_counts_mapcell(
    total: Option<usize>,
    threads1: Option<usize>,   //readers
    threads2: Option<usize>,   //writers (in turn invoking mapcell)
    threads_mc: Option<usize>, //threads in the mapcell; so mc*threads2 is the actual worker thread count
    recommend_threads_mapcell: usize,
) -> anyhow::Result<(usize, usize, usize)> {
    if let Some(total) = total {
        let threads1 = some_min1(threads1)?;
        let threads2 = min1(total - threads1);

        let (threads2, threads_mc) =
            divide_mapcell_threads_writer_mapcell(threads2, threads_mc, recommend_threads_mapcell)?;

        anyhow::Ok((threads1, threads2, threads_mc))
    } else {
        let total = std::thread::available_parallelism();
        if let Ok(total) = total {
            let threads1 = some_min1(threads1)?;
            let threads2 = min1(total.get() - threads1);

            let (threads2, threads_mc) = divide_mapcell_threads_writer_mapcell(
                threads2,
                threads_mc,
                recommend_threads_mapcell,
            )?;

            anyhow::Ok((threads1, threads2, threads_mc))
        } else {
            //println!("Could not autodetect the number of threads available. Will assume the minimum number, but it is better if you specify");

            let threads1 = some_min1(threads1)?;
            let threads2 = some_min1(threads2)?;

            let (threads2, threads_mc) = divide_mapcell_threads_writer_mapcell(
                threads2,
                threads_mc,
                recommend_threads_mapcell,
            )?;

            anyhow::Ok((threads1, threads2, threads_mc))
        }
    }
}

fn divide_mapcell_threads_writer_mapcell(
    threads2: usize,           //writers (in turn invoking mapcell)
    threads_mc: Option<usize>, //threads in the mapcell; so mc*threads2 is the actual worker thread count
    recommend_threads_mapcell: usize,
) -> anyhow::Result<(usize, usize)> {
    if let Some(threads_mc) = threads_mc {
        anyhow::Ok((threads2, threads_mc))
    } else {
        //Currently this is just a simple algorithm;
        //if the mapcell script can be multithreaded, then we just run one writer, and the mapcell script gets all threads.
        //writing will be marginally slower but this way we will not process too many cells at a time,
        //and the process will not be bottlenecked by a single job

        if recommend_threads_mapcell > 1 {
            anyhow::Ok((1, threads2))
        } else {
            anyhow::Ok((threads2, 1))
        }
    }
}
