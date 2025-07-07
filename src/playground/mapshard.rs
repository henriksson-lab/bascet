use unix_named_pipe;

use std::path::PathBuf;

// Play ground for using named pipes

pub fn test(p: PathBuf) {
    unix_named_pipe::create(&p, None).expect("could not create fifo");

    // https://en.wikipedia.org/wiki/Named_pipe

    // mkfifo pipe

    /*

    //Pipe appears to reach end once writing process is done.
    //Reader and writer can be attached in any order

    Order is thus:

    1. Create the input pipes (R1, R2) and output (BAM) if we want to convert on-the-fly

    2. start BWA taking input, giving output

    3. start writer thread

    4. start reader thread

    5. wait for writer to finish
    6. wait for bwa to finish
    7. optional: wait for reader to finish

     */

    /*


    we can use pipes for some



     */
}
