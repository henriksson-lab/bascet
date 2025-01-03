pub static RDB_WORK_DIR: &str = "out";

pub const RDB_PATH_INDEX_READS: &str = "rdb_reads.index";
pub const RDB_PATH_INDEX_CONTIGS: &str = "rdb_contigs.index";
pub const RDB_PATH_INDEX_KMC_DBS: &str = "rdb_kmc_dbs.index";
pub const RDB_PATH_INDEX_KMC_DUMPS: &str = "rdb_kmc_dumps.index";

pub const RDB_FILENAME_READS: &str = "reads.fastq";
//TODO:
pub const RDB_FILENAME_CONTIGS: &str = "constigs.fasta";
pub const RDB_FILENAME_KMC_PRE: &str = "reads.fastq";
pub const RDB_FILENAME_KMC_SUF: &str = "reads.fastq";

pub static DEFAULT_SEED_RANDOM: std::sync::LazyLock<u64> =
    std::sync::LazyLock::new(|| rand::random::<u64>());
