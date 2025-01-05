#!/bin/bash

## Trivial wrapper that just counts the number of lines in the input fastq


#Default settings
USE_THREADS=1



for i in "$@"; do
 case $i in
  --bascet-api)
  echo "bascet-mapcell-api 1.0" # Tell the API version this script conforms to
  exit 0
  ;;
  --expect-files)
  echo "*" # Tell which files should extracted from the input file. Can enable * to give them all. or "foo.txt,bar.txt"
  exit 0
  ;;
  --missing-file-mode)
  echo "skip" # Tell what to do if the expected files are not present
  exit 0
  ;;
  --compression-mode)
  echo "default" # Tell how to compress. options are default, uncompressed
  exit 0
  ;;
  --input-dir)
  INPUT_DIR="$2" # Directory with expected files from the cell
  shift # past argument=value
  shift
  ;;
  --output-dir)
  OUTPUT_DIR="$2"  # Where to store output to. this directory can be assumed to exist
  shift # past argument=value
  shift
  ;;
  --num-threads)
  USE_THREADS="$2" # How many threads to use
  shift # past argument=value
  shift
  ;;
  --default)
  DEFAULT=YES
  shift # past argument with no value
  ;;
  -*|--*)
  echo "Unknown option $i"  #not clear if we should keep?
  exit 1
  ;;
  *)
  ;;
 esac
done


######################## Execute script below #####################

if [ -z ${INPUT_DIR} ]; then 
echo "input directory is unset"; 
exit 1;
fi

if [ -z ${OUTPUT_DIR} ]; then 
echo "output directory is unset"; 
exit 1;
fi


echo "INPUT_DIR   = ${INPUT_DIR}"
echo "OUTPUT_DIR  = ${OUTPUT_DIR}"
echo "USE_THREADS  = ${USE_THREADS}"

#Can assume to be running in the output directory
#kraken2 -db /data/henlab/kraken/standard "${INPUT_DIR}/contig.fa" --threads 8 --report kraken_report --report-minimizer-data --minimum-hit-groups 3 > kraken.stdout
#--paired

### The last line must be "MAPCELL-OK".
echo "MAPCELL-OK"


# kraken2 --db /data/henlab/kraken/standard   seqs.fa
# kraken2 --paired --classified-out cseqs#.fq seqs_1.fq seqs_2.fq

# check https://genome-idx.s3.amazonaws.com/kraken/k2_standard_8gb_20200919.tar.gz
