#!/bin/bash

#Default settings
USE_THREADS=1



for i in "$@"; do
 case $i in
  --bascet-api)
  echo "bascet-mapcell-api 1.0" # Tell the API version this script conforms to
  exit 0
  ;;
  --expect-files)
  echo "r1.fq,r2.fq" # Tell which files should extracted from the input file. Can enable * to give them all. or "foo.txt,bar.txt"
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

  --preflight-check)
  if ! command -v kmc 2>&1 >/dev/null
  then
    echo "kmc could not be found"
    exit 1
  fi
  echo "MAPCELL-CHECK"
  exit 0
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

echo "Building a KMC3 database from reads"

echo "INPUT_DIR   = ${INPUT_DIR}"
echo "OUTPUT_DIR  = ${OUTPUT_DIR}"
echo "USE_THREADS  = ${USE_THREADS}"

#Can assume to be running in the output directory

KMC_TEMP=.
INPUT_FILE_NAME_1=${INPUT_DIR}/r1.fq
INPUT_FILE_NAME_2=${INPUT_DIR}/r2.fq
#OUTPUT_DB=kmc
#OUTPUT_DUMP=kmc_dump.txt


## TODO support other parameters

#Need to provide list of input files, as a file
echo $INPUT_FILE_NAME_1 >  inlist.txt
echo $INPUT_FILE_NAME_2 >> inlist.txt

#Run KMC on FASTQ input. Note that KMC cannot handle $vars as arguments for some reason!!
kmc -cs2000000000  -k31 -ci=1 -fq  @inlist.txt  kmc .

#Remove list of files
rm inlist.txt

#kmc [options] <input_file_name> <output_file_name> <working_directory>
#-f<a/q/m/bam/kmc> - input in FASTA format (-fa), FASTQ format (-fq), multi FASTA (-fm) or BAM (-fbam) or KMC(-fkmc); default: FASTQ
#-k<len>           - k-mer length (k from 1 to 256; default: 25)
#-cs<value>        - maximal value of a counter (default: 255)                               u32::MAX-1 = 4_294_967_294   has been used
#-ci<value>        - exclude k-mers occurring less than <value> times (default: 2)

### To enable reading from the database, dump as txt  (in the future, possible to )
#kmc_tools transform $OUTPUT_DB dump $OUTPUT_DUMP
kmc_tools transform kmc dump kmc_dump.txt


### The last line must be "MAPCELL-OK".
echo "MAPCELL-OK"

