#!/bin/bash

## Run FastQC on reads per cell


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
        if ! command -v fastqc 2>&1 >/dev/null
        then
            echo "fastqc could not be found"
            exit 1
        fi
        echo "MAPCELL-CHECK"
        exit 0
        ;;
        --recommend-threads)
        echo "1" # Tell how many threads that is recommended by default
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

echo "Running FastQC"

echo "INPUT_DIR   = ${INPUT_DIR}"
echo "OUTPUT_DIR  = ${OUTPUT_DIR}"
echo "USE_THREADS  = ${USE_THREADS}"

#Can assume to be running in the output directory
fastqc -o ./ --extract -t ${USE_THREADS} ${INPUT_DIR}/r1.fq ${INPUT_DIR}/r2.fq
#head ${INPUT_DIR}/contig.fa > firstpart.txt

#Only keep files of interest
unzip -c r1_fastqc.zip r1_fastqc/fastqc_data.txt > r1_fastqc_data.txt
unzip -c r2_fastqc.zip r2_fastqc/fastqc_data.txt > r2_fastqc_data.txt

unzip -c r1_fastqc.zip r1_fastqc/summary.txt > r1_summary.txt
unzip -c r2_fastqc.zip r2_fastqc/summary.txt > r2_summary.txt

#remove the zip files
rm r1_fastqc.zip
rm r2_fastqc.zip

### The last line must be "MAPCELL-OK".
echo "MAPCELL-OK"

# get 
# r1_fastqc.zip/fastqc_data.txt
# r1_fastqc.html


#unzip r1_fastqc.zip r1_fastqc/fastqc_data.txt 
