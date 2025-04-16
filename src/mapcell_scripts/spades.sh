#!/bin/bash

## Run SPADES assembly


#Default settings
USE_THREADS=1



for i in "$@"; do
    case $i in
        --bascet-api)
        echo "bascet-mapcell-api 1.0" # Tell the API version this script conforms to
        exit 0
        ;;
        --expect-files)
        echo "r1.fq,r2.fq" # Tell which files should extracted from the input file. Can enable * to give them all. or "foo.txt,bar.txt"    ### TODO allow separation by whitespace
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
        --recommend-threads)
        echo "1" # Tell how many threads that is recommended by default
        exit 0
        ;;
        --preflight-check)
        if ! command -v spades.py 2>&1 >/dev/null
        then
          echo "spades.py could not be found"
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

echo "Running Spades"

echo "INPUT_DIR   = ${INPUT_DIR}"
echo "OUTPUT_DIR  = ${OUTPUT_DIR}"
echo "USE_THREADS  = ${USE_THREADS}"

#Can assume to be running in the output directory
spades.py -1 ${INPUT_DIR}/r1.fq -2 ${INPUT_DIR}/r2.fq --sc -o spades
mv spades/contigs.fasta contigs.fasta

### The last line must be "MAPCELL-OK".
echo "MAPCELL-OK"
