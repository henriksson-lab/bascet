#!/bin/bash

## Run ARIBA on reads per cell

# https://github.com/sanger-pathogens/ariba

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
        --recommend-threads)
        echo "1" # Tell how many threads that is recommended by default
        exit 0
        ;;
        --preflight-check)
        if ! command -v ariba 2>&1 >/dev/null
        then
        echo "ariba could not be found"
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

if [ -z ${DATABASE_DIR} ]; then 
    echo "database directory is unset"; 
    exit 1;
fi

echo "Running ARIBA"

echo "INPUT_DIR   = ${INPUT_DIR}"
echo "OUTPUT_DIR  = ${OUTPUT_DIR}"
echo "USE_THREADS  = ${USE_THREADS}"

#Can assume to be running in the output directory
# LC's note: will have to run additional commands to prep DB
# users can also select multiple DBs
# makes sense to do this before they run, rather than per cell
# ariba getref ncbi out.ncbi
# ariba prepareref -f out.ncbi.fa -m out.ncbi.tsv out.ncbi.prepareref
ariba run \
    --threads ${USE_THREADS} \
    ${DATABASE_DIR} \
    ${INPUT_DIR}/r1.fq ${INPUT_DIR}/r2.fq \
    ./out.run
# --tmp_dir   # todo


### The last line must be "MAPCELL-OK".
echo "MAPCELL-OK"


# ariba getref ncbi out.ncbi
# ariba prepareref -f out.ncbi.fa -m out.ncbi.tsv out.ncbi.prepareref
# out.ncbi.prepareref is the DATABASE_DIR