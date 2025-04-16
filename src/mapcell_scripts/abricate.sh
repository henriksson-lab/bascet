#!/bin/bash

## Run ABRicate for BLAST-based annotation

#https://github.com/tseemann/abricate

#Default settings
USE_THREADS=1

for i in "$@"; do
    case $i in
        --bascet-api)
        echo "bascet-mapcell-api 1.0" # Tell the API version this script conforms to
        exit 0
        ;;
        --expect-files)
        echo "contigs.fa" # Tell which files should extracted from the input file. Can enable * to give them all. or "foo.txt,bar.txt"
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
        if ! command -v abricate 2>&1 >/dev/null
        then
            echo "abricate could not be found"
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

if [ -z ${MINID} ]; then 
    echo "using default minid=80"; 
    MINID=80
fi

if [ -z ${MINCOV} ]; then 
    echo "using default mincov=80"; 
    MINCOV=80
fi


echo "Running ABRicate"

echo "INPUT_DIR   = ${INPUT_DIR}"
echo "OUTPUT_DIR  = ${OUTPUT_DIR}"
echo "USE_THREADS  = ${USE_THREADS}"

#Can assume to be running in the output directory
# LC's note: will need to specify db, ID, coverage
abricate --threads ${USE_THREADS} \
    --db ${DATABASE_DIR} \
    --noheader \
    --nopath \
    --minid ${MINID} \
    --mincov ${MINCOV} \
    ${INPUT_DIR}/contigs.fa > ./abricate.tsv

### The last line must be "MAPCELL-OK".
echo "MAPCELL-OK"