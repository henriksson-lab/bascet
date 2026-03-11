######### Option #1
#TODO need we even multiple writers if we do it this way?
our_rust_debarcoder > some_unsorted.pregascet.0
sort --temporary-directory=dir   some_unsorted.pregascet.0 some_unsorted.pregascet.1 ... | bgzip -c /dev/stdin > test.gascet.0.gz
tabix -p bed test.gascet.0.gz

######### Option #2
#TODO need we even multiple writers if we do it this way? linux sorts starts right away, does not attempt to fill memory!!!
our_rust_debarcoder | sort --temporary-directory=dir > some_sorted.pregascet.0
sort --merge  some_sorted.pregascet.0 some_sorted.pregascet.1 ... | bgzip -c /dev/stdin > test.gascet.0.gz
tabix -p bed test.gascet.0.gz


#if sort is clever and starts sorting right away then it can get more done while debarcoding is still ongoing.
#this needs testing!!



###########################

# assume as input:
# 3400M reads  . 5M genomes. then 680 genomes for 1x coverage
# if we take 10M reads at a time

#table(round(runif(10000000,min=1, max=680)))
#more or less equal distribution. 500 reads per cell
#makes sense to directory store in a format with blocking per cell

#tabix still makes sense for later separate use, as it is easy to pull out reads for each cell

#it seems that we can pipe data to bgzip from rust to generate the index. 


# How much memory needed for 10M reads? 150bp*2 reads.  
# 10e6*(150*2*2)/1e6 = 6000 = 6 GB
# 1M reads at a time => 50 reads per cell

# benefit from a parallel sorter
#https://docs.rs/rayon/latest/rayon/slice/trait.ParallelSliceMut.html#method.par_sort_unstable     better than anything we could make
#https://docs.rs/rayon/latest/rayon/
#https://docs.rs/rayon/latest/rayon/struct.ThreadPoolBuilder.html#method.build_global   set number of rayon threads

#https://blog.logrocket.com/implementing-data-parallelism-rayon-rust/   should we use this in general?


############################## 
# sort -o out.foo --merge  --temporary-directory=dir ...list of files ...
# todo: can also pipe to bgzip right away. if no -o, goes to stdout
#     -m, --merge
#             Merge only.  The input files are assumed to be pre-sorted.  If they are not sorted the output order is undefined.
# with this, makes sense to write uncompressed files first; or about 3000 temp files

# if we write our own merging algo, we can store presorted BLOCKS in a few files. 


################################

#tabix file format https://samtools.github.io/hts-specs/tabix.pdf
#tabix paper https://pmc.ncbi.nlm.nih.gov/articles/PMC3042176/

https://en.wikipedia.org/wiki/BED_(file_format)


Possible to have a header!

#name   from    to      umi     r1      r2      q1      q2
foo     1       1       apa     rrr     bbb     aaaa    dsfff

but if we skip header then easier to use sort utils from command line



sort ---- whatever
bgzip test.bed 
zcat test.bed.gz 
tabix -p bed test.bed.gz 


### for query, chr:beginPos-endPos   ..... so - is not valid in barcode names!!


########################## can pull out header
tabix -H test.bed.gz
#name	from	to	umi	r1	r2	q1	q2



########################## can pull out list of cells
mahogny@beagle:~/github/bascet/test$ tabix -l test.bed.gz
foo

########################## get reads for one particular cell
tabix  test.bed.gz foo


##########################
https://docs.rs/noodles-tabix/latest/noodles_tabix/


##########################  better!!
https://docs.rs/rust-htslib/latest/rust_htslib/tbx/index.html   #only reading

https://docs.rs/rust-htslib/latest/rust_htslib/bgzf/struct.Writer.html    


######### piping to bgzip is tricky
https://github.com/samtools/htslib/issues/1726

# Now this is required.
$ echo test | bgzip -c /dev/stdin > test.gz



#### Index if needed
fragpath <- "/husky/fromsequencer/241210_joram_rnaseq/trimmed/atac_fragments.tsv.gz"
fragpath_index <- paste(fragpath,".tbi",sep="")
if(!file.exists(fragpath_index)){
  print("Indexing fragment file")
  system(paste("tabix -p vcf ",fragpath))
  #"tabix -p vcf fragments.tsv.gz"
  #"fragments.tsv.gz.tbi"
}


