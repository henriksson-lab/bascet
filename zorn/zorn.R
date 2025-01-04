

################################################################################
################ Generics for running jobs #####################################
################################################################################



setGeneric(
  name = "RunJob",
  def = function(runner, jobname, withdata, cmd, arraysize) standardGeneric("RunJob")
)

setGeneric(
  name = "WaitForJob",
  def = function(job) standardGeneric("WaitForJob")
)

setGeneric(
  name = "CancelJob",
  def = function(job) standardGeneric("CancelJob")
)

setGeneric(
  name = "JobStatus",
  def = function(job) standardGeneric("JobStatus")
)

setGeneric(
  name = "JobLog",
  def = function(job) standardGeneric("JobLog")
)





### Figure out how many chards


setClass("BascetInstance", slots=list(
  bin="character"
  )
) 

BascetInstance <- function(bin){
  new(
    "BascetInstance",
    bin=bin
  )
}

#bascet_instance.default <- BascetInstance(bin="bascet")
bascet_instance.default <- BascetInstance(bin="/home/mahogny/jupyter/bascet/target/debug/robert")

################################################################################
################ Things for running SLURM ######################################
################################################################################


setClass("SlurmInstance", slots=list(
  settings="character", 
  ncpu="character", 
  partition="character", 
  account="character", 
  time="character",
  prepend="character",
  mem="character"
  )
) 



setClass("SlurmJob", slots=list(
  pid="character", 
  cmd="character", 
  logfile="character",
  arraysize="numeric"
  )
) 



SlurmInstance <- function(settings=NULL, ncpu=NULL, partition=NULL, account=NULL, time=NULL, prepend=NULL, mem=NULL){
  
  ## Create a new default
  if(is.null(settings)){
    settings <- new(
      "SlurmInstance", 
      ncpu="1", 
      partition="",  #or NULL?? 
      account="",  #or NULL??
      time="0-72:00:00",
      prepend="",
      mem=""
    )
  }
  
  ##add new stuff
  if(!is.null(ncpu)){
    settings@ncpu <- ncpu
  }

  if(!is.null(partition)){
    settings@partition <- partition
  }

  if(!is.null(account)){
    settings@account <- account
  }

  if(!is.null(time)){
    settings@time <- time
  }

  if(!is.null(prepend)){
    settings@prepend <- prepend
  }

  if(!is.null(mem)){
    settings@mem <- mem
    #mem can have "g" at end. need to parse to integer  TODO
  }
  
  settings
}




setMethod(
  f = "RunJob",
  signature ="SlurmInstance",
  definition = function(runner, jobname, withdata, cmd, arraysize) {
    
    print("Running job with slurm")
    
    ## Set up SBATCH settings
    scriptcontent <- c("#!/usr/bin/env bash")
    if(runner@account!=""){
      scriptcontent <- c(scriptcontent, paste("#SBATCH -A",runner@account))
    }
    if(runner@ncpu!=""){
      scriptcontent <- c(scriptcontent, paste("#SBATCH -n",runner@ncpu))
    }
    if(runner@time!=""){
      scriptcontent <- c(scriptcontent, paste("#SBATCH -t",runner@time))
    }
    if(runner@partition!=""){
      scriptcontent <- c(scriptcontent, paste("#SBATCH -p",runner@partition))
    }
    if(runner@mem!=""){
      scriptcontent <- c(scriptcontent, paste("#SBATCH --mem",runner@mem))
    }
    scriptcontent <- c(scriptcontent, paste("#SBATCH -J ",jobname))

    ## Add the data needed by the command
    scriptcontent <- c(scriptcontent, withdata)

    ## Add the command
    this_cmd <- stringr::str_replace_all(cmd,stringr::fixed("$TASK_ID"),"$SLURM_ARRAY_TASK_ID")
    scriptcontent <- c(scriptcontent, this_cmd)
    
    ## Write the script to a temporary file
    slurm_script <- tempfile(fileext = ".sh")
    print(slurm_script)    
    writeLines(con=slurm_script, c(
      "#!/bin/bash",
      scriptcontent)
    )
    print(scriptcontent)

    ## Run the script; catch the message from sbatch
    cmd <- paste("sbatch ",slurm_script)
    ret <- system(cmd, intern = TRUE)
    print(ret)
    
    ## Remove the temporary file. Worst case, done at the end of the R session, but better done earlier
    file.remove(slurm_script)
    
    
    ## Check if it worked, or if there was an error
    if(stringr::str_starts(ret, "Submitted batch job ")){
      pid <- stringr::str_remove(ret, stringr::fixed("Submitted batch job "))
      
      #Return the job with PID set  
      job <- new(
        "SlurmJob",
        pid=pid,
        cmd=cmd, 
        logfile="todo",
        arraysize=arraysize
        )
    } else {
      stop("Failed to start job")
      NULL
    }
  }
)





#Has possibility of ctrl+c; just keeps polling, possibly with a status indicator from log. or keep plotting log file
setMethod(
  f = "WaitForJob",
  signature ="SlurmJob",
  definition = function(job) {
    # sacct -j JOBID -o jobid,submit,start,end,state
    while(TRUE) {
      info <- JobStatus(job)
      if(info$status=="RUNNING" || info$status=="PENDING"){
        print(info$status)
        Sys.sleep(5)
      } else {
        break;
      }
    }
    print(info$status)    
  }
)



setMethod(
  f = "CancelJob",
  signature ="SlurmJob",
  definition = function(job) {
    #First check if it is running at all
    
    system(paste("scancel", job@pid))
  }
)




setMethod(
  f = "JobStatus",
  signature ="SlurmJob",
  definition = function(job) {
    # sacct -j JOBID -o jobid,submit,start,end,state
    
    #ret <- system(paste("sacct -j",job@pid, "-o jobid,submit,start,end,state"), intern = TRUE)
    #print(ret)
    ret <- system(paste("sacct -j",job@pid, "-o state"), intern = TRUE)[3]
    status <- stringr::str_remove_all(ret, " ")  
    data.frame(status=status, chard=1)
    
    
    ## not enough! jobarray
  }
)


setMethod(
  f = "JobLog",
  signature ="SlurmJob",
  definition = function(job) {
    #read this file
    
    job@logfile
  }
)



####### testing
if(FALSE){
  
  inst <- SlurmInstance(partition="shared", account = "naiss2024-22-1647", ncpu = "10")
  job <- RunJob(inst,"ls $JOBID",1)
  
  JobStatus(job)
  CancelJob(job)
  
  
  
  ret <- system(paste("sacct -j",job@pid, "-o jobid,submit,start,end,state"), intern = TRUE)
  ret <- system(paste("sacct -j",job@pid, "-o jobid,state"), intern = TRUE)[3]
  ret <- system(paste("sacct -j",job@pid, "-o state"), intern = TRUE)[3]
  status <- stringr::str_remove_all(ret, " ")  
  #first line after header
  ret[3]
}



################################################################################
################ Things for running locally ####################################
################################################################################


library(processx)

setClass("LocalInstance", slots=list(
  maxcpu="character"
  )
) 


setClass("LocalJob", slots=list(
  cmd="character",
  proc="ANY", #processx:process
  logfile="character",
  arraysize="numeric"
  )
) 

##### 
# Create new local runner instance
LocalInstance <- function(maxcpu="10"){
  new("LocalInstance", maxcpu=maxcpu)
}




setMethod(
  f = "RunJob",
  signature ="LocalInstance",
  definition = function(runner, jobname, withdata, cmd, arraysize) {
    
    print("Starting local job")

    all_cmd <- c()
    for(i in seq_len(arraysize)){
      
      ### todo: not enough! need to take withdata into account
      
      this_cmd <- stringr::str_replace_all(cmd,stringr::fixed("$TASK_ID"),i)
      onep <- processx::process$new(this_cmd)
      all_cmd <- c(all_cmd,this_cmd)
    }
    
    
    tfile <- tempfile(pattern = jobname, fileext=".sh") #putting jobname here helps it show up in "ps"; but may cause issues if bad jobname given
    writeLines(con=tfile,c(
      "#!/bin/bash",
      all_cmd
    ))
    
    
    job <- new(
      "LocalJob",
      cmd=cmd,
      proc=processx::process$new("bash",tfile),
      logfile="foo",
      arraysize=arraysize
    )
    
    Sys.sleep(1)
    file.remove(tfile) #assumes process has started. can we do better?
    
    #Return the job with PIDs set  
    job
  }
)





setMethod(
  f = "CancelJob",
  signature ="LocalJob",
  definition = function(job) {
    if(job@proc$is_alive()){
      job@pid$kill()
    }
  }
)




setMethod(
  f = "JobStatus",
  signature ="LocalJob",
  definition = function(job) {
    if(job@proc$is_alive()){
      data.frame(status="RUNNING")
    } else {
      data.frame(status="DONE")  ### if killed, keep track and set cancelled
    }
  }
)


setMethod(
  f = "JobLog",
  signature ="LocalJob",
  definition = function(object) {
    #read this file
    
    #job@logfile
  }
)






############ Testing
if(FALSE){
  
  inst <- LocalInstance()
  
  thejob <- RunJob(LocalInstance(),"ls",1)
  
  CancelJob(thejob)
  
  
  JobStatus(thejob)
  
  
}




################################################################################
################ helper functions ##############################################
################################################################################


#' Helper function: Figure out which shards belong together given root input name and extension
#' i.e. root/name.##.ext
detect_shards_for_file <- function(bascetRoot, inputName, ext){
  allfiles <- list.files(bascetRoot)
  allfiles[stringr::str_detect(allfiles, paste0(inputName,"\\.[0123456789]+\\.",ext))]
}

#detect_shards_for_file("~/jupyter/zorn/test","fakein","zip")

#' Helper function: Generate suitable output filenames according to shard system
#' i.e. root/name.##.ext
make_output_shard_names <- function(bascetRoot, outputName, ext, num_shards){
  file.path(bascetRoot, paste0(outputName,".",seq_len(num_shards),".",ext))
}

#make_output_shard_names("/","bar","zip", 5)

###
#' Helper function: Create array of values in bash scripts
# example: myArray=("cat" "dog" "mouse" "frog")
make_bash_array <- function(variable, vals){
  paste0(
    variable,
    "=(",
    stringr::str_flatten(sprintf("\"%s\"",vals), collapse = " "),
    ")")  
}

#cat(make_bash_array("files_r2",c("a","b")))



################################################################################
################ Bascet command line tool: debarcoding raw fastq ###############
################################################################################



###############################################
#' Detect metadata for raw input FASTQ files
#' 
#' @param rawRoot Path to folder with FASTQ files
#' @returns A data frame with metadata for the raw input files
#' @examples
#' DetectRawFileMeta("/path/to/raw_fastq", verbose = TRUE)
DetectRawFileMeta <- function(rawRoot, verbose=FALSE){
  #rawRoot <- "/husky/fromsequencer/241206_novaseq_wgs3/raw"
  allfiles <- list.files(rawRoot)
  
  if(verbose){
    print("Found the following files in the directory")
    print(allfiles)
  }
  
  allfiles <- allfiles[
    stringr::str_ends(allfiles,".fq.gz") | 
    stringr::str_ends(allfiles, ".fastq.gz") | 
    stringr::str_ends(allfiles, ".fq") | 
    stringr::str_ends(allfiles,".fastq")]
  
  if(verbose){
    print("Found the following raw read-like files")
    print(allfiles)
  }

  r1_files <- allfiles[stringr::str_detect(allfiles,stringr::fixed("_R1"))]
  
  if(verbose){
    print("Found the following R1-like files")
    print(r1_files)
  }
  
  r2_corresponding <- stringr::str_replace(r1_files,stringr::fixed("R1"),"R2")
  
  if(!all(r2_corresponding %in% allfiles)){
    stop("Not all R1 files appear to have a corresponding R2 file")
  }
  
  ### Gather first set of metadata
  meta <- data.frame(
    r1=r1_files,
    r2=r2_corresponding,
    dir=file.path(rawRoot) #this standarizes the trailing /
  )
  meta$prefix <- ""

  #Guess prefixes
  meta$possible_prefix <- stringr::str_split_i(meta$r1, "_S[0123456789]",1)
  unique_prefix <- unique(meta$possible_prefix)
  if(verbose){
    print("Detected possible prefixes")
    print(unique_prefix)
  }
  
  #If there is more than one prefix, then we have to add them. otherwise just keep it simple
  if(length(unique_prefix)>1){
    meta$prefix <- paste0(meta$possible_prefix,"_")
  } else {
    if(verbose){
      print("Detected one one possible prefix, so not adding it")
    }
  }

  #Sanitize prefixes. Some characters will break BAM tags etc
  meta$prefix <- stringr::str_remove_all(meta$prefix, " ")
  meta$prefix <- stringr::str_remove_all(meta$prefix, "/")
  meta$prefix <- stringr::str_remove_all(meta$prefix, "\"")
  
  #Return metadata
  meta[,c("prefix","r1","r2","dir")]
}

#rawmeta <- DetectRawFileMeta("/husky/fromsequencer/241206_novaseq_wgs3/raw")



###############################################
#' Generate BAM with barcodes from input raw FASTQ
BascetGetRawAtrandiWGS <- function(bascetRoot, rawmeta, outname="debarcoded", runner, bascet_instance=bascet_instance.default){
  
  RunJob(
    runner = runner, 
    jobname = "bascet_getraw",
    withdata = c(
      make_bash_array("files_r1",file.path(rawmeta$dir, rawmeta$r1)),
      make_bash_array("files_r2",file.path(rawmeta$dir, rawmeta$r2))
    ),
    cmd = paste(bascet_instance@bin, "getraw --r1 files_r1[$TASK_ID] --r2 files_r1[$TASK_ID] -o ", file.path(bascetRoot, paste0(outName,".$TASK_ID"))),
    arraysize = nrow(rawmeta)
  )
}




################################################################################
################ Bascet command line tools: RNA-seq ############################
################################################################################


###############################################
#' Aligned debarcoded BAMs
#'  #sort or not here?
BascetAlign <- function(bascetRoot, genomeReference, inputName="debarcoded", outputName="unsorted_aligned", runner, bascet_instance=bascet_instance.default){ 

  #Figure out input and output file names  
  inputFiles <- detect_shards_for_file(bascetRoot, inputName, "bam")
  num_shards <- length(inputFiles)
  outputFiles <- make_output_shard_names(bascetRoot, outputName, "bam", num_shards)
  
  #Run the job
  RunJob(
    runner = runner, 
    jobname = "bascet_align",
    withdata = c(
      make_bash_array("files_in",inputFiles),
      make_bash_array("files_out",outputFiles)
    ),
    cmd = paste(bascet_instance@bin, "align --in files_in[$TASK_ID] --o files_out[$TASK_ID] --reference ", genomeReference),
    arraysize = num_shards
  )
  
  
}


################################################################################
################ Bascet command line tools: WGS ################################
################################################################################





###############################################
#' Generate zip file with fastq from each cell
# this command should be able to take multiple debarcoded files as input
BascetPartition <- function(bascetRoot, inputName="debarcoded", outputName="rawreads", runner, bascet_instance=bascet_instance.default){

  #Figure out input and output file names  
  inputFiles <- detect_shards_for_file(bascetRoot, inputName, "zip")
  num_shards <- length(inputFiles)
  outputFiles <- make_output_shard_names(bascetRoot, outputName, "zip", num_shards)

  #Run the job
  RunJob(
    runner = runner, 
    jobname = "bascet_partition",
    withdata = c(
      make_bash_array("files_in",inputFiles),
      make_bash_array("files_out",outputFiles)
    ),
    cmd = paste(bascet_instance@bin, "getraw --in files_in[$TASK_ID] --o files_out[$TASK_ID]"),
    arraysize = num_shards
  )

}


###############################################
#' Run Spades to assemble the genomes
BascetAssemble <- function(bascetRoot, inputName="rawreads", outputName="assembled", runner, bascet_instance=bascet_instance.default){
  
  #Figure out input and output file names  
  inputFiles <- detect_shards_for_file(bascetRoot, inputName, "zip")
  num_shards <- length(inputFiles)
  outputFiles <- make_output_shard_names(bascetRoot, outputName, "zip", num_shards)
  
  #Run the job
  RunJob(
    runner = runner, 
    jobname = "bascet_asm",
    withdata = c(
      make_bash_array("files_in",inputFiles),
      make_bash_array("files_out",outputFiles)
    ),
    cmd = paste("bascet assemble --in files_in[$TASK_ID] --o files_out[$TASK_ID]"),
    arraysize = num_shards
  )
  
}


###############################################
#' Build kmer database
BascetCount <- function(bascetRoot, inputName="assembled", outputName="kmers", runner, bascet_instance=bascet_instance.default){
  
  
  #Figure out input and output file names  
  inputFiles <- detect_shards_for_file(bascetRoot, inputName, "zip")
  num_shards <- length(inputFiles)
  outputFiles <- make_output_shard_names(bascetRoot, outputName, "zip", num_shards)
  
  #Run the job
  RunJob(
    runner = runner, 
    jobname = "bascet_kmc",
    withdata = c(
      make_bash_array("files_in",inputFiles),
      make_bash_array("files_out",outputFiles)
    ),
    cmd = paste("bascet count --in files_in[$TASK_ID] --o files_out[$TASK_ID]"),
    arraysize = num_shards
  )
  
}



###############################################
#' Select kmers that appear useful for clustering
BascetFeaturise <- function(bascetRoot, inputName="kmers", outputName="features", runner, bascet_instance=bascet_instance.default){

  #Figure out input and output file names  
  inputFiles <- detect_shards_for_file(bascetRoot, inputName, "zip")
  num_shards <- length(inputFiles)
  outputFiles <- make_output_shard_names(bascetRoot, outputName, "zip", num_shards)
  
  #Run the job
  RunJob(
    runner = runner, 
    jobname = "bascet_featurise",
    withdata = c(
      make_bash_array("files_in",inputFiles),
      make_bash_array("files_out",outputFiles)
    ),
    cmd = paste("bascet featurise --in files_in[$TASK_ID] --o files_out[$TASK_ID]"),
    arraysize = num_shards
  )
  
}


###############################################
#' Build count table from kmer table and selected kmers
BascetQuery <- function(bascetRoot, inputKMER="kmers", inputFeatures="features", outputName="query", runner, bascet_instance=bascet_instance.default){

  #Figure out input and output file names  
  inputKMER <- detect_shards_for_file(bascetRoot, inputKMER, "zip")
  num_shards <- length(inputKMER)
  outputFiles <- make_output_shard_names(bascetRoot, outputName, "zip", num_shards)
  
  #Note: Need to give all input files for each job
  inputFeatures_comma <- stringr::str_flatten(inputFeatures, collapse = ",")

  #Run the job
  RunJob(
    runner = runner, 
    jobname = "bascet_query",
    withdata = c(
      make_bash_array("files_kmer",inputKMER),
      make_bash_array("files_out",outputFiles)
    ),
    cmd = paste(bascet_instance@bin, "query --features ",inputFiles_comma," --kmers files_kmer[$TASK_ID] --o files_out[$TASK_ID]"), 
    arraysize = num_shards
  )

}


################################################################################
################ Bascet command line tools: isolate genomes ####################
################################################################################


###############################################
####### Add raw FASTQs of isolates, enabling them to be treated as cells, assembled etc
# todo think about what to name this file. maybe separate from cell fastq to enable easy rerunning
# todo allow this function to be called multiple times?
BascetAddIsolateRawFastq <- function(bascetRoot, listFastqR1, listFastqR2, names, runner, bascet_instance=bascet_instance.default){
  
  
}




###############################################
####### Add isolate genomes, treating them as assembled, enabling clustering, comparison with cells, etc
# todo think about what to name this file. maybe separate from cell assembly to enable easy rerunning
# todo allow this function to be called multiple times?
BascetAddAssembledIsolate <- function(bascetRoot, listFasta, names, runner, bascet_instance=bascet_instance.default){
  
  
}




################################################################################
################ The map system ################################################
################################################################################



###############################################
#' Call a function for all cells
BascetMapCell <- function(bascetRoot, withfunction, inputName, outputName, runner, bascet_instance=bascet_instance.default){
  
  #Figure out input and output file names  
  inputFiles <- detect_shards_for_file(bascetRoot, inputName, "zip")
  num_shards <- length(inputFiles)
  outputFiles <- make_output_shard_names(bascetRoot, outputName, "zip", num_shards)
  
  #Run the job
  RunJob(
    runner = runner, 
    jobname = paste0("bascet_map_",withfunction),
    withdata = c(
      make_bash_array("files_in",inputFiles),
      make_bash_array("files_out",outputFiles)
    ),
    cmd = paste(bascet_instance@bin, "map --in files_in[$TASK_ID] --o files_out[$TASK_ID] -f ", withfunction),
    arraysize = num_shards
  )  
}
  
  
  

###############################################
#' Convenience function; alternative is to somehow implement as.data.frame
MapListAsDataFrame <- function(mylist){
  out <- do.call(rbind, mylist)
  rownames(out) <- names(mylist)
  out
}




###############################################
#' Aggregate data from previous Map call
#' 
#' todo: allow multi-cpu support? parallel library
#
#' todo note, this is effectively a pure-R map function. different name?
BascetAggregateMap <- function(bascetRoot, bascetName, aggrFunction){

  #Get file coordinates of all objects in zip file
  cellname_coord <- BascetCellNames(bascetRoot, inputName)
  
  #Open the file, prep for reading
  bascet_file <- OpenBascet(bascetRoot, bascetName)
    
  #Loop over all files in the bascet
  output <- list()
  for(cellname in cellname_coord_inshard$cell){
    output[[cellname]] <- aggrFunction(bascet_file, cellname)
  }
  output
}




#tmp <- BascetReadMapFile(bascetFile, cellID, "out.csv", as="tempfile")





################################################################################
################ Reading of Bascet files #######################################
################################################################################


#' A bascet, along with all the shards
setClass("Bascet", slots=list(
  num_shards="numeric",
  files="character",
  cellmeta="ANY"
  )
) 


###############################################
#' Get list of cells in a bascet
BascetCellNames <- function(bascetRoot, bascetName){
  
  #Can support BAM later as well
  ext="zip"
  shards <- detect_shards_for_file(bascetRoot,bascetName,ext)
  
  cellnames <- list()
  for(i in seq_along(shards)){
    allfiles <- unzip(file.path(bascetRoot,shards[i]),list = TRUE)$Name
    df <- data.frame(cell=unique(stringr::str_split_i(allfiles,"/",1)))   
    df$shard <- i - 1
    cellnames[[i]] <- df
  }
  do.call(rbind, cellnames)
}


if(FALSE){
  BascetCellNames("/home/mahogny/jupyter/zorn/test/data", "shard")
}



#' Open a Bascet, prepare it for reading.
#' 
#' The current code is based on pure R, but more efficient calls can be made
#' in the future. We thus advise against direct zip-file manipulation and
#' do not guarantee future support for this
OpenBascet <- function(bascetRoot, bascetName){
  
  shards <- detect_shards_for_file(bascetRoot,bascetName,"zip")
  num_shards <- length(shards)
  cellname_coord <- BascetCellNames(bascetRoot, bascetName)

  new("Bascet", 
      num_shards=num_shards, 
      files=file.path(bascetRoot, shards), 
      cellmeta=BascetCellNames(bascetRoot, bascetName))
}




#' Read one file from a Bascet
#' 
#' This can be made faster by, e.g., once and for all reading the location of
#' all objects in the file
#' 
BascetReadFile <- function(bascetFile, cellID, filename, as=c("tempfile"), verbose=FALSE){

  ## Check if the cell is present at all
  cellmeta <- bascetFile@cellmeta[bascetFile@cellmeta$cell==cellID,,drop=FALSE]
  if(nrow(cellmeta)==0){
    stop("Cell not present in file")
  }
  print(cellmeta)

  if(as=="pipe"){
    
    #bascet pipe <<<file  <<<cellname
    
    
    
  } else if(as=="tempfile"){

    #Need a directory to unzip to
    tname.dir <- tempfile()
    dir.create(tname.dir)

    #Extract this zip file and then check that it worked
    name_of_zip <- bascetFile@files[cellmeta$shard+1]
    extract_files <- file.path(cellID,filename)
    
    if(verbose){
      print(name_of_zip)
      print(extract_files)
      print(tname.dir)
    }

    unzip(name_of_zip, files=extract_files, exdir=tname.dir)
    name_of_outfile <- file.path(tname.dir, cellID, filename) ## should be checked
    if(!file.exists(name_of_outfile)){
      stop(paste("unzip failed to produce expected ",name_of_outfile))
    }

    #Move the output file to a new temp file, such that the callback function need not delete whole directory
    tname.out <- tempfile()
    file.rename(name_of_outfile, tname.out)

    #Recursive delete. to be safe, remove expected files
    file.remove(file.path(tname.dir, cellID))
    file.remove(tname.dir)
    
    #Return temp file location
    return(tname.out)
  } else {
    stop("Unsupported output format")
  }
}



if(FALSE){
  
  #tmp <- BascetReadFile(bascetFile, cellID, "out.csv", as="tempfile")  from aggr function
  
  
  one_bascet <- OpenBascet("/home/mahogny/jupyter/zorn/test/data","shard")
  thefile <- BascetReadFile(one_bascet, "0-1-2-3", "out.txt")
  thefile
  readLines(thefile)
}







