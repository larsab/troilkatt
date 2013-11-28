#The first argument is the directory to process
processDir = commandArgs( trailingOnly = TRUE)[1]
organismCode = commandArgs( trailingOnly = TRUE)[2]
outputFile = commandArgs( trailingOnly = TRUE)[3]

#the affy libraries are for working with CEL files
#
# To install these, exeucte:
#source("http://bioconductor.org/biocLite.R")
# biocLite("affy")
# biocLite("affyio")
library("affy")
library("affyio")

#files now holds a list of all celfiles in the directory to be processed
files = list.celfiles(processDir)

#ptype is now a vector with the type of each array
ptype = sapply(files, function(f) read.celfile.header(paste(processDir, f, sep="/"))[1])

#ptype levels are the levels of that vector (i.e. the types of arrays present)
#this is important because only arrays of the same type can be processed together
ptype_levels = levels(as.factor(unlist(ptype)))
print(ptype_levels)

#mapping holds the three major human platforms, the ReadAffy function used below
#will read the dataset using the CDF specified in here.  This should correspond
#to the brainarray CDFs for Entrez.  I was only using three human platforms before 
#so they were hand defined but this loop should map them.
mapping <- list()
for (level in ptype_levels) {
  striphyphen = gsub('-', '', level)
  stripped = gsub('_', '', striphyphen)
  stripped = sub("hugene10st.*", "hugene10st", stripped, ignore.case=T, fixed=F)
  stripped = sub("hugene11st.*", "hugene11st", stripped, ignore.case=T, fixed=F)
  stripped = sub("hugene20st.*", "hugene20st", stripped, ignore.case=T, fixed=F)
  stripped = sub("hugene21st.*", "hugene21st", stripped, ignore.case=T, fixed=F)
  stripped = sub("huex10st.*", "huex10stv2", stripped, ignore.case=T, fixed=F)
  mapping[[level]] <- paste(stripped, organismCode, 'ENTREZG', sep='_')
}

for (level in ptype_levels) {
  #pfiles vector only contains files of this type
  pfiles = paste(processDir, subset(files, ptype==level), sep="/")

  #
  # Verfiy that files can be read and that they are not corrupted
  #
  validFiles <- rep(NA, length(pfiles))
  validFilesCnt = 1
  for(i in 1:length(pfiles)) {
    tryCatch( {
      x <- ReadAffy(cdfname=mapping[[level]], filenames = pfiles[i])
      validFiles[validFilesCnt] <- c(pfiles[i])
      validFilesCnt = validFilesCnt + 1
    }, error = function(e) {
      print(e)
      print(paste("CEL file is corrupted: ", pfiles[i]))    
    }, finally = {
      print(paste("Checked file", files[i], "at", Sys.time()))
    })
  }
  pfiles2 <- rep(NA, validFilesCnt - 1)
  for (i in 1:validFilesCnt - 1) {
    pfiles2[i] <- c(validFiles[i])
  }

  #
  # Do normalization
  #
  tryCatch( {
    #ReadAffy loads the array data using the custom CDF
    Data = ReadAffy(cdfname=mapping[[level]], filenames = pfiles2)
    #expresso processes the data
    express = expresso(Data, normalize.method="quantiles", bgcorrect.method="rma",pmcorrect.method="pmonly",summary.method="medianpolish")
    #this line writes out the PCL file.  It will be named GSEXXX_RAW.platform.<level>.pcl
    write.exprs(express, file=paste(outputFile, "platform", level, "pcl", sep="."))
  }, error = function(e) {
    print(e)
    print(paste("Could not normalize platform", level))
    errorOccured = 1
  }, finally = {
    print(paste("Processed platform", level, "at", Sys.time()))
  })
}

