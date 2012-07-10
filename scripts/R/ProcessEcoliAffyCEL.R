
#The first argument is the directory to process
processDir = commandArgs( trailingOnly = TRUE)[1]
outputFile = commandArgs( trailingOnly = TRUE)[2]
mapFile = commandArgs( trailingOnly = TRUE)[3]

#the affy libraries are for working with CEL files
library("affy")
library("affyio")

#install laevis chip annotations (only needs to be done once per R install)
#source("http://bioconductor.org/biocLite.R")
#biocLite("ecoli2.db")
#biocLite("ecoliK12.db0")
#biocLite("ecoliSakai.db0")
#biocLite("org.EcK12.eg.db")
#biocLite("org.EcSakai.eg.db")

#load ecoli
library("ecoli2.db")
# Necessary?
library("ecoliK12.db0")
# Necessary?
library("ecoliSakai.db0")
library("org.EcK12.eg.db")
library("org.EcSakai.eg.db")


#files now holds a list of all celfiles in the directory to be processed
files = list.celfiles(processDir)

#ptype is now a vector with the type of each array
ptype = sapply(files, function(f) read.celfile.header(paste(processDir, f, sep="/"))[1])

#ptype levels are the levels of that vector (i.e. the types of arrays present)
#this is important because only arrays of the same type can be processed together
ptype_levels = levels(as.factor(unlist(ptype)))

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
      x <- ReadAffy(filenames = pfiles[i])
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
    Data = ReadAffy(filenames = pfiles2)
    #expresso processes the data
    express = expresso(Data, normalize.method="quantiles", bgcorrect.method="rma",pmcorrect.method="pmonly",summary.method="medianpolish")
    #this line writes out the PCL file.  It will be named ENTREZG_$(level)_$(processDir).pcl
    write.exprs(express, file=paste(outputFile, level, "part", sep="."))
    fns = featureNames(express)
    ema = mget(fns, drosophila2ENTREZID)
    sink(paste(mapFile, level, "part", sep="."))
    print(emap)
    sink()
  }, error = function(e) {
    print(e)
    print(paste("Could not normalize platform", level))
    errorOccured = 1
  }, finally = {
    print(paste("Processed platform", level, "at", Sys.time()))
  })
}


