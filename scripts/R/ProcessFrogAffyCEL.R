
#The first argument is the directory to process
processDir = commandArgs( trailingOnly = TRUE)[1]
outputFile = commandArgs( trailingOnly = TRUE)[2]
mapFile = commandArgs( trailingOnly = TRUE)[3]

#the affy libraries are for working with CEL files
library("affy")
library("affyio")

#install laevis chip annotations (only needs to be done once per R install)
#source("http://bioconductor.org/biocLite.R")
#biocLite("xlaevis.db")

#load frog affy annotations
library("xlaevis.db")

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
    #ReadAffy loads the array data using the custom CDF
    Data = ReadAffy(filenames = pfiles)
    #expresso processes the data
    express = expresso(Data, normalize.method="quantiles", bgcorrect.method="rma",pmcorrect.method="pmonly",summary.method="medianpolish")
    #this line writes out the PCL file.  It will be named ENTREZG_$(level)_$(processDir).pcl
    write.exprs(express, file=paste(outputFile, level, "part", sep="."))
    fns = featureNames(express)
    emap = mget(fns, xlaevisENTREZID)
    sink(paste(mapFile, level, "part", sep="."))
    print(emap)
    sink()
}


