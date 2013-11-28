#!/usr/local/bin/Rscript
library(limma)
#input file (targets)
targetsFile = commandArgs(trailingOnly = TRUE)[1]
#output file
outputFile = commandArgs(trailingOnly = TRUE)[2]
#column names
colFile = commandArgs(trailingOnly = TRUE)[3]

targets<-readTargets(targetsFile)
RG<-read.maimages(targets,  source="agilent.median", annotation=NULL, green.only=TRUE)
RG <- backgroundCorrect(RG, method="normexp", offset=16)
MA <- normalizeBetweenArrays(RG, method="quantile")
MA.avg <- avereps(MA, ID=MA$genes$ProbeName)
#change column names
cols = scan(colFile,what="character")
colnames(MA.avg) = cols
write.table(MA.avg$E, file=outputFile, sep="\t", quote=FALSE)

