#!/usr/local/bin/Rscript
library(limma)

#input file (targets)
targetsFile = commandArgs(trailingOnly = TRUE)[1]
#output file
outputFile = commandArgs(trailingOnly = TRUE)[2]
#file with correct column names
colFile = commandArgs(trailingOnly = TRUE)[3]

targets<-readTargets(targetsFile)

RG<-read.maimages(targets, source="genepix", annotation = c("Row", "Col", "FeatureNum", "ControlType", "ID"))
RG <- backgroundCorrect(RG, method="normexp", offset=16)
MA <- normalizeWithinArrays(RG, method="loess")
MA <- normalizeBetweenArrays(MA, method="quantile")
MA.avg <- avereps(MA, ID=MA$genes$ID)
#change column names
cols = scan(colFile,what="character")
colnames(MA.avg) = cols
#write to output file
write.table(MA.avg$M, file=outputFile, sep="\t", quote=FALSE)



