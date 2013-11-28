#!/usr/local/bin/Rscript
library(limma)
#input file (targets)
targetsFile = commandArgs(trailingOnly = TRUE)[1]
#output file
outputFile = commandArgs(trailingOnly = TRUE)[2]
#column names
colFile = commandArgs(trailingOnly = TRUE)[3]
print(targetsFile)
print(colFile)
targets<-readTargets(targetsFile)
RG<-read.maimages(targets,  source="agilent.median", annotation = c("Row", "Col", "FeatureNum", "ControlType", "ProbeName"))
RG <- backgroundCorrect(RG, method="normexp", offset=16)
MA <- normalizeWithinArrays(RG, method="loess")
MA <- normalizeBetweenArrays(MA, method="quantile")
MA.avg <- avereps(MA, ID=MA$genes$ProbeName)
#change column names
cols = scan(colFile,what="character")
colnames(MA.avg) = cols
#write to output file
write.table(MA.avg$M, file=outputFile, sep="\t", quote=FALSE)
