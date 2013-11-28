#!/usr/local/bin/Rscript

inputfile <- commandArgs(trailingOnly=TRUE)[1]
outputfile <- commandArgs(trailingOnly=TRUE)[2]


library(affy)
library(affyPLM)
x<-read.table(inputfile, header=TRUE, row.names=1)
y<-new("ExpressionSet", exprs=as.matrix(x))
yy<-normalize.ExpressionSet.quantiles(y, transfn="none")
#yy<-normalize.ExpressionSet.quantiles(y, transfn="log")
#exprs(yy)<-log2(exprs(yy))
write.exprs(yy, file=outputfile)
