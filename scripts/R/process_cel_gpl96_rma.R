#!/usr/bin/Rscript


processDir = commandArgs( trailingOnly = TRUE)[1]
outputFile = commandArgs( trailingOnly = TRUE)[2]

library(affy)
library(affyio)

#files now holds a list of all celfiles in the directory to be processed
files <- list.celfiles(processDir)

#ptype is now a vector with the type of each array
ptype <- sapply(files, function(f) read.celfile.header(paste(processDir, f, sep="/"))[1])

#ptype levels are the levels of that vector (i.e. the types of arrays present)
#this is important because only arrays of the same type can be processed together
ptype_levels <- levels(as.factor(unlist(ptype)))

#cdffile <- "HGU133Plus2_Hs_REFSEQ"
#valid_platform <- "HG-U133_Plus_2"

cdffile <- "HGU133A_Hs_REFSEQ"
valid_platform <- "HG-U133A"
good_probe_file <- "unique_probes_gpl96_refseq.txt"

#automatic downloading of cdf file
UMRepos<-getOption("repositories2") 
UMRepos["UMRepository"] = 'http://arrayanalysis.mbni.med.umich.edu/repository' 
options('repositories2' = UMRepos) 


pfiles <- paste(processDir, subset(files, ptype==valid_platform), sep="/")
validFiles <- rep(NA, length(pfiles))
validFilesCnt <- 1

for(i in 1:length(pfiles)) {
    tryCatch( {
      x <- ReadAffy(cdfname=cdffile, filenames = pfiles[i])
      validFiles[validFilesCnt] <- c(pfiles[i])
      validFilesCnt <- validFilesCnt + 1
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


tryCatch( {
    data <- ReadAffy(cdfname=cdffile, filenames = pfiles2)
	cat("Finished reading data", "\n")

	data_bg <- bg.correct(data, method="rma")
	#data_bg <- bg.correct(data, method="mas")

	cat("Finished background correcting", "\n")

	#data_n <- normalize(data_bg, method="quantiles")
	#cat("Finished normalization", "\n")

	cdfinfo <- getCdfInfo(data)
	Index <- pmindex(data)

	good_probes<-read.table(good_probe_file, header=FALSE, row.names=NULL)
	good_probes<-t(good_probes)

	cat("Summarizing probes and outputing to file", "\n")

	m<-length(good_probes)
	c.pps<-new("ProbeSet", pm=matrix(), mm=matrix())

	ch<-matrix(nrow=m, ncol=6)
	sink(outputFile)

	for (x in seq(along=good_probes)) {
		id<-good_probes[x]
		#cat(x, "\n")
		loc <- get(id, envir=cdfinfo)
		l.pm<-loc[,1]
		l.mm<-loc[,2]
		c.pps@pm<-intensity(data_bg)[l.pm, , drop=FALSE]
		c.pps@mm<-intensity(data_bg)[l.mm, , drop=FALSE]

		#pm<-pmcorrect.mas(c.pps)

		i<-log2(c.pps@pm)
		#i<-log2(pm)
		cu<-cutree(hclust(as.dist(1-cor(t(i))), method="av"), h=c(0, 0.2, 0.4, 0.5, 0.7, 0.8, 1.0))

		#p<-probeset(data_n, x)
		mm<-express.summary.stat(c.pps, pmcorrect="pmonly", summary="medianpolish")$exprs
		#mm<-generateExprVal.method.mas(pm)$exprs

		if(x==1){
			cat("HEAD\t")
			for(y in colnames(c.pps@pm)){
				cat(y, "\t", sep="")
			}
			cat("\n")
		}
	
		#print probeset membership
		cat("PROBES", "\t", id, "\t", sep="")
		for(j in 1:nrow(i)){
			cat(l.pm[j], "\t", sep="")
		}
		cat("\n")
	
		#cluster the probes in a probeset by expression similarity (based on pearson cor)
		#threshold 0.2
		cat("PROBE_MEM_0.2", "\t", id, "\t", sep="")
		for (j in 1:nrow(i)){
			cat(cu[j, "0.2"], "\t", sep="")
		}
		cat("\n")
		#threshold 0.5
		cat("PROBE_MEM_0.5", "\t", id, "\t", sep="")
		for (j in 1:nrow(i)){
			cat(cu[j, "0.5"], "\t", sep="")
		}
		cat("\n")
		#threshold 0.8
		cat("PROBE_MEM_0.8", "\t", id, "\t", sep="")
		for (j in 1:nrow(i)){
			cat(cu[j, "0.8"], "\t", sep="")
		}
		cat("\n")

		#print summarized probe to original probe correlation
		cat("COR", "\t", id, "\t", sep="")
		for (j in 1:nrow(i)){
			co<-cor(mm, i[j,])
			cat(round(co, 2))
			cat("\t")
		}
		cat("\n")

		#print expression of summarized probe
		cat("EXPR", "\t", id, "\t", sep="")
		for (j in 1:length(mm)){
			cat(round(mm[j], 4), "\t", sep="")
		}
		cat("\n")

	}

	sink()

  }, error = function(e) {
    print(e)
    print(paste("Could not normalize platform", valid_platform))
    errorOccured = 1
  }, finally = {
    print(paste("Processed platform", valid_platform, "at", Sys.time()))
  })



