# Copyright (C) 2007 Matt Hibbs
# License: Creative Commons Attribution-NonCommerical-ShareAlike 3.0 Unported (CC BY-NC-SA 3.0)
# See http://creativecommons.org/licenses/by-nc/3.0/
# Attribution shall include the copyright notice above.


calcSVDUs <- function(fileOfFilenames) {
	fileList <- read.table(fileOfFilenames, colClasses=c("character"));
	numFiles <- dim(fileList)[1];
	for (i in 1:numFiles) {
		calculateAndSaveSVD.U(fileList[i,1]);
	}
}

calculateAndSaveSVD.U <- function(filename) {
	data <- read.table(filename, sep="\t", stringsAsFactors=FALSE, quote="", comment.char="");
	sz <- dim(data);
	x <- data[3:sz[1],4:sz[2]];
	x <- apply(x,c(1,2),as.numeric);
	result <- svd(x);
	data[3:sz[1],4:sz[2]] <- result$u;
	newfilename <- paste(filename,"svd_u",sep=".");
	write.table(data,sep="\t",file=newfilename,quote=FALSE,col.names=FALSE,row.names=FALSE);
}

calculateAndSaveSVDProjection <- function(filename, variance) {
	data <- read.table(filename, sep="\t", stringsAsFactors=FALSE, quote="", comment.char="");
	sz <- dim(data);
	x <- data[3:sz[1],4:sz[2]];
	x <- apply(x,c(1,2),as.numeric);
	result <- svd(x);
	dsq <- result$d * result$d;
	dvar <- dsq / sum(dsq);
	dsum <- 0;
	duse <- 0;
	new.d <- result$d;
	for (i in 1:length(dvar)) {
		dsum <- dsum + dvar[i];
		if (dsum < variance) {
			duse <- i;
		}
		else {
			new.d[i] <- 0;
		}
	}
	reproj <- result$u %*% diag(new.d) %*% t(result$v);
	data[3:sz[1],4:sz[2]] <- reproj;
	newfilename <- paste(filename,".svd_",variance,"_proj",sep="");
	write.table(data,sep="\t",file=newfilename,quote=FALSE,col.names=FALSE,row.names=FALSE);
}

calculateAndSaveSVD.UProjection <- function(filename, variance) {
	data <- read.table(filename, sep="\t", stringsAsFactors=FALSE, quote="", comment.char="");
	sz <- dim(data);
	x <- data[3:sz[1],4:sz[2]];
	x <- apply(x,c(1,2),as.numeric);
	result <- svd(x);
	dsq <- result$d * result$d;
	dvar <- dsq / sum(dsq);
	dsum <- 0;
	duse <- 0;
	new.d <- result$d;
	for (i in 1:length(dvar)) {
		dsum <- dsum + dvar[i];
		if (dsum < variance) {
			duse <- i;
		}
		else {
			new.d[i] <- 0;
		}
	}
	#Require that 4 dimensions are retained
	duse <- max(duse, 4);
	new.data <- matrix(nrow=sz[1],ncol=(3+duse));
	
	d <- as.matrix(data);
	new.data[1:sz[1],1:3] <- d[1:sz[1],1:3];
	new.data[1:2,4:(duse+3)] <- d[1:2,4:(duse+3)];
	new.data[3:sz[1],4:(duse+3)] <- result$u[1:(sz[1]-2),1:duse];
	
	newfilename <- paste(filename,".svd_",variance,"_bal",sep="");
	write.table(new.data,sep="\t",file=newfilename,quote=FALSE,col.names=FALSE,row.names=FALSE);
}
