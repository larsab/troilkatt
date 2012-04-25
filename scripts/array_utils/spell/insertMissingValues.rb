#!/usr/bin/ruby
# Copyright (C) 2007 Matt Hibbs
# License: Creative Commons Attribution-NonCommerical-ShareAlike 3.0 Unported (CC BY-NC-SA 3.0)
# See http://creativecommons.org/licenses/by-nc/3.0/
# Attribution shall include the copyright notice above.


if ARGV.length < 2
  puts "ARGS:"
  puts "0 - pcl file to insert missing values into"
  puts "1 - configuration file (often allInfo.txt)"
  puts "2 - output file"
  puts "This script will use the information from the config file to create the"
  puts "missing values deemed necessary for this pcl.  A new file with the same"
  puts "name will be created with the extension \".mv\" added containing the "
  puts "new result" 
  exit 0
end


#Important configuration columns
GDSID_col = 1
numChan_col = 20
zeroMV_col = 22
mvCutoff_col = 23
nMVsCol = 18
nTotalCol = 19

#Read the configuration file and locate the needed info
if ARGV[0].rindex("/") != nil
  filename = ARGV[0].slice(ARGV[0].rindex("/") + 1, ARGV[0].length)
else
  filename = ARGV[0]
end
GDSID = filename.slice(0, filename.index("."))

numChan = mvCutoff = zeroMV = nil
nMVs = nVals = nil
entryFound = false
IO.foreach(ARGV[1]) do |line|
	line.chomp!
	parts = line.split("\t")
	if parts[GDSID_col] == GDSID
	  entryFound = true
		numChan = parts[numChan_col].to_i
		zeroMV  = parts[zeroMV_col].to_i
		if parts[mvCutoff_col] == "NA"
			mvCutoff = "NA"
		else
			mvCutoff= parts[mvCutoff_col].to_f
		end
		nMVs = parts[nMVsCol].to_f
		nVals = parts[nTotalCol].to_f
	end
end

if entryFound == false
  puts "GSID not found in info file"
  exit 0
end

if nMVs > nVals
  puts "Too many missing values"
  exit 0
end

#Open the new output file
fout = File.open(ARGV[2], "w")

#Read the pcl file and insert the desired missing values
numCols = 0
IO.foreach(ARGV[0]) do |line|
	line.chomp!
	if $. == 1
		parts = line.split("\t")
		numCols = parts.length
	end
	if $. < 3
		fout.puts line
	else
		parts = line.split("\t")
		if line != "" && parts.length > 3
  		fout.print parts[0] + "\t" + parts[1] + "\t" + parts[2]
  		for i in 3...parts.length
  			begin
  				val = Float(parts[i])
  				if zeroMV == 1 && val == 0
  					fout.print "\t"
  				elsif mvCutoff == "NA" || val >= mvCutoff
  					fout.print "\t" + val.to_s
  				else
  					fout.print "\t"
  				end
  			rescue
  				fout.print "\t"
  			end
  		end
  		for i in parts.length...numCols
  			fout.print "\t"
  		end
  		fout.puts
		end
	end
end

#Close the output file
fout.flush
fout.close
