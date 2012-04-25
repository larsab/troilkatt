#!/usr/bin/ruby
# Copyright (C) 2007 Matt Hibbs
# License: Creative Commons Attribution-NonCommerical-ShareAlike 3.0 Unported (CC BY-NC-SA 3.0)
# See http://creativecommons.org/licenses/by-nc/3.0/
# Attribution shall include the copyright notice above.


if ARGV.length < 6
	puts "ARGS:"
	puts "0 - pcl starting file"
	puts "1 - configuration file (often allInfo.txt)"
	puts "2 - organism information file"
	puts "3 - path to utilities (ruby scripts, JAR files, etc.)"
	puts "4 - path to sleipnir (KNNImputer)"
	puts "5 - path to final data folders"
	puts "[6 - set to REMAKE to regenerate all files, otherwise existing"
	puts "     intermediates are used and only missing files are made]"
	exit 0
end

remake = false
if ARGV.length > 6 && ARGV[6] == "REMAKE"
	remake = true
end

pth = ARGV[3]
sleip_pth = ARGV[4]

#If remaking all, or there is no missing value file 
if remake == true || !File.exists?(ARGV[0] + ".mv")
	cmd = "ruby " + pth + "insertMissingValues.rb " + ARGV[0] + " " + ARGV[1]
	puts cmd
	system cmd
end

#If remaking all, or there is no knn file
if (remake == true || !File.exists?(ARGV[0] + ".mv.knn")) && File.exists?(ARGV[0] + ".mv")
	cmd = "ruby " + pth + "runKnnImpute.rb " + ARGV[0] + ".mv " + ARGV[1] + " " + sleip_pth
	puts cmd
	system cmd
end

#If remaking all, or there is no mapped file
if (remake == true || !File.exists?(ARGV[0] + ".mv.knn.map")) && File.exists?(ARGV[0] + ".mv.knn")
	cmd = "ruby " + pth + "mapGeneNames.rb " + ARGV[0] + ".mv.knn " + ARGV[1] + " " + ARGV[2] + " &>" + ARGV[0] + ".mv.knn.maplog"
	puts cmd
	system cmd
end

#If remaking all, or there is no avg file
if (remake == true || !File.exists?(ARGV[0] + ".mv.knn.map.avg")) && File.exists?(ARGV[0] + ".mv.knn.map")
	cmd = "java -Xmx1g -jar " + pth + "MeanGenesThatAgree.jar " + ARGV[0] + ".mv.knn.map >" + ARGV[0] + ".mv.knn.map.avg"
	puts cmd
	system cmd
	#Whenever a new averaged file is made, make a new final file
	cmd = "ruby " + pth + "collectFinalData.rb " + ARGV[0] + ".mv.knn.map.avg " + ARGV[1] + " " + pth + " " + ARGV[5]
	puts cmd
	system cmd
end



