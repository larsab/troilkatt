#!/usr/bin/ruby

if ARGV.length < 1
	puts "ARGS:"
	puts "0 - file containing information about what datasets to normalize:"
	puts "\tCol 0 - GDS### (should correspond to a GDS###.pcl file)"
	puts "\tCol 1 - GPL### (platform identifier, and directory structure)"
	puts "\tCol 2 - Cutoff for missing value insertion"
	puts "\tCol 3 - Should be log xform'd? (1=yes, 0=no)"
	puts "\tCol 4 - Should be median div'd? (1=yes, 0=no)"
	puts "1 - script path (path to all processing scripts"
	exit 0
end

IO.foreach(ARGV[0]) do |line|
	line.chomp!
	parts = line.split("\t")
	#Insert missing values
	cmd  = "ruby " + ARGV[1] + "createMissingValues.rb " + parts[1] + "/" + parts[0] + ".pcl "
	cmd += parts[2] + " >" + parts[1] + "/" + parts[0] + ".mv.pcl"
	puts cmd
	system cmd

	#Filter out lines with few valid conditions
	cmd = "ruby " + ARGV[1] + "removeGenesInFewConditions.rb " + parts[1] + "/" + parts[0] + ".mv.pcl 0.75 >"
	cmd += parts[1] + "/" + parts[0] + ".mv.flt.pcl"
	puts cmd
	system cmd

	#KNNImpute missing values
	cmd = ARGV[1] + "KNNImputer --skip=2 --input=" + parts[1] + "/" + parts[0] + ".mv.flt.pcl >"
	cmd += parts[1] + "/" + parts[0] + ".mv.flt.knn.pcl"
	puts cmd
	system cmd
end


