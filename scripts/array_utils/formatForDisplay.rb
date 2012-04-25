#!/usr/bin/ruby

if ARGV.length < 2
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

#Anything that should be DIV'd will need to have AvgDivLogNorm.jar run on it with
#divide by median and log transform result active

IO.foreach(ARGV[0]) do |line|
	line.chomp!
	parts = line.split("\t")

	if parts[4] == "1"
		cmd = "java -Xmx512m -jar " + ARGV[1] + "AvgDivLogNorm.jar " + parts[1] + "/" + parts[0]
		cmd += ".mv.flt.knn.map.med.pcl 0 1 1 0 >" + parts[1] + "/" + parts[0] + ".mv.flt.knn.map.med.disp.pcl"
		puts cmd
		system cmd

		cmd = "java -Xmx512m -jar " + ARGV[1] + "AvgDivLogNorm.jar " + parts[1] + "/" + parts[0]
		cmd += ".mv.flt.knn.map.mag.pcl 0 1 1 0 >" + parts[1] + "/" + parts[0] + ".mv.flt.knn.map.mag.disp.pcl"
		puts cmd
		system cmd
	else
		cmd = "cp " + parts[1] + "/" + parts[0] + ".mv.flt.knn.map.med.pcl " + parts[1] + "/" + parts[0]
		cmd += ".mv.flt.knn.map.med.disp.pcl"
		puts cmd
		system cmd

		cmd = "cp " + parts[1] + "/" + parts[0] + ".mv.flt.knn.map.mag.pcl " + parts[1] + "/" + parts[0]
		cmd += ".mv.flt.knn.map.mag.disp.pcl"
		puts cmd
		system cmd
	end
end

