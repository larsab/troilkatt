#!/usr/bin/ruby

if ARGV.length < 1
	puts "ARGS:"
	puts "0 - file containing information about what datasets to normalize:"
	puts "\tCol 0 - GDS### (should correspond to a GDS###.pcl file)"
	puts "\tCol 1 - GPL### (platform identifier, and directory structure)"
	exit 0
end

puts "DatasetID\t#probes\t#median\t#med_agree"

IO.foreach(ARGV[0]) do |line|
	line.chomp!
	parts = line.split("\t")
	
	print parts[0]
	
	file = parts[1] + "/" + parts[0] + ".mv.flt.knn.pcl"
	fin = File.open(file)
	lines = fin.read.split(/[\r\n]+/)
	print "\t" + (lines.length - 2).to_s
	fin.close

	file = parts[1] + "/" + parts[0] + ".mv.flt.knn.map.med.pcl"
	fin = File.open(file)
	lines = fin.read.split(/[\r\n]+/)
	print "\t" + (lines.length - 2).to_s
	fin.close

	file = parts[1] + "/" + parts[0] + ".mv.flt.knn.map.mag.pcl"
	fin = File.open(file)
	lines = fin.read.split(/[\r\n]+/)
	print "\t" + (lines.length - 2).to_s
	fin.close
	
	puts
end
				
	

