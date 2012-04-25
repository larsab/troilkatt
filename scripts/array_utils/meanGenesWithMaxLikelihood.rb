#!/usr/bin/ruby

if ARGV.length < 1
	puts "ARGS:"
	puts "0 - file containing information about what datasets to normalize:"
	puts "\tCol 0 - GDS### (should correspond to a GDS###.pcl file)"
	puts "\tCol 1 - GPL### (platform identifier, and directory structure)"
	puts "1 - path to data"
	exit 0
end

IO.foreach(ARGV[0]) do |line|
	line.chomp!
	parts = line.split("\t")
  
	cmd  = "java -Xmx2g MedianMultiples " + ARGV[1] + parts[1] + "/" + parts[0] + ".mv.flt.knn.map.pcl 1"
	cmd += " 1>" + ARGV[1] + parts[1] + "/" + parts[0] + ".mv.flt.knn.map.mlm.pcl"
	cmd += " 2>" + ARGV[1] + parts[1] + "/" + parts[0] + ".maxlikl.log.txt"
	puts cmd
	system cmd

end


