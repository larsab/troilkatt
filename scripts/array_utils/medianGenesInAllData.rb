#!/usr/bin/ruby

if ARGV.length < 1
	puts "ARGS:"
	puts "0 - file containing information about what datasets to normalize:"
	puts "\tCol 0 - GDS### (should correspond to a GDS###.pcl file)"
	puts "\tCol 1 - GPL### (platform identifier, and directory structure)"
	puts "1 - script path (path to all processing scripts"
	exit 0
end

IO.foreach(ARGV[0]) do |line|
	line.chomp!
	parts = line.split("\t")
	#Median all genes
	cmd  = "java -Xmx512m -jar " + ARGV[1] + "MedianMultiples.jar " + parts[1] + "/" + parts[0] + ".mv.flt.knn.map.pcl 0"
	cmd += " >" + parts[1] + "/" + parts[0] + ".mv.flt.knn.map.med.pcl"
	puts cmd
	system cmd

	#Median all genes if they agree
  cmd  = "java -Xmx512m -jar " + ARGV[1] + "MedianMultiples.jar " + parts[1] + "/" + parts[0] + ".mv.flt.knn.map.pcl 0.6"
	cmd += " >" + parts[1] + "/" + parts[0] + ".mv.flt.knn.map.mag.pcl"
	puts cmd
	system cmd

end


