#!/usr/bin/ruby

if ARGV.length < 1
	puts "ARGS:"
	puts "0 - file containing information about what datasets to normalize:"
	puts "\tCol 0 - GDS### (should correspond to a GDS###.pcl file)"
	puts "\tCol 1 - GPL### (platform identifier, and directory structure)"
	exit 0
end

IO.foreach(ARGV[0]) do |line|
	line.chomp!
	parts = line.split("\t")

	cmd = "cp " + parts[1] + "/" + parts[0] + ".final.pcl final/" + parts[0] + ".final.pcl"
	puts cmd
	system cmd

end

