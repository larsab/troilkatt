#!/usr/bin/ruby

if ARGV.length < 1
	puts "ARGS:"
	puts "0 - path to data"
	puts "1 - path for results"
	exit 0
end

Dir.foreach(ARGV[0]) do |file|
	if file.slice(-4,4) == ".pcl"
		cmd = "java -Xmx2g MedianMultiples " + ARGV[0] + file + " 1"
		cmd += " >" + ARGV[1] + file + ".mlm.pcl"
		puts cmd
		system cmd
	end
end

#IO.foreach(ARGV[0]) do |line|
#	line.chomp!
#	parts = line.split("\t")
  
#	cmd  = "java -Xmx2g MedianMultiples " + ARGV[1] + parts[1] + "/" + parts[0] + ".mv.flt.knn.map.pcl 1"
#	cmd += " >" + ARGV[1] + parts[1] + "/" + parts[0] + ".mv.flt.knn.map.mlm.pcl"
#	puts cmd
#	system cmd

#end


