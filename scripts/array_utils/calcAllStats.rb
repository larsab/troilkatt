#!/usr/local/bin/ruby

if ARGV.length < 2
	puts "ARGS:"
	puts "0 - normInfoFile"
	puts "1 - path to scripts"
	exit 0
end

IO.foreach(ARGV[0]) do |line|
	line.chomp!
	parts = line.split("\t")

	cmd = "ruby " + ARGV[1] + "calcValueStats.rb " + parts[1] + "/" + parts[0] + ".final.pcl"
	puts cmd
	#system cmd
end

