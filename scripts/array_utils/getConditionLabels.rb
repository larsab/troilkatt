#!/usr/bin/ruby

if ARGV.length < 1
	puts "ARGS:"
	puts "0 - norm Info file"
	exit(0)
end

breakChar = "~"

IO.foreach(ARGV[0]) do |line|
	line.strip!
	parts = line.split("\t")
	
	fname = "final/" + parts[0] + ".final.pcl"

	print parts[0] + "\t"

	fin = open(fname)
	if fin != nil && !fin.eof?
		condLine = fin.readline.chomp!
		condParts = condLine.split("\t")
		if condLine.include?(breakChar)
			$stderr.puts "WARNING: " + fname + " contains the break string"
		end
		print condParts[3]
		for i in 4...condParts.length
			print breakChar + condParts[i]
		end
	end
	puts

end
	
