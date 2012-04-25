#!/usr/bin/ruby

if ARGV.length < 2
	puts "ARGS:"
	puts "0 - pcl file"
	puts "1 - percent of conditions required"
	exit 0
end

condPercent = ARGV[1].to_f
numConds = 0
numRequired = 0
IO.foreach(ARGV[0]) do |line|
	#line.strip!
	if $. == 1
		parts = line.split("\t")
		numConds = parts.length - 3
		numRequired = (numConds * condPercent).ceil
		$stderr.puts numRequired.to_s + " out of " + numConds.to_s
	end
	if $. < 3
		puts line
	else
		parts = line.split("\t")
		numPresent = 0
		for i in 3...parts.length
			if parts[i].length > 0
				numPresent += 1
			end
		end
		if numPresent >= numRequired
			puts line
		end
	end
end

