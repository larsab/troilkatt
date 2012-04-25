#!/usr/bin/ruby

if ARGV.length < 1
  puts "ARGS"
	puts "0 - file"
	exit 0
end

#print ARGV[0] + " contains "

allVals = Array.new
IO.foreach(ARGV[0]) do |line|
	line.chomp!
	parts = line.split("\t")
	if $. == 1
		numCols = parts.length
	elsif $. != 2
		for i in 3...parts.length
			if parts[i].length > 0
				allVals[allVals.length] = parts[i].to_f
			end
		end
	end
end

allVals.sort!
min = allVals[0]
max = allVals[allVals.length-1]
median = allVals[(allVals.length / 2).to_i]
sum = 0
for val in allVals
	sum += val
end
if allVals.length > 0
	mean = sum / allVals.length
else
	mean = 0
	print ARGV[0], " has no values\n"
end

puts ARGV[0] + "\t[" + min.to_s + "," + max.to_s + "]\t" + mean.to_s + "\t" + median.to_s


