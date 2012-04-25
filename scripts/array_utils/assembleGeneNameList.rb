#!/usr/local/bin/ruby

require 'set'

if ARGV.length < 1
	puts "ARGS:"
	puts "0 - list of files"
	exit 0
end

allNames = Set.new

IO.foreach(ARGV[0]) do |file|
	file.chomp!

	IO.foreach(file) do |line|
		line.chomp!
		if $. > 2
			parts = line.split("\t")
			allNames.add(parts[1])
		end
	end

end

for name in allNames.to_a.sort
	puts name
end


