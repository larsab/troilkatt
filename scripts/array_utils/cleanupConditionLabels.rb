#!/usr/bin/ruby

IO.foreach(ARGV[0]) do |line|
	line.chomp!
	parts = line.split("\t")

	print parts[0] + "\t"
	if parts.length > 1
		conds = parts[1].split("~")
		str = ""
		for cond in conds
			loc = cond.index(":")
			str += cond.slice(loc+2,cond.length) + "~"
		end
		puts str.chop!
	else
		puts
	end
end

