#!/usr/bin/ruby

if ARGV.length < 3
	puts "ARGS:"
	puts "0 - further alias mapping file"
	puts "1 - norm info file"
	puts "2 - path to data"
	exit 0
end

aliasMap = Hash.new
IO.foreach(ARGV[0]) do |line|
	line.chomp!
	parts = line.split("\t")
	aliasMap[parts[0]] = parts[1]
end

IO.foreach(ARGV[1]) do |line|
	line.chomp!
	parts = line.split("\t")
	file = parts[0] + ".mv.flt.knn.map.pcl"
	file_with_path = ARGV[2] + parts[1] + "/" + file

	new_file_with_path = ARGV[2] + parts[1] + "/" + parts[0] + ".mv.flt.knn.map.fly.pcl"
	fout = File.new(new_file_with_path,"w")

	numNotInMap = 0
	IO.foreach(file_with_path) do |pclLine|
		pclLine.chomp!
		if $. < 3
			fout.puts pclLine
		else
			pclParts = pclLine.split("\t")
			if aliasMap[pclParts[1]] != nil
				fout.print pclParts[1] + "\t" + aliasMap[pclParts[1]]
				for i in 2...pclParts.length
					fout.print "\t" + pclParts[i]
				end
				fout.puts
			else
				fout.puts pclLine
				numNotInMap += 1
			end
		end
	end

	fout.close
	
	$stderr.puts file + "\t" + numNotInMap.to_s
	
end

