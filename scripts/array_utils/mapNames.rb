#!/usr/bin/ruby

if ARGV.length < 3
	puts "ARGS:"
	puts "0 - alias mapping file"
	puts "1 - norm info file"
	puts "2 - path to data"
	puts "3 - path to platforms"
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
	file = parts[0] + ".mv.flt.knn.pcl"
	file_with_path = ARGV[2] + parts[1] + "/" + file

	new_file_with_path = ARGV[2] + parts[1] + "/" + parts[0] + ".mv.flt.knn.map.pcl"
	fout = File.new(new_file_with_path,"w")

	numGenes = 0
	IO.foreach(file_with_path) do |pclLine|
		pclLine.chomp!
		if $. < 3
			fout.puts pclLine
		else
			pclParts = pclLine.split("\t")
			if aliasMap[pclParts[1]] != nil
				for hgncName in aliasMap[pclParts[1]].split("|")
					fout.print pclParts[0] + "\t" + hgncName
					for i in 2...pclParts.length
						fout.print "\t" + pclParts[i]
					end
					fout.puts
					numGenes += 1
				end
			end
		end
	end

	fout.close
	
	$stderr.puts file + "\t" + numGenes.to_s
	
end

