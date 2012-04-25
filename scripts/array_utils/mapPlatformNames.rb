#!/usr/bin/ruby

if ARGV.length < 3
	puts "ARGS:"
	puts "0 - platform index"
	puts "1 - norm info file"
	puts "2 - path to data"
	puts "3 - path to platforms"
	exit 0
end

pidx = ARGV[0].to_i

IO.foreach(ARGV[1]) do |line|
	line.chomp!
	parts = line.split("\t")
	file = parts[0] + ".mv.flt.knn.pcl"
	file_with_path = ARGV[2] + parts[1] + "/" + file
	platform_path = ARGV[3] + "/" + parts[1] + ".annot"

	id2name = Hash.new
	IO.foreach(platform_path) do |pline|
		pline.chomp!
		fchar = pline.slice(0,1)
		if fchar != "^" && fchar != "#" && fchar != "!"
			pparts = pline.split("\t")
			if pparts[pidx] != nil
				fbParts = pparts[pidx].split(/\/\/\//)
				fb = nil
				if pparts[pidx].slice(0,2) == "FB"
					fb = pparts[pidx]
				end
				for fbpart in fbParts
					if fbpart.slice(0,2) == "FB"
						fb = fbpart
					end
				end
				id2name[pparts[0]] = fb
			end
		end
	end

	new_file_with_path = ARGV[2] + parts[1] + "/" + parts[0] + ".mv.flt.knn.map.pcl"
	fout = File.new(new_file_with_path,"w")

	numGenes = 0
	IO.foreach(file_with_path) do |pclLine|
		pclLine.chomp!
		if $. < 3
			fout.puts pclLine
		else
			pclParts = pclLine.split("\t")
			if id2name[pclParts[0]] != nil
				fout.print pclParts[0] + "\t" + id2name[pclParts[0]]
				for i in 2...pclParts.length
					fout.print "\t" + pclParts[i]
				end
				fout.puts
				numGenes += 1
			end
		end
	end

	fout.close
	
	$stderr.puts file + "\t" + numGenes.to_s
	
end

