#!/usr/bin/ruby
# Copyright (C) 2007 Matt Hibbs
# License: Creative Commons Attribution-NonCommerical-ShareAlike 3.0 Unported (CC BY-NC-SA 3.0)
# See http://creativecommons.org/licenses/by-nc/3.0/
# Attribution shall include the copyright notice above.


require 'bio'

if ARGV.length < 1
	puts "ARGS:"
	puts "0 - file containing dset filenames and pubmed IDs, one per line"
	puts "\tCol 0 - filename"
	puts "\tCol 1 - pubmed ID"
	puts "1 - path to files"
	exit 0
end

puts "PubMedID\tFilename\tGeoID\tPlatformID\tchannelCount\tDatasetName\tDescription\tNumConditions\tNumGenes\tFirstAuthor\tAllAuthors\tTitle\tJournal\tPubYear\tConditionDescriptions\tTags"

IO.foreach(ARGV[0]) do |line|
	line.chomp!
	parts = line.split("\t")

	ref = nil
	pmid = 0
	if parts.length > 1
		pmid = parts[1].to_i
		begin
			ref = Bio::PubMed.pmfetch(pmid).split("\n")
		rescue
			begin
				ref = Bio::PubMed.pmfetch(pmid).split("\n")
			rescue
				ref = Bio::PubMed.pmfetch(pmid).split("\n")
			end
		end
	end

	first_author = all_authors = journal = pub_year = title = abstract = ""

	if ref == nil || ref[0].slice(0,6) == "<html>"
		first_author = all_authors = journal = abstract = "Unknown"
		title = "No publication known"
		pub_year = 0
	else
		inTitle = foundFirstAuthor = inAbstract = false
		for refLine in ref
			if inTitle
				if refLine.slice(0,1) == " "
					title += refLine.slice(5,refLine.length)
				else
					inTitle = false
				end
			end

			if inAbstract
				if refLine.slice(0,1) == " "
					abstract += refLine.slice(5,refLine.length)
				else
					inAbstract = false
				end
			end
			
			if refLine.slice(0,4) == "TI  "
				title = refLine.slice(6,refLine.length)
				inTitle = true
			end
	
			if refLine.slice(0,4) == "AB  "
				abstract = refLine.slice(6,refLine.length)
				inAbstract = true
			end
	
			if refLine.slice(0,4) == "JT  "
				journal = refLine.slice(6,refLine.length)
			end
	
			if refLine.slice(0,4) == "DP  "
				pub_year = refLine.slice(6,4)
			end
	
			if refLine.slice(0,4) == "AU  "
				if !foundFirstAuthor
					first_author = refLine.slice(6,refLine.length)
					all_authors = refLine.slice(6,refLine.length)
					foundFirstAuthor = true
				else
					all_authors += ", " + refLine.slice(6,refLine.length)
				end
			end
		end
	end
	
	channelCount = num_conds = num_genes = 0
	largestVal = -1000
	smallestVal = 1000
	condDesc = ""
	IO.foreach(ARGV[1] + parts[0]) do |fline|
		fline.chomp!
		if $. == 1
			fparts = fline.split("\t")
			condDesc = fparts[3]
			for i in 4...fparts.length
				condDesc += "~" + fparts[i]
			end
			num_conds = fparts.length - 3
		elsif $. > 2
			num_genes += 1
			fparts = fline.split("\t")
			for i in 3...fparts.length
				if fparts[i].to_f > largestVal
					largestVal = fparts[i].to_f
				end
				if fparts[i].to_f < smallestVal
					smallestVal = fparts[i].to_f
				end
			end
		end
	end
	if smallestVal < -1 && largestVal < 100
		channelCount = 2
	else
		channelCount = 1
	end
	
	print pmid.to_s + "\t" + parts[0] + "\tGDS?\tGPL?\t" + channelCount.to_s + "\t"
	print title + "\t" + abstract + "\t" + num_conds.to_s + "\t" + num_genes.to_s
	print "\t" + first_author + "\t" + all_authors + "\t" + title + "\t" + journal
	print "\t" + pub_year.to_s + "\t" + condDesc + "\tdefault"
	puts

end


