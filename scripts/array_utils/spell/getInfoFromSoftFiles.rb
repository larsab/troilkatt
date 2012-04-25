#!/usr/bin/ruby
# Copyright (C) 2007 Matt Hibbs
# License: Creative Commons Attribution-NonCommerical-ShareAlike 3.0 Unported (CC BY-NC-SA 3.0)
# See http://creativecommons.org/licenses/by-nc/3.0/
# Attribution shall include the copyright notice above.


if ARGV.length < 1
	puts "ARGS:"
	puts "0 - directory of soft files to collect header information from"
	puts "STDOUT - tab delmited header information"
	exit 0
end

tag_org = "!dataset_platform_organism = "
tag_plat = "!dataset_platform = "
tag_dset = "^DATASET = "
tag_title = "!dataset_title = "
tag_desc = "!dataset_description = "
tag_pmid = "!dataset_pubmed_id = "
tag_featCount = "!dataset_feature_count = "
tag_chanCount = "!dataset_channel_count = "
tag_sampCount = "!dataset_sample_count = "
tag_valType = "!dataset_value_type = "
tag_modDate = "!dataset_update_date = "


puts "File\tDatasetID\tOrganism\tPlatform\tValueType\t#channels\tTitle\tDesctiption\tPubMedID\t#features\t#samples\tdate"

Dir.foreach(ARGV[0]) do |file|
  if file.slice(-4,4) == "soft"
    org = platform = dset = title = desc = pmid = featCount = chanCount = sampCount = valType = date = ""
    IO.foreach(file) do |line|
      line.chomp!
      
      if line.include?(tag_org)
        org = line.slice(tag_org.length, line.length)
      elsif line.include?(tag_plat)
        platform = line.slice(tag_plat.length, line.length)
      elsif line.include?(tag_dset)
        dset = line.slice(tag_dset.length, line.length)
      elsif line.include?(tag_title)
        title = line.slice(tag_title.length, line.length)
      elsif line.include?(tag_desc)
        desc = line.slice(tag_desc.length, line.length)
      elsif line.include?(tag_pmid)
        pmid = line.slice(tag_pmid.length, line.length)
      elsif line.include?(tag_featCount)
        featCount = line.slice(tag_featCount.length, line.length)
      elsif line.include?(tag_chanCount)
        chanCount = line.slice(tag_chanCount.length, line.length)
      elsif line.include?(tag_sampCount)
        sampCount = line.slice(tag_sampCount.length, line.length)
      elsif line.include?(tag_valType)
        valType = line.slice(tag_valType.length, line.length)
			elsif line.include?(tag_modDate)
				date = line.slice(tag_modDate.length, line.length)
      end

    end

    print file + "\t" + dset + "\t" + org + "\t" + platform + "\t" + valType + "\t" + chanCount + "\t" + title
    puts  "\t" + desc + "\t" + pmid + "\t" + featCount + "\t" + sampCount + "\t" + date
  end
end

