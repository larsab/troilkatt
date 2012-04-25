#!/usr/bin/ruby

Dir.foreach(".") do |file|
  if file.slice(-4,4) == "soft"
    labels = Hash.new
    in_data_section = false
    at_labels = false
    platform = ""
    org = ""
    dset = ""
    fout = nil
    IO.foreach(file) do |line|
      line.strip!
      if !in_data_section
        if line.include?("!dataset_platform = ")
          platform = line.slice(20,line.length)
        elsif line.include?("!dataset_platform_organism = ")
          org = line.slice(29,line.length).gsub(" ","_")
        elsif line.include?("^DATASET = ")
          dset = line.slice(11,line.length)
        elsif line.slice(0,1) == "#"
          parts = line.split(" = ")
          key = parts[0].slice(1,parts[0].length)
          labels[key] = parts[1]
        end
      end
      if at_labels
        if !File.exists?(org)
          Dir.mkdir(org)
        end
        if !File.exists?(org + "/" + platform)
          Dir.mkdir(org + "/" + platform)
        end
        fout = File.open(org + "/" + platform + "/" + dset + ".pcl", "w")

        at_labels = false
        in_data_section = true
        parts = line.split("\t")
        fout.print labels[parts[0]] + "\t" + labels[parts[1]] + "\tGWEIGHT"
        for i in 2...parts.length
          fout.print "\t" + labels[parts[i]]
        end
        fout.puts
        fout.print "EWEIGHT\t\t"
        for i in 2...parts.length
          fout.print "\t1"
        end
        fout.puts
      elsif line.include?("!dataset_table_begin")
        at_labels = true
      elsif in_data_section
        if line.include?("!dataset_table_end")
          in_data_section = false
          fout.close
        else
          parts = line.split("\t")
          fout.print(parts[0] + "\t" + parts[1] + "\t1")
          for i in 2...parts.length
            fout.print "\t" + parts[i]
          end
	  fout.puts
        end
      end
    end
  end
end

      
