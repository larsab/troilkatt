#!/usr/bin/ruby

Dir.foreach(".") do |file|
  if file.slice(file.length-4,4) == "soft"
    
    org = ""
    platform = ""
    dset = ""
    IO.foreach(file) do |line|
      line.strip!
      if line.include?("!dataset_platform = ")
        platform = line.slice(20,line.length)
      elsif line.include?("!dataset_platform_organism = ")
        org = line.slice(29,line.length)
      elsif line.include?("^DATASET = ")
        dset = line.slice(11,line.length)
      end
    end

    puts dset + ": " + platform + ": " + org
  end
end

