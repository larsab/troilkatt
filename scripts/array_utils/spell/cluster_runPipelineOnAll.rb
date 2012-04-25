#!/usr/bin/ruby
# Copyright (C) 2007 Matt Hibbs
# License: Creative Commons Attribution-NonCommerical-ShareAlike 3.0 Unported (CC BY-NC-SA 3.0)
# See http://creativecommons.org/licenses/by-nc/3.0/
# Attribution shall include the copyright notice above.


if ARGV.length < 7
	puts "ARGS:"
	puts "0 - path to pcl files"
	puts "1 - configuration file (often allInfo.txt)"
	puts "2 - organism information file"
	puts "3 - path to pipeline scripts"
	puts "4 - path to Sleipnir binaries (KNNImputer)"
	puts "5 - path to final data folders"
	exit 0
end

IO.foreach(ARGV[1]) do |line|
	if $. > 1
		line.chomp!
		parts = line.split("\t")
		if parts[1] != nil && parts[1].strip! != ""
			filename = ARGV[0] + parts[1] + ".pcl"
			
			#Need to create a shell script to run this command
			fout = File.open(parts[1] + ".sh","w")
			fout.puts "#!/bin/bash\n"
			fout.puts "#PBS -l nodes=1:ppn=1\n"
			fout.puts "cd $PBS_O_WORKDIR"
			cmd = "ruby " + ARGV[3] + "fullyNormalizeDataset.rb " + filename + " "
			cmd += ARGV[1] + " " + ARGV[2] + " " + ARGV[3] + " " + ARGV[4] + " " + ARGV[5]
			fout.puts cmd
			fout.flush
			fout.close
			
			#And then run the script
			cmd = "qsub " + parts[1] + ".sh"
			puts cmd
			system cmd
			
			#And finally, clean up after the script
			cmd = "rm " + parts[1] + ".sh"
			#system cmd
		end
	end
end

