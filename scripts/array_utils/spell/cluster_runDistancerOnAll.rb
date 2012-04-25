#!/usr/bin/ruby
# Copyright (C) 2007 Matt Hibbs
# License: Creative Commons Attribution-NonCommerical-ShareAlike 3.0 Unported (CC BY-NC-SA 3.0)
# See http://creativecommons.org/licenses/by-nc/3.0/
# Attribution shall include the copyright notice above.


if ARGV.length < 4
  puts "ARGS:"
  puts "0 - path to pcl files"
  puts "1 - file of filenames"
  puts "2 - path to Sleipnir binaries (Distancer)"
  puts "3 - destination path for dabs"
  exit 0
end

IO.foreach(ARGV[1]) do |line|
  line.chomp!
     
  #Need to create a shell script to run this command
  fout = File.open(line + ".sh","w")
  fout.puts "#!/bin/bash\n"
  fout.puts "#PBS -l nodes=1:ppn=1\n"
  fout.puts "cd $PBS_O_WORKDIR"
  cmd = ARGV[2] + "Distancer -i " + ARGV[0] + line + " -o " + ARGV[3] + line + ".dab"
  fout.puts cmd
  fout.flush
  fout.close
  
  #And then run the script
  cmd = "qsub " + line + ".sh"
  puts cmd
  system cmd
      
  #And finally, clean up after the script
  cmd = "rm " + line + ".sh"
  #system cmd
end