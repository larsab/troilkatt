#!/usr/bin/ruby
# Copyright (C) 2007 Matt Hibbs
# License: Creative Commons Attribution-NonCommerical-ShareAlike 3.0 Unported (CC BY-NC-SA 3.0)
# See http://creativecommons.org/licenses/by-nc/3.0/
# Attribution shall include the copyright notice above.


if ARGV.length < 4
  puts "ARGS:"
  puts "0 - path to pcl files"
  puts "1 - file of filenames, one per line"
  puts "2 - path to Sleipnir binaries (Distancer)"
  puts "3 - path to place results"
  exit 0
end

IO.foreach(ARGV[1]) do |line|
  line.chomp!

  cmd = ARGV[2] + "Distancer -i " + ARGV[0] + line + " -o " + ARGV[3] + line + ".dab"
  puts cmd
  system cmd    
  
end