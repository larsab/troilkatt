#!/usr/bin/ruby
# Copyright (C) 2007 Matt Hibbs
# License: Creative Commons Attribution-NonCommerical-ShareAlike 3.0 Unported (CC BY-NC-SA 3.0)
# See http://creativecommons.org/licenses/by-nc/3.0/
# Attribution shall include the copyright notice above.


if ARGV.length < 6
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
      
      cmd = "ruby " + ARGV[3] + "normalizeWithExtraVersions.rb " + filename
      cmd += " " + ARGV[1] + " " + ARGV[2] + " " + ARGV[3] + " " + ARGV[4] + " " + ARGV[5]
      puts cmd
      system cmd
    end
  end
end