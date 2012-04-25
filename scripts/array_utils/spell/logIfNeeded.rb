#!/usr/bin/ruby
# Copyright (C) 2007 Matt Hibbs
# License: Creative Commons Attribution-NonCommerical-ShareAlike 3.0 Unported (CC BY-NC-SA 3.0)
# See http://creativecommons.org/licenses/by-nc/3.0/
# Attribution shall include the copyright notice above.


if ARGV.length < 3
  puts "ARGS:"
  puts "0 - pcl file"
  puts "1 - configuration file (often allInfo.txt)"
  puts "2 - path to DivLogNorm.jar"
  puts "Script will log transform the file if needed as specified in the config file"
  puts "**NOTE:  The original pcl file is _replaced_ with the log xformed version**"
  exit 0
end

#Important configuration columns
GDSID_col = 1
log_col = 21

#Read the configuration file and locate the needed info
if ARGV[0].rindex("/") != nil
  filename = ARGV[0].slice(ARGV[0].rindex("/") + 1, ARGV[0].length)
else
  filename = ARGV[0]
end
GDSID = filename.slice(0, filename.index("."))
  
logged = nil
IO.foreach(ARGV[1]) do |line|
  line.chomp!
  parts = line.split("\t")
  if parts[GDSID_col] == GDSID
    logged  = parts[log_col].to_i
  end
end

#If the data needs to be log transformed, do it and pipe to the final file
if logged == 0
  cmd = "java -Xmx1g -jar " + ARGV[2] + "DivLogNorm.jar " + ARGV[0] + " 0 1 0 >" + ARGV[0] + ".tmplog"
  puts cmd
  system cmd
  cmd = "mv " + ARGV[0] + ".tmplog " + ARGV[0]
  puts cmd
  system cmd
#Otherwise, do nothing
else
end
