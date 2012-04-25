#!/usr/bin/ruby

IO.foreach(ARGV[0]) do |line|
  line.strip!
  parts = line.split("\t")
  if parts[3].slice(0,3) != "GSM"
    puts line
  end
end
