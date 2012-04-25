#!/usr/bin/ruby

if ARGV.length < 2
  puts "ARGS:"
  puts "0 - extension of files to run on"
  puts "1 - ruby command to run"
  puts "2... - arguments to script"
  puts "STDOUT - recieves all output"
  exit 0
end

Dir.foreach(".") do |file|
  if file.slice(-ARGV[0].length, ARGV[0].length) == ARGV[0]
    cmd = "ruby " + ARGV[1] + " " + file
    if ARGV.length > 2
      for i in 2...ARGV.length
        cmd += " " + ARGV[i]
      end
    end
    system cmd
  end
end

