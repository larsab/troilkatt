#!/usr/bin/ruby
# Copyright (C) 2007 Matt Hibbs
# License: Creative Commons Attribution-NonCommerical-ShareAlike 3.0 Unported (CC BY-NC-SA 3.0)
# See http://creativecommons.org/licenses/by-nc/3.0/
# Attribution shall include the copyright notice above.


if ARGV.length < 4
    puts "ARGS"
    puts "0 - pcl file to svd transform"
    puts "1 - path to calcSVDUs.R"
    puts "2 - choice of SB - signal balance"
    puts "              RP - re-projection"
    puts "              PB - project and balance"
    puts "3 - variance cutoff for RP and PB (unused for SB)"
    puts "Calls the appropriate R function requested"
    exit 0  
end

tmpRscript = ARGV[0] + "." + ARGV[2] + ".R"
fout = File.open(tmpRscript,"w")
fout.puts "source(\"" + ARGV[1] + "calcSVDUs.R\");"
fout.puts "filename <- \"" + ARGV[0] + "\";"
fout.puts "variance <- " + ARGV[3] + ";"
if (ARGV[2] == "SB")
    fout.puts "calculateAndSaveSVD.U(filename);";
elsif (ARGV[2] == "RP")
    fout.puts "calculateAndSaveSVDProjection(filename, variance);"
elsif (ARGV[2] == "PB")
    fout.puts "calculateAndSaveSVD.UProjection(filename, variance);"  
end
fout.flush
fout.close

cmd = "R --no-save --no-restore --quiet < " + tmpRscript
puts cmd
system cmd

cmd = "rm " + tmpRscript
puts cmd
system cmd


