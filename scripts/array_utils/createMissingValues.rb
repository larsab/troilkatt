#!/usr/bin/ruby

if ARGV.length < 3
  puts "ARGS:"
  puts "0 - pcl file to create missing values for"
  puts "1 - output file"
  puts "2 - cutoff value (values less than this are removed)"
  exit 0
end

cutoff = ARGV[2].to_f
numCols = 0
outName=ARGV[1]
puts outName
fout=File.open(outName, "w")
IO.foreach(ARGV[0]) do |line|
  #line.strip!
  if $. == 1
    parts = line.split("\t")
    numCols = parts.length
  end
  if $. < 3
    fout.puts line
  else
    parts = line.split("\t")
    fout.print parts[0] + "\t" + parts[1] + "\t" + parts[2]
    for i in 3...parts.length
      begin
        val = Float(parts[i])
        if val >= cutoff
          fout.print "\t" + val.to_s
        else
          fout.print "\t"
        end
      rescue
        fout.print "\t"
      end
    end
    for i in parts.length...numCols
      fout.print "\t"
    end
    fout.puts
  end
end

