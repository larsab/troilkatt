#!/usr/bin/ruby
# Copyright (C) 2007 Matt Hibbs
# License: Creative Commons Attribution-NonCommerical-ShareAlike 3.0 Unported (CC BY-NC-SA 3.0)
# See http://creativecommons.org/licenses/by-nc/3.0/
# Attribution shall include the copyright notice above.


if ARGV.length < 1
  puts "ARGS:"
  puts "0 - directory of pcl files to assess"
  puts "Expects all files to end with .pcl extension, will output a variety of information to std out"
  exit 0
end

def is_numeric?(x)
  true if Float(x) rescue false
end
    
print "File\t#samples\tMin\tMax\tMean\t#Neg\t#Pos\t#Zero\t#MV\t#Total\t#Channels\t"
puts  "logged\tzerosAreMVs\tMVcutoff"


files = Array.new
Dir.foreach(ARGV[0]) do |file|
  if file.slice(-4,4) == ".pcl"
    files.push(file)
  end
end

for file in files
  
  min = 1e10
  max = -1e10
  mean = numSamp = numPos = numNeg = numZero = numMissing = numTotal = 0

  IO.foreach(file) do |line|
    line.strip!
    if $. > 2
      parts = line.split("\t")
      numSamp += 1
      for i in 3...parts.length
        if !is_numeric?(parts[i])
          numMissing += 1
        else
          numTotal += 1
          val = Float(parts[i])
          mean += val
          if val > max
            max = val
          end
          if val < min
            min = val
          end
          if val > 0
            numPos += 1
          elsif val < 0
            numNeg += 1
          else
            numZero += 1
          end
        end
      end
    end
  end
  
  tested_numChans = 2
  tested_logXformed = 0
  tested_zerosMVs = 0
  tested_MVcutoff = "NA"
  
  mean /= numTotal
  if numNeg == 0 || (numPos / numNeg.to_f) > 7.5
    tested_numChans = 1
  end
  if mean < 17
    tested_logXformed = 1
  end
  if numZero > 5*numMissing
    tested_zerosMVs = 1
  end
  if tested_numChans == 1
    if tested_logXformed == 1
      tested_MVcutoff = 0
    elsif min > -500
      tested_MVcutoff = 2
    end
  end

  print file + "\t" + numSamp.to_s
  print "\t" + min.to_s + "\t" + max.to_s + "\t" + mean.to_s + "\t" + numNeg.to_s + "\t" + numPos.to_s + "\t"
  print numZero.to_s + "\t" + numMissing.to_s + "\t" + numTotal.to_s + "\t" + tested_numChans.to_s + "\t"
  puts  tested_logXformed.to_s + "\t" + tested_zerosMVs.to_s + "\t" + tested_MVcutoff.to_s
  
  $stdout.flush
end