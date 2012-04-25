#!/usr/bin/ruby

Dir.foreach(".") do |file|
  if file.slice(-4,4) == "soft"
    print file
    IO.foreach(file) do |line|
      #line.strip!
      if line.slice(0,1) == "#"
        print "\t" + line.slice(1,line.length)
      end
    end
    puts
  end
end

