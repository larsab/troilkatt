#!/usr/bin/ruby

Dir.foreach(".") do |file|
  if file.slice(file.length-2,2) == "gz"
    cmd = "gunzip " + file
    puts cmd
    system cmd
  end
end

