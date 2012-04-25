#!/usr/bin/ruby

#ARGV:
# 0 - file extension to match
# 1 - new extension to append
# 2... - command and arguments to run

if ARGV.length < 3
	puts "0 - old_ext - extension of files in folder to run on"
	puts "1 - new_ext - new extension to append to result files"
	puts "2 - command - script to run on the files"
	puts "3 - arg#s   - arguments to script that appear after input file"
	puts "\nExecuted commands have the form:"
	puts "\t<command> --input=<*.old_ext> [<arg1> <arg2> ...] >*.new_ext"
	exit(0)
end

Dir.foreach(".") do |file|
	if file.slice(-ARGV[0].length,ARGV[0].length) == ARGV[0]
    		cmd = ""
	  	cmd = ARGV[2] + " --input=" + file
		for i in 3...ARGV.length
			cmd = cmd + " " + ARGV[i]
		end
		fileParts = file.split(".")
		stub = fileParts[0]
		cmd = cmd + " >" + stub + "." + ARGV[1]
    		puts cmd
		system cmd
	end
end
	
