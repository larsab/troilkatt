#
# ExecutePerFile unit test program
#
# This program does the following:
# 1. Copy the file specified as the first argument, to the directory specified as 
#    the second argument.
# 2. Open the file specified as the third argument, and verify that it contains
#    the filenames of the files copied in the first step.
#

if __name__ == '__main__':
    import sys, os, shutil
    
    assert(len(sys.argv) == 4), 'Usage: python %s inputFile outputDir metafile' % (sys.argv[0])
    
    inputFile = sys.argv[1]
    outputFile = sys.argv[2]
    metafile = sys.argv[3]
         
    # 1. Parse meta-data
    lines = open(metafile).readlines()
    inputFiles = []
    for l in lines:
        inputFiles.append(os.path.basename(l.strip()))
    
    if os.path.basename(inputFile) not in inputFiles:
        print 'Input file %s not found in metafile' % (inputFile)
        sys.exit(-1)

     # 2. Copy files
    shutil.copy(inputFile, outputFile)