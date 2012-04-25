#
# ExecuteDirTest unit test file program
#
# This program does the following:
# 1. Copy all files in the directory specified as the first argument, to the
#    directory specified as the second argument.
# 2. Open the file specified as the third argument, and verify that it contains
#    the filenames of the files copied in the first step.
#

if __name__ == '__main__':
    import sys, os, shutil
    
    assert(len(sys.argv) == 4), 'Usage: python %s inputDir outputDir metafile' % (sys.argv[0])
    
    inputDir= sys.argv[1]
    outputDir = sys.argv[2]
    metafile = sys.argv[3]
    
    # 1. Copy files
    inputFiles = os.listdir(inputDir)
    for f in inputFiles:
         shutil.copy(os.path.join(inputDir, f), outputDir)
         
    # 2. Parse meta-data
    lines = open(metafile).readlines()
    if len(lines) != len(inputFiles):
        print 'Number of input files (%d) != number of filenames in metafile (%d)' % (len(inputFiles), len(lines))
    
    for l in lines:
        mf = os.path.basename(l.strip())
        if mf not in inputFiles:
            print 'File %s in meta-file not found in input directory' % (mf)
            sys.exit(-1)
