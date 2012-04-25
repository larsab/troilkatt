#
# ExecuteDirTest unit test file program
#
# This program does the following:
# 1. Copy all files in the metafile specified as the first argument, to the
#    directory specified as the second argument.
#

if __name__ == '__main__':
    import sys, shutil
    
    assert(len(sys.argv) == 3), 'Usage: python %s metafile outputDir' % (sys.argv[0])
    
    metafile = sys.argv[1]
    outputDir = sys.argv[2]
    
    lines = open(metafile).readlines()
    for l in lines:
        mf = l.strip()
        shutil.copy(mf, outputDir)