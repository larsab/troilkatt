#
# Script run by the BigMem MapReduce program. It reads the content of a file into memory.
#
            
"""
Command line arguments: %s inputFile
"""
if __name__ == '__main__':
    import sys
    assert(len(sys.argv) == 2), 'Usage: %s inputFile' % (sys.argv[0])
    inputFilename = sys.argv[1]
    
    lines = []
    inf = open(inputFilename)
    while 1:
        line = inf.readline()
        if line == '':
            break
        lines.append(line)
    inf.close()
    
    print 'Done'