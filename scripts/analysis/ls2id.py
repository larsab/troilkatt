"""
Convert an ls (hadoop or local Fs) list to IDs
"""

import sys

def parseLine(line):
    print 'Line is: <start>%s<end>' % (line)
    dm = raw_input("Enter delimeter: ")
    print 'Parts are:'
    idCnt = 0
    for p in line.split(dm):
        print '%d. %s' % (idCnt, p)
        idCnt += 1
    idCol = int(raw_input('Enter ID column to use (-1 for a new line): '))
    return (dm, idCol)

if __name__ == '__main__':                                                                                                                           
    assert(len(sys.argv) == 2), 'Usage: python ls.output'                                                                      

    inputFile = sys.argv[1]
    ids = set()    
                
    fp = open(inputFile)
    rows = fp.readlines()
    fp.close()
    dm = None
    idCol = 0  
    for r in rows:
        (dm, idCol) = parseLine(r.strip())
        if idCol != -1:
            break
    
    for r in rows:
        cols = r.strip().split(dm)
        if len(cols) < idCol:
            continue
        print cols[idCol]
