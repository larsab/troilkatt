"""
Find datasets or series with overlapping samples
"""

import sys

if __name__ == '__main__':
    assert(len(sys.argv) == 3), 'Usage: %s gid2gsm.map outputFile' % (sys.argv[0])
    
    fp = open(sys.argv[1])
    gid2gsm = {}
    while 1:
        l = fp.readline()
        if l == '':
            break
        parts = l.split('\t')

        for i in range(len(parts)):
            parts[i] = parts[i].strip()

        gid2gsm[parts[0]] = parts[1:]
    fp.close()

    fp = open(sys.argv[2], 'w')
    fp.write('ID1\tID2\tOverlapping\tlen(GSM1)\tlen(GMS2)\n')

    sortedKeys = gid2gsm.keys()
    sortedKeys.sort()

    for i in range(len(sortedKeys)):
        gsm1 = gid2gsm[sortedKeys[i]]
        for j in range(i + 1,  len(sortedKeys)):        
            gsm2 = gid2gsm[sortedKeys[j]]

            matches = 0
            for gsm in gsm1:
                if gsm in gsm2:
                    matches += 1
            if matches > 0:
                fp.write('%s\t%s\t%d\t%d\t%d\n' % (sortedKeys[i], sortedKeys[j], matches, len(gsm1), len(gsm2)))
    fp.close()
