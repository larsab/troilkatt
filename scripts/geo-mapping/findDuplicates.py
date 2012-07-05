import datetime
import sys

"""
Get a dictonary with gid to date mappings.
"""
def getDates(table):
    gid2date = {}

    lines = open(table).readlines()
    for l in lines[1:]:
        cols = l.split('\t')
        if len(cols) < 2:
            print 'Invalid row in table: %s' % (l)
            sys.exit(-1)
        gid = cols[0].strip()
        if gid == "":
            print 'Row without ID in table: %s' % (l)
            continue

        date = cols[1].strip()
        if date == "":
            print 'Row without date in table: %s' % (l)
            continue

        gid2date[gid] = datetime.datetime.strptime(date, '%b %d %Y')

    return gid2date
        

if __name__ == '__main__':
    assert(len(sys.argv) == 4), 'Usage: %s overlapFile tableFile outputFile' % (sys.argv[0])

    fp = open(sys.argv[3], 'w')
    fp.write(
"""#This file contains a list of datasets or series that have identical, and only identical, samples.
#The first dataset or series has a publication date later or equal to the second dataset or series.
""")
    fp.write('#GSE1\tGSE2\n')

    gid2date = getDates(sys.argv[2])

    lines = open(sys.argv[1]).readlines()
    for l in lines[1:]:
        parts = l.split('\t')
        gse1 = parts[0]
        gse2 = parts[1]
        matches = int(parts[2])
        nGse1 = int(parts[3])
        nGse2 = int(parts[4])

        if (matches == nGse1) and (nGse1 == nGse2):
            if not gid2date.has_key(gse1) or not gid2date.has_key(gse2):
                print 'Error: %s is duplicate of %s, but date not known for both' % (gse1, gse2)
                continue

            if gid2date[gse1] >= gid2date[gse2]:
                fp.write('%s\t%s\n' % (gse1, gse2))
            else:
                fp.write('%s\t%s\n' % (gse2, gse1))

    fp.close()
