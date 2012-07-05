import sys, datetime
from findDuplicates import getDates

if __name__ == '__main__':
    assert(len(sys.argv) == 5), 'Usage: %s overlapFile tableFile outputFileOlder outputFileNewer' % (sys.argv[0])

    dates = getDates(sys.argv[2])

    fp1 = open(sys.argv[3], 'w')
    fp1.write("""#Datasets or series where all samples in series/dataset A are in series/dataset B, and the publication
#date of A is older than B.
#GSID 1\tGSID 2\tSamples in GSID
""")

    fp2 = open(sys.argv[4], 'w')
    fp2.write("""#Datasets or series where all samples in series/dataset A are in series/dataset B, and the publication
#date of A is newer than B.
#GSID 1\tGSID 2\tOverlapping samples.
""")

    lines = open(sys.argv[1]).readlines()
    for l in lines[1:]:
        parts = l.split('\t')
        gse1 = parts[0]
        gse2 = parts[1]
        matches = int(parts[2])
        nGse1 = int(parts[3])
        nGse2 = int(parts[4])

        if (matches == nGse1) and (nGse1 == nGse2):
            continue # is duplicate
        elif matches == nGse1: # GSE1 is subset of GSE2
            if dates[gse1] <= dates[gse2]:
                fp1.write('%s\t%s\t%d\n' % (gse1, gse2, matches))
            else:
                fp2.write('%s\t%s\t%d\n' % (gse1, gse2, matches))
        elif matches == nGse2:
            if dates[gse2] <= dates[gse1]:
                fp1.write('%s\t%s\t%d\n' % (gse2, gse1, matches))
            else:
                fp2.write('%s\t%s\t%d\n' % (gse2, gse1, matches))

    fp1.close()
    fp2.close()
