import sys, datetime
from findDuplicates import getDates

if __name__ == '__main__':
    assert(len(sys.argv) == 6), 'Usage: %s overlapFile tableFile mergesOutputFile splitsOutputFile neitherFile' % (sys.argv[0])

    supersets = {}       # key: super ID, values: list of (sub ID, sub nSamples)
    supersetSamples = {} # number of samples in superset

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
        elif matches == nGse1:
            if supersets.has_key(gse2):
                supersets[gse2].append( (gse1, nGse1) )            
            else:
                supersets[gse2] = [ (gse1, nGse1) ]
                supersetSamples[gse2] = nGse2
        elif matches == nGse2:
            if supersets.has_key(gse2):
                supersets[gse1].append( (gse2, nGse2) )
            else:
                supersets[gse1] = [ (gse2, nGse2) ]
                supersetSamples[gse1] = nGse1

    dates = getDates(sys.argv[2])

    merges = open(sys.argv[3], 'w')
    merges.write(
"""#This file contains a list of datasets or series where dataset/series A has all samples in B, C...N, and A
#has the newsert publication date
#
#Superset ID\tSuperset samples\tSubsets...
""")

    splits = open(sys.argv[4], 'w')
    splits.write(
"""#This file contains a list of datasets or series where dataset/series A has all samples in B, C...N, and A
#has the oldest publication date
#
#Superset ID\tSuperset samples\tSubsets...
""")

    neither = open(sys.argv[5], 'w')
    neither.write(
"""#This file contains a list of datasets or series where dataset/series A has all samples in B, C...N, and A
#has a publication date that is neither the newst nor oldest of all publications dates.
#
#Superset ID\tSuperset samples\tSubsets...
""")

    for s in supersets.keys():
        superOldest = True # True if superset is older than all subsets
        superNewest = True # True if superset is newer than all subsets
        supersetDate = dates[s]

        nSub = 0
        for t in supersets[s]:
            gid, cnt = t
            nSub += cnt

            subsetDate = dates[gid]
            if subsetDate > supersetDate:   # subset is newer
                superNewest = False
            elif subsetDate < supersetDate: # subset is older
                superOldest = False
        
        if nSub == supersetSamples[s]:
            if superNewest: # superset merged all subsets
                merges.write('%s\t%d' % (s, supersetSamples[s]))
                for t in supersets[s]:
                    id, cnt = t
                    merges.write('\t%s' % (id))
                merges.write('\n')
            elif superOldest: # superset was split into subsets
                splits.write('%s\t%d' % (s, supersetSamples[s]))
                for t in supersets[s]:
                    id, cnt = t
                    splits.write('\t%s' % (id))
                splits.write('\n')
            else:
                neither.write('%s\t%d' % (s, supersetSamples[s]))
                for t in supersets[s]:
                    id, cnt = t
                    neither.write('\t%s' % (id))
                neither.write('\n')

    merges.close()
    splits.close()
    neither.close()
