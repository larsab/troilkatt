#
# Split a gene names file into: 
# - Systematic names
# - Alias to systematic name mappings
# - Systematic to common name mapping
#

if __name__ == '__main__':
    import sys, os

    assert(len(sys.argv) == 4), 'Usage: %s gene-names-file organism output-dir' % (sys.argv[0])

    geneNamesFilename = sys.argv[1]
    organism = sys.argv[2]
    outputDir = sys.argv[3]
    systematicFilename = os.path.join(outputDir, '%s_systematic_names.tab' % (organism))
    aliasFilename = os.path.join(outputDir, '%s_alias_to_systematic.tab' % (organism))
    commonFilename = os.path.join(outputDir, '%s_systematic_to_common.tab' % (organism))
    mapFilename = os.path.join(outputDir, '%s.map' % (organism))

    geneNamesFile = open(geneNamesFilename)
    systematicFile = open(systematicFilename, 'w')
    aliasFile = open(aliasFilename, 'w')
    commonFile = open(commonFilename, 'w')
    mapFile = open(mapFilename, 'w')
    
    alias2systematic = {}
    while 1:
        line = geneNamesFile.readline()
        if line == '':
            break

        cols = line.split('\t')
        systematicName = cols[0].strip()
        commonName = cols[1].strip()            
        aliases = cols[2].split('|')
                
        systematicFile.write('%s\n' % (systematicName))
        commonFile.write('%s\t%s\n' % (systematicName, commonName))
        mapFile.write('%s\t%s\n' % (systematicName, systematicName))

        if commonName not in alias2systematic:
            alias2systematic[commonName] = [systematicName]
        else:
            alias2systematic[commonName].append(systematicName)

        for a in aliases:
            a = a.strip()
            if a != '':
                if a not in alias2systematic:
                    alias2systematic[a] = [systematicName]
                else:
                    alias2systematic[a].append(systematicName)
    
    aliasKeys = alias2systematic.keys()
    aliasKeys.sort()
    for a in aliasKeys:
        newLine = '%s\t%s' % (a, alias2systematic[a][0])
        mapFile.write("%s\t%s\n" % (a, alias2systematic[a][0]))
        for n in alias2systematic[a][1:]:
            newLine = newLine + '|' + n
            mapFile.write("%s\t%s\n" % (a, n))
        aliasFile.write('%s\n' % (newLine))        
        

    geneNamesFile.close()
    systematicFile.close()
    aliasFile.close()
    commonFile.close()
    mapFile.close()
