#
# Parse the complete HGNC dataset file (downlaoded from www.genenames.org)
#

if __name__ == '__main__':
    import sys

    assert(len(sys.argv) == 3), 'Usage: %s input-file output-file' % (sys.argv[0])
    
    print 'Parse HGNC file %s and write output to %s' % (sys.argv[1], sys.argv[2])
    inputFile = open(sys.argv[1])
    outputFile = open(sys.argv[2], 'w')

    # Skip header line
    inputFile.readline()

    cnt = 0
    notApproved = 0
    while 1:
        line = inputFile.readline()
        if line == '':
            break
        
        cols = line.split('\t')
        if len(cols) < 8:
            print 'Incomplete line: %s' % (line)
            continue

        systematicName = cols[0]
        commonName = cols[1]
        status = cols[3].strip().lower()
        aliases = cols[8].split(',')

        if status != 'approved':
            notApproved += 1
            continue        

        newLine = '%s\t%s\t%s' % (systematicName.strip(), 
                                  commonName.strip(),
                                  aliases[0].strip())
        for a in aliases[1:]:
            newLine = newLine + '|' + a.strip()
        newLine = newLine + '\n'
        outputFile.write(newLine)        
        cnt += 1

    inputFile.close()
    outputFile.close()
        
    print '%d gene names written' % (cnt)
    print '%d not approved' % (notApproved)
