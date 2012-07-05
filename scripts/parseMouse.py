#
# Parse the mouse MRK dump file (downlaoded from ftp.informatics.jax.org/)
#
if __name__ == '__main__':
    import sys

    assert(len(sys.argv) == 3), 'Usage: %s input-file output-file' % (sys.argv[0])
    
    print 'Parse MRK_Dump1 file %s and write output to %s' % (sys.argv[1], sys.argv[2])
    inputFile = open(sys.argv[1])
    outputFile = open(sys.argv[2], 'w')

    # Skip header line
    inputFile.readline()

    s2c = {}
    s2a = {}
    while 1:
        line = inputFile.readline()
        if line == '':
            break
        
        cols = line.split('\t')
        if len(cols) < 2:
            print 'Incomplete line: %s' % (line)
            continue

        systematicName = cols[0] 
        commonName = cols[1]        

        if systematicName not in s2c:
            # Just use first name as common name
            s2c[systematicName] = commonName
        else:
            if systematicName not in s2a:
                s2a[systematicName] = [commonName]
            else:
                s2a[systematicName].append(commonName)
        
            

    cnt = 0
    ids = s2c.keys()
    ids.sort()
    for systematicName in ids:
        commonName = s2c[systematicName]
        aliases = ['']
        if systematicName in s2a:
            aliases = s2a[systematicName]
            
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
