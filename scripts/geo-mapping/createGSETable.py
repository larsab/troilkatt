import sys, os

def getValue(line):
    parts = l.split('=')
    if len(parts) == 2:
        val = parts[1].strip()
        return val
    else:
        return None

def appendValueList(line, list):
    parts = l.split('=')
    if len(parts) == 2:
        val = parts[1].strip()
        items = val.split(',')
        for i in items:
            if i not in list:
                list.append( i.strip() )

if __name__ == '__main__':
    assert(len(sys.argv) == 3), 'Usage: python %s metaDir outputFile' % (sys.argv[0])

    metaDir = sys.argv[1]
    files = os.listdir(metaDir)
    files.sort()
    fout = open(sys.argv[2], 'w')
    fout.write('GSE\tDate\tOrganisms\tPlatforms\tGSMs\n')

    for f in files:
        lines = open(os.path.join(metaDir, f)).readlines()
        gid = f.split('_')[0]
        date = None
        organisms = []
        platforms = []
        platformTitles = []
        gsms = []
        
        for l in lines:
            if l.find('!Series_submission_date') == 0:
                date = getValue(l)            
            elif l.find('!Platform_organism') == 0:
                appendValueList(l, organisms)            
            elif l.find('!Series_platform_id') == 0:
                appendValueList(l, platforms)            
            elif l.find('!Platform_title') == 0:
                appendValueList(l, platformTitles)            
            elif l.find('!Sample_geo_accession') == 0:
                appendValueList(l, gsms)
                
        fout.write(gid + '\t')
        if date != None:
            fout.write(date)    
        fout.write('\t')
        for i in range(len(organisms)):
            if i == 0:
                fout.write(organisms[i])
            else:
                fout.write(',' + organisms[i])
        fout.write('\t')
        for i in range(len(platforms)):
            if i == 0:
                fout.write(platforms[i])
            else:
                fout.write(',' + platforms[i])
        fout.write('\t')        
        for i in range(len(gsms)):
            if i == 0:
                fout.write(gsms[i])
            else:
                fout.write(',' + gsms[i])
        fout.write('\t')
        for i in range(len(platformTitles)):
            if i == 0:
                fout.write(platformTitles[i])
            else:
                fout.write(',' + platformTitles[i])
        fout.write('\n')

    fout.close()
