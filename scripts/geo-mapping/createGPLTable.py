import sys, os

def getValue(line):
    parts = l.split('=')
    if len(parts) == 2:
        val = parts[1].strip()
        return val
    else:
        return None

if __name__ == '__main__':
    assert(len(sys.argv) == 3), 'Usage: python %s metaDir outputFile' % (sys.argv[0])

    metaDir = sys.argv[1]
    files = os.listdir(metaDir)
    fout = open(sys.argv[2], 'w')
    fout.write('GPL\tDate\tOrganism\tPlatform title\n')

    for f in files:
        lines = open(os.path.join(metaDir, f)).readlines()
        gid = f.split('.')[0]
        date = None
        organism = None
        title = None
        
        for l in lines:
            if l.find('!Annotation_date') == 0:
                date = getValue(l)            
            elif l.find('!Annotation_platform_organism') == 0:
                organism = getValue(l)
            elif l.find('!Annotation_platform_title') == 0:
                title = getValue(l)                      
            elif l.find('!Sample_geo_accession') == 0:
                appendValueList(l, gsms)
                
        fout.write(gid + '\t')
        if date != None:
            fout.write(date)    
        fout.write('\t')
        if organism != None:
            fout.write(organism)    
        fout.write('\t')
        if title != None:
            fout.write(title)            
        fout.write('\n')

    fout.close()
