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
    fout.write('GDS\tDate\tOrganism\tPlatform\tSampels\tGenes\tGSEs\tGSMs\n')

    for f in files:
        lines = open(os.path.join(metaDir, f)).readlines()
        gid = f.split('.')[0]
        date = None
        organsim = None
        platform = None
        samples = None
        genes = None
        gses = []
        gsms = []
        
        for l in lines:
            if l.find('!dataset_update_date') == 0:
                date = getValue(l)
            elif l.find('!dataset_reference_series') == 0:
                appendValueList(l, gses)
            elif l.find('!dataset_sample_organism') == 0:
                organism = getValue(l)
            elif l.find('!dataset_sample_count') == 0:
                samples = int(getValue(l))
            elif l.find('!dataset_feature_count') == 0:
                genes = int(getValue(l))
            elif l.find('!dataset_platform ') == 0:
                platform = getValue(l)
            elif l.find('!subset_sample_id') == 0:
                appendValueList(l, gsms)
            
        assert(samples == len(gsms)), 'In %s: %d != %d' % (gid, samples, len(gsms))
                
        fout.write(gid + '\t')
        if date != None:
            fout.write(date)    
        fout.write('\t')
        if organism != None:
            fout.write(organism)
        fout.write('\t')
        if platform != None:
            fout.write(platform)
        fout.write('\t')
        if samples != None:
            fout.write(str(samples))
        fout.write('\t')
        if genes != None:
            fout.write(str(genes))
        fout.write('\t')
        for i in range(len(gses)):
            if i == 0:
                fout.write(gses[i])
            else:
                fout.write(',' + gses[i])
        fout.write('\t')
        for i in range(len(gsms)):
            if i == 0:
                fout.write(gsms[i])
            else:
                fout.write(',' + gsms[i])
        fout.write('\n')

    fout.close()
