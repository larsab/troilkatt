import sys, os

# Get list for comma seperated string
def getList(col):
    parts = col.split(',')
    rv = []
    for p in parts:
        v = p.strip().upper()
        if v != "":
            rv.append(v)
    return rv

# Get list for comma seperated string
# Do not cast to uppercase
def getList2(col):
    parts = col.split(',')
    rv = []
    for p in parts:
        v = p.strip()
        if v != "":
            rv.append(v)
    return rv

# Add values to a list in the dict, or create a new list
def addToList(m, k, vals):
    for v in vals:
        if m.has_key(k):
            if v not in m[k]:
                m[k].append(v)
        else:
            m[k] = [v]

# Add values to a list in the dict, or create a new list
def addToList2(m, keys, v):
    for k in keys:
        if m.has_key(k):
            if v not in m[k]:
                m[k].append(v)
        else:
            m[k] = [v]

# Create mapping file
def writeFile(m, filename):
    fp = open(filename, 'w')
    for k in m.keys():
        fp.write('%s' % (k))
        for v in m[k]:
            fp.write('\t%s' % (v))
        fp.write('\n')
    fp.close()


#
# Create mapping files
#
if __name__ == '__main__':
    assert(len(sys.argv) == 4), 'Usage: %s gdsTable gseTable gplTable' % (sys.argv[0])
    inputDir = sys.argv[1]

    gse2gsm = {} # key: gse, value: list of gsm's in gse
    gsm2gse = {} # key: gsm, value: list of gse's with that gsm
    org2gse = {}    
    gpl2gse = {}
    gse2gpl = {}
    gse2gplTitle = {}

    gds2gse = {}
    gse2gds = {}    
    gds2gsm = {}
    gsm2gds = {}
    org2gds = {}
    gpl2gds = {}
    gds2gpl = {}
    gds2gplTitle = {}
    

    # Parse GDS Table
    lines = open(sys.argv[1]).readlines()
    for l in lines[1:]:
        cols = l.split('\t')
        if len(cols) != 8:
            print 'Invalid row in GDS table: %s' % (l)
            print 'Expected 8 cols, found %d cols' % (len(cols))
            sys.exit(-1)

        gds = cols[0].strip().upper()
        if gds == None:
            print 'No GDS in row: %s' % (l)
            sys.exit(-1)

        org = cols[2].strip()
        gpl = cols[3].strip().upper()
        gses = getList(cols[6])
        gsms = getList(cols[7])
        
        if len(gses) == 0:
            print 'Error: No GSE mapping for: %s' % (gds)
        else:
            gds2gse[gds] = gses
            addToList(gse2gds, gds, gses)        

        if len(gsms) == 0:
            print 'Error: No GSMs for: %s' % (gds)
        else:
            gds2gsm[gds] = gsms
            addToList2(gsm2gds, gsms, gds)
        
        if org == "":
            print 'Warning: No organism for: %s' % (gds)
        else:
            addToList(org2gds, org, [gds])
        
        if gpl == "":
            print 'Warning: No GPL for: %s' % (gds)
        else:
            addToList(gpl2gds, gpl, [gds])
            addToList(gds2gpl, gds, [gpl])

    writeFile(gds2gse, 'gds2gse.map')
    writeFile(gse2gds, 'gse2gds.map')
    writeFile(gds2gsm, 'gds2gsm.map')
    writeFile(gsm2gds, 'gsm2gds.map')
    writeFile(org2gds, 'org2gds.map')
    writeFile(gpl2gds, 'gpl2gds.map')

    # Parse GSE Table
    lines = open(sys.argv[2]).readlines()
    for l in lines[1:]:
        cols = l.split('\t')
        if len(cols) != 6:
            print 'Invalid row in GSE table: %s' % (l)
            sys.exit(-1)
        gse = cols[0].strip().upper()
        if gse == "":
            print 'No GSE ID in row: %s' % (l)
            sys.exit()
        orgs = getList2(cols[2])
        gpls = getList(cols[3])
        gsms = getList(cols[4])
        gplTitles = getList2(cols[5])

        if len(gsms) == 0:
            print 'Error: no GSMs for: %s' % (gse)
        else:
            gse2gsm[gse] = gsms
            addToList2(gsm2gse, gsms, gse)
        
        if org == "":
            print 'Warning no organisms for: %s' % (gse)
        else:
            addToList2(org2gse, orgs, gse)
        
        if len(gpls) == 0:
            print 'Warning no GPLs for: %s' % (gse)
        else:
            addToList2(gpl2gse, gpls, gse)
            addToList(gse2gpl, gse, gpls)

        if len(gplTitles) == 0:
            print 'Warning no GPL Title for: %s' % (gse)
            addToList(gse2gplTitle, gse, ['Unknown'])
        else:
            addToList(gse2gplTitle, gse, gplTitles)

    writeFile(gse2gsm, 'gse2gsm.map')
    writeFile(gsm2gse, 'gsm2gse.map')
    writeFile(org2gse, 'org2gse.map')
    writeFile(gpl2gse, 'gpl2gse.map')

    # Parse GPL Table
    lines = open(sys.argv[3]).readlines()
    gpl2title = {}
    for l in lines[1:]:
        cols = l.split('\t')
        if len(cols) != 4:
            print 'Invalid row in GSE table: %s' (l)
            sys.exit(-1)
        gpl = cols[0].strip().upper()
        if gpl == "":
            print 'No GPL ID in row: %s' % (l)
        title = cols[3].strip()

        gpl2title[gpl] = title
        
    for gds in gds2gpl.keys():
        gpls = gds2gpl[gds]
        for gpl in gpls:
            if gpl in gpl2title.keys():
                addToList(gds2gplTitle, gds, [gpl2title[gpl]])
            else:
                addToList(gds2gplTitle, gds, ['Unknown'])

    writeFile(gds2gplTitle, 'gds2gplTitle.map')
    writeFile(gse2gplTitle, 'gse2gplTitle.map')
    
