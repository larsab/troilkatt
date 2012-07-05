'''
Print various statistics about a PCL file
'''

if __name__ == '__main__':
    import sys, os
    
    assert(len(sys.argv) == 2), 'Usage: %s directory' % (sys.argv[0])
    
    files = os.listdir(sys.argv[1])
    
    rows = 0
    columns = 0
    
    for f in files:
        if f[-4:] != '.pcl':
            continue
        
        fp = open(f)
        lines = fp.readlines()
        fp.close()
        
        if len(lines) == 0:
            continue
        
        firstLine = lines[0]
        lineCols = firstLine.split('\t')
        
        # First 3 columns are systematic gene name, common name, and GWEIGHT
        myCols = len(lineCols) - 3
        myRows = len(lines) - 2 
        
        columns = columns + myCols
        
        # First 2 columns are the header and the EWEIGHT row
        rows = rows + myRows
        
        print '%s: %d cols and %d rows' % (f, myCols, myRows)
        
    print 'Total rows: %d' % (rows)
    print 'Total columns: %d' % (columns)
        