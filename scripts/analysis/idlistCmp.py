import sys

if __name__ == '__main__':                                                                                                                           
    assert(len(sys.argv) == 4), 'Usage: python tableCmp table1 table2 accuracy'                                                                      
                                                                                                                                                     
    table1 = sys.argv[1]                                                                                                                             
    table2 = sys.argv[2]   
    accuracy = 0
    try:                                                                                                                           
        accuracy = float(sys.argv[3])
    except ValueError, e:
        print 'Invalid accuracy argument, not a floating point number: %s' % (sys.argv[3])      
        sys.exit(2)                                                                                                                  
                                                                                                                                                     
    fp1 = open(table1)                                                                                                                               
    fp2 = open(table2)                                                                                                                               
                                                                                                                                                     
    row = 0                                                                                                                                          
    rows1 = fp1.readlines()                                                                                                                          
    rows2 = fp2.readlines()                                                                                                                          
    
    nRows1 = len(rows1)
    nRows2 = len(rows2)                                                                                                                                     
    if nRows1 != nRows2:                                                                                                                     
        print 'Table dimensions differ: table 1 has %d rows, while table 2 ha %d rows' % (nRows1, nRows2)                                    
        sys.exit(0)                                                                                                                                  
                                                                                                                                                     
    nCols1 = len(rows1[0].split('\t'))                                                                                                               
    nCols2 = len(rows2[0].split('\t'))                                                                                                               
    if nCols1 != nCols2:
        print 'Table dimensions differ: table 1 had %d columns, while table 2 has %d columns' % (ncols1, nCols2)
        sys.exit(0)
    
    totalValues = nRows1 * nCols1
    invalidValues = 0
    comparedValues = 0
    allEquals = True
    for i in range(nRows1):
        rowDiffers = False
        cols1 = rows1[i].split('\t')
        cols2 = rows2[i].split('\t')
        
        if (len(cols1) != len(cols2)):
            rowDiffers = True
        else:
            for j in range(len(cols1)):                            
                v1 = None
                invalidV1 = False
                try: 
                    v1 = float(cols1[j])
                except ValueError, e:
                    invalidV1 = True
                    
                v2 = None
                invalidV2 = False
                try: 
                    v2 = float(cols2[j])
                except ValueError, e:
                    invalidV2 = True
                    
                if invalidV1 and invalidV2:
                    invalidValues += 1
                    # Neither is a number so the values are compared directly
                    if cols1[j] != cols2[j]:
                        rowDiffers = True
                        break
                    else:
                        continue
                elif invalidV1 or invalidV2:
                    # Only one table has an invalid number            
                    rowDiffers = True
                    break
                
                try:
                    comparedValues += 1
                    if v2 > v1 + accuracy or v2 < v1- accuracy:
                        # Not within accepted range
                        rowDiffers = True
                        break
                except TypeError, e:
                    print e
                    print v1
                    print v2                
                    print accuracy
                    print invalidV1
                    print invalidV2
                    sys.exit(2)
                          
        if rowDiffers:
            print 'Row %d differs' % (i)
            print 'Table1 < %s' % (rows1[i])
            print 'Table2 > %s' % (rows2[i])
            allEqual = False
    
    if allEquals:
        print 'Tables have all values within the acceptable range'
        print 'Of %d values %d where invalid numbers and %s where compared' % (totalValues, invalidValues, comparedValues)
