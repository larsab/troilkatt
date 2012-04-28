"""
Compare two lists of ids
"""

import sys

if __name__ == '__main__':                                                                                                                           
    assert(len(sys.argv) == 3), 'Usage: python idlistCmp file1 file2'                                                                      
                                                                                                                                                     
    table1 = sys.argv[1]                                                                                                                             
    table2 = sys.argv[2]   
                                                                                                                                                     
    ids1 = set()
    ids2 = set()
                
    fp1 = open(table1)                                                                                                                                                                                                                                                                                   
    rows1 = fp1.readlines()  
    for r in rows:
        ids1.add(r.strip())
    fp1.close()
     
    fp2 = open(table2)                                                                                                                         
    rows2 = fp2.readlines()   
    for r in rows:
        ids2.add(r.strip())
    fp2.close()                                                                                                                       
    
    print 'Ids only in %s:' % (table1)
    for i in ids1:
        if i not in ids2:
            print i
    
    print 'Ids only in %s:' % (table2)
    for i in ids2:
        if i not in ids1:
            print i