
"""
Compare two lists of ids
"""

import sys

if __name__ == '__main__':                                                                                                                           
    assert(len(sys.argv) == 3), 'Usage: python idlistCmp file1 file2'                                                                      
                                                                                                                                                     
    file1 = sys.argv[1]                                                                                                                             
    file2 = sys.argv[2]   
                                                                                                                                                     
    ids1 = set()
    ids2 = set()
                
    fp1 = open(file1)                                                                                                                                                                                                                                                                                   
    rows1 = fp1.readlines()  
    for r in rows1:
        ids1.add(r.strip())
    fp1.close()
     
    fp2 = open(file2)                                                                                                                         
    rows2 = fp2.readlines()   
    for r in rows2:
        ids2.add(r.strip())
    fp2.close()                                                                                                                       
    
    print 'Ids only in %s:' % (file1)
    for i in ids1:
        if i not in ids2:
            print i
    
    print 'Ids only in %s:' % (file2)
    for i in ids2:
        if i not in ids1:
            print i