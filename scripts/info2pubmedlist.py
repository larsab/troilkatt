'''
Parse a Spell .info file to retrieve: 
1. A list of filenames -> PubMed ID's (filename<tab>pubmedID per line)
2. A list of filenames (one filename per line)
'''

def createPubmedList(infoFilename, outputFilename):
    # Info file columns
    #  0. File
    #  1. DatasetID
    #  2. Organism
    #  3. Platform
    #  4. ValueType
    #  5. #channels
    #  6. Title
    #  7. Description
    #  8. PubMedID
    #  9. #features
    # 10. #samples
    # 11. date
    # 12. Min
    # 13. Max
    # 14. Mean
    # 15. #Neg
    # 16. #Pos
    # 17. #Zero
    # 18. #MV
    # 19. #Total
    # 20. #Channels
    # 21. logged
    # 22. zerosAreMVs
    # 23. MVcutoff
    
    fp = open(infoFilename)
    lines = fp.readlines()
    fp.close()
    
    of = open(outputFilename, 'w')
    
    for l in lines:
        if l[0] == '#': # is comment
            continue        
        
        cols = l.split('\t')
        if len(cols) != 24:
            print 'Invalid columns count for line: ' + l
        else:            
            of.write('%s\t%s\n' % (cols[0], cols[8]))
            
    of.close()

def createFilelist(infoFilename, outputFilename):
    # First column in .info file is filename
    fp = open(infoFilename)
    lines = fp.readlines()
    fp.close()
    
    of = open(outputFilename, 'w')
    
    for l in lines:
        if l[0] == '#': # is comment
            continue        
        
        cols = l.split('\t')
        if len(cols) != 24:
            print 'Invalid columns count for line: ' + l
        else:            
            of.write('%s\n' % (cols[0]))
            
    of.close()

"""
Arguments:
[0]: .info file with all filenames in the compendia
[1]: pubmed filename
[2]: filelist filename
"""
if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 3:
        print 'Usage: python %s info-file filelist-file pubmed-file' % (sys.argv[0]) 
        exit(-1)
    
    infoFilename = sys.argv[1]
    pubmedFilename = sys.argv[2]
    filelistFilename = sys.argv[3]
    createPubmedList(infoFilename, pubmedFilename)
    createFilelist(infoFilename, filelistFilename)
    print 'Done'