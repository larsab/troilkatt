'''
Script to parse an Uniprot idmapping_selected.tab file to retrieve identifier and gene names.

Uniprot idDescription from idmapping_selected format
ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/idmapping/README:

[T]his tab-delimited table which includes the following mappings delimited by tab:

1. UniProtKB-AC
2. UniProtKB-ID
3. GeneID (EntrezGene)
4. RefSeq
5. GI
6. PDB
7. GO
8. IPI
9. UniRef100
10. UniRef90
11. UniRef50
12. UniParc
13. PIR
14. NCBI-taxon
15. MIM
16. UniGene
17. PubMed
18. EMBL
19. EMBL-CDS
20. Ensembl
21. Ensembl_TRS
22. Ensembl_PRO
23. Additional PubMed

This script produces the following output files:
    * <KEGG_ID>uniprot2entrez.tab: a tab delimited text file where each line has the following format: 
    
  symbol<tab>gene ID<newline>, where symbols are either UniProtKB-AC or UniProtKB-ID, and gene ID is the
  EntrezGene ID. 
'''

import sys, os

"""
Organism specific symbol postfix. In idmapping_selected.tab the UniProtKB-ID identifiers have a organism
specific postfix that is to find the mappings for a specific organism.
"""
org2ID = {
                 'xla': "_XENLA",   # Frog
                 'mmu': "_MOUSE",   # Mouse
                 'sce': "_YEAST",  # Yeast
                 'hsa': "_HUMAN",  # Human
                 'ath': "_ARATH",  # Arabidopsis
                 'rno': "_RAT",   # Rat
                 'ddi': "_DICDI", # Slime mold
                 'dme': "_DROME", # Fly
                 'cel': "_CAEEL", # Worm
                 'dre': "_DANRE", # Zebrafish
                 'spo': "_SCHPO", # Fission yeast
                 'cfa': "_CANFA", # Dog
                 'bta': "_BOVIN"} # Cow

"""
Print usage description
"""
def usage():
    print """
Usage: python parseEntrezGeneInfo.py [options]\n

Required:
    -i FILENAME    Specify idmapping_selected.tab FILENAME.
    -o DIRECTORY   Specify output DIRECTORY.
    -k KEGG_ID     Specify the KEGG ID for the organism.
    
Optional:
    -h             Display command line options.

"""

"""
Parse command line arguments. See the usage() output for the currently supported command
line arguments. Note also that most options are specified in the troilkatt.xml configuration
file.

@param args: command line arguments (sys.argv[1:])
"""
def parseArgs(argv):
    import getopt    
    
    try:
        opts, args = getopt.getopt(argv, "hi:o:k:")
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err)
        usage()
        sys.exit(2)
    
    args = {}
    for o, a in opts:
        if o == "-i":
            args['idmappingFile'] = a
        elif o == "-o":
            args['outputDirectory'] = a
        elif o == "-k":
            args['keggID'] = a
        elif o in ("-h", "--help"):
            usage()
            sys.exit()        
        else:
            raise Exception("Unhandled option")
    
    #
    # Make sure required arguments are specified
    if not args.has_key('idmappingFile'):
        print 'idmapping_selected.tab filename is not specified'
        usage()
        sys.exit(2)
    if not args.has_key('outputDirectory'):
        print 'Output directory is not specified'
        usage()
        sys.exit(2)
    if not args.has_key('keggID'):
        print 'KEGG ID is not specified'
        usage()
        sys.exit(2)
        
    # 
    # Make sure output directory exist
    #  
    if os.path.isdir(args['outputDirectory']) == False:
        print 'Output directory does not exists: ' + args['outputDirectory']
        print 'Creating new directory'
        os.makedirs(args['outputDirectory'])
        if os.path.isdir(args['outputDirectory']) == False:
            print 'Could not create output directory'
            sys.exit(-1)
            
    return args
    
if __name__ == '__main__':        
    args = parseArgs(sys.argv[1:])
    
    # Gene symbol to ID mapping
    symbol2id = {}
    
    if not org2ID.has_key(args['keggID']):    
        raise Exception("Organism ID not found for organism: %s" % (args['keggID']))
    orgID = org2ID[args['keggID']]
    
    
    #
    # Read and parse idmappings_selected file
    #
    inputFile = open(args['idmappingFile'])    
    lineCnt = 0
    orgLines = 0
    invalidGeneID = 0    
    duplicateAC = 0
    duplicateID = 0
    while 1:
        line = inputFile.readline()        
        if line == '':
            break
        
        lineCnt += 1
        # See above for file format
        parts = line.split('\t')
        if len(parts) != 23:
            print 'Warning: invalid line (%d): %s' % (lineCnt, line)
        
        # All are converted to upper case before mapping
        uniProtAC = parts[0].strip().upper()
        uniProtID = parts[1].strip().upper()
        geneID = parts[2].strip().upper()
        
        if uniProtID.find(orgID) == -1: # Does not belong to this organism
            continue
        orgLines += 1
        
        if geneID == '':
            print 'Warning: gene ID not specified for: ' + uniProtAC
            invalidGeneID += 1
            continue       
        
        if symbol2id.has_key(uniProtAC):
            print 'Warning: multiple entries for: ' + uniProtAC
            duplicateAC += 1
            continue
        symbol2id[uniProtAC] = geneID
        
        if symbol2id.has_key(uniProtID):
            print 'Warning: multiple entries for: ' + uniProtID
            duplicateID += 1
            continue
        symbol2id[uniProtID] = geneID
    inputFile.close()
                            
    #
    # Write output files
    #    
    outputFile = open(os.path.join(args['outputDirectory'], args['keggID'] + '_uniprot2entrez.tab'), 'w')
    for s in symbol2id:
        outputFile.write('%s\t%s\n' % (s, symbol2id[s]))
    outputFile.close()
    print 'Added %d mappings for %s' % (len(symbol2id), orgID)
    print 'Found %d (of %d) rows for %s' % (orgLines, lineCnt, orgID)
    print 'Errors:\n'
    print '\tMissing Entrez Gene ID: %d' % (invalidGeneID)
    print '\tDuplicate UniProt ACs: %d' % (duplicateAC)
    print '\tDuplicate UniProt IDs: %d' % (duplicateID)