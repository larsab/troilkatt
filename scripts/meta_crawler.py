"""
This module downloads and parses various meta-files such as gene name lists and gold standards.
To add a new meta-file to download an entry must be added to the META_FILES list in this file.
In addition it may be necessary to write a new parser function if the meta-file needs to be 
changed before it is saved.
"""

import urllib
import os

from troilkatt_crawler import TroilkattCrawler

"""
Parse the yeast gene names downloaded from yeastgenome.org to the format
used by our data processing scripts. That is convert the file to a format
that has two tab delimited columns, where the first column has the systematic
gene name, and the second has a list of aliases separated by |

@param fileLines: list of lines read from the remote file
@param dstPath: filname for destination file
@param savePath: filename for additional file where a copy is saved. The copy is then timestamped
 and stored in the Troilkatt DB. savePath can be None or the same as dstPath
"""
def parseYeastGenenames(fileLines, dstPath, savePath):
    
    print 'Parse yeast gene names and write to: ' + dstPath      
    
    # The input file maps systematic names to aliases, while the Spell tools use the reverse
    # mapping. Note that one alias can map to multiple systematic names, and that the identity
    # mapping is also needed.
    alias2systematic = {}
    
    #
    # Reverse the mapping
    #
    for l in fileLines:
        l = l.strip()
        
        # Each line has 3 up to columns: systematic name, aliases, and description
        cols = l.split("\t")
        if len(cols) == 0:
            print 'Empty line in input file is ignored'
            continue
        
        systematicName = cols[0]
        aliases = [systematicName]
        
        if len(cols) > 1:
            aliases = aliases + cols[1].split('|')
        
        for a in aliases:
            if a == '':
                continue
            
            if a in alias2systematic:
                if systematicName not in alias2systematic[a]:
                    alias2systematic[a].append(systematicName)
                # else: mapping already exists
            else:
                alias2systematic[a] = [systematicName]
    
    #
    # Write to output files
    #
    of = open(dstPath, 'w')
    
    if dstPath == savePath:
        savePath = None
    sf = None
    if savePath != None:
        sf = open(savePath, 'w')   
        
    keys = alias2systematic.keys()
    keys.sort()
    for a in keys:
        # Each alias has at least the identity mapping
        outputLine = '%s\t%s' % (a, alias2systematic[a][0])
        
        for s in alias2systematic[a][1:]:
            outputLine = outputLine + '|%s' % (s) 
        
        outputLine = outputLine + '\n'
        
        of.write(outputLine)
        if sf != None:
            sf.write(outputLine)   
        
    of.close()
    if sf != None:
        sf.close()
        
"""
Parse the yeast gene names downloaded from yeastgenome.org to the format
used by our data processing scripts. That is convert the file to a format
that has two tab delimited columns, where the first column has the systematic
gene name, and the second has a list of aliases separated by |

@param fileLines: list of lines read from the remote file
@param dstPath: filname for destination file
@param savePath: filename for additional file where a copy is saved. The coipy is then timestamped
 and stored in the Troilkatt DB. savePath can be None or the same as dstPath
"""
def parseYeastGenenames2(fileLines, dstPath, savePath):
    
    print 'Parse yeast gene names and write to: ' + dstPath      
    
    
    
    # The input file maps systematic names to aliases, while the Spell tools use the reverse
    # mapping. Note that one alias can map to multiple systematic names, and that the identity
    # mapping is also needed.
    alias2systematic = {}
    
    #
    # Reverse the mapping
    #
    for l in fileLines:        
        if l[0] == '!': # is comment
            continue        
    
        #
        #    Columns within SGD_features.tab:
        #
        # 1.  Primary SGDID (mandatory)
        # 2.  Feature type (mandatory)
        # 3.  Feature qualifier (optional)
        # 4.  Feature name (optional)
        # 5.  Standard gene name (optional)
        # 6.  Alias (optional, multiples separated by |)
        # 7.  Parent feature name (optional)
        # 8.  Secondary SGDID (optional, multiples separated by |)
        # 9.  Chromosome (optional)
        # 10. Start_coordinate (optional)
        # 11. Stop_coordinate (optional)
        # 12. Strand (optional)
        # 13. Genetic position (optional)
        # 14. Coordinate version (optional)
        # 15. Sequence version (optional)
        # 16. Description (optional)
        #
        
        cols = l.split("\t")
        if len(cols) != 16:
            print 'Invalid column: (%d columns): %s' % (len(cols), cols)
            continue
        
        systematicName = cols[4]
        aliases = [systematicName] + cols[5].split('|')                
        
        for a in aliases:
            if a == '':
                continue
            
            if a in alias2systematic:
                if systematicName not in alias2systematic[a]:
                    alias2systematic[a].append(systematicName)
                # else: mapping already exists
            else:
                alias2systematic[a] = [systematicName]
    
    #
    # Write to output files
    #
    of = open(dstPath, 'w')
    
    if dstPath == savePath:
        savePath = None
    sf = None
    if savePath != None:
        sf = open(savePath, 'w')   
        
    keys = alias2systematic.keys()
    keys.sort()
    for a in keys:
        # Each alias has at least the identity mapping
        outputLine = '%s\t%s' % (a, alias2systematic[a][0])
        
        for s in alias2systematic[a][1:]:
            outputLine = outputLine + '|%s' % (s) 
        
        outputLine = outputLine + '\n'
        
        of.write(outputLine)
        if sf != None:
            sf.write(outputLine)   
        
    of.close()
    if sf != None:
        sf.close()
    
"""
Default parser: just write all lines to the outputfile unmodified

@param fileLines: list of lines read from the remote file
@param dstPath: filname for destination file
@param savePath: filename for additional file where a copy is saved. The coipy is then timestamped
 and stored in the Troilkatt DB. savePath can be None or the same as dstPath
"""
def parseDefault(fileLines, dstPath, savePath):
    print 'Write all lines to' + dstPath
    of = open(dstPath, 'w')
    
    if dstPath == savePath:
        savePath = None
    sf = None
    if savePath != None:
        sf = open(savePath, 'w')
    
    for l in fileLines:
        of.write(l)
        if sf != None:
            sf.write(l) 
                
    of.close()
    sf.close()
    
# List of files to download, their local filesystem name, and the function used to parse/modify 
# the file
#
# Each entry in the list is a dictionary with the following keys:
# source: URL of remote file to download
# parser: function used to parse the downloaded file
# output filename: filename for downlaoded and parsed file on local filesystem
# local directory: directory where the output file is saved
# description: optional description (that is not used for anything)
#
META_FILES = [
              {'source': 'http://downloads.yeastgenome.org/gene_registry/registry.genenames.tab',
               'output filename': 'named_yeast_genes.tab',
               'local directory': 'TROILKATT.DATA_DIR',               
               'parser': parseYeastGenenames,
               'description': 'Systematic to alias mapping of yeast gene names'},
                {'source': 'ftp://genome-ftp.stanford.edu/yeast/data_download/chromosomal_feature/SGD_features.tab',
               'output filename': 'all_yeast_genes.tab',
               'local directory': 'TROILKATT.DATA_DIR',               
               'parser': parseYeastGenenames2,
               'description': 'Systematic to alias mapping of yeast gene names'}
              ]

'''
Meta crawler. Download diverse meta-data files such as gene names, 
gold standards etc.

The list of metafiles to download is defined by the above toDownload data structure.
'''
class MetaCrawler(TroilkattCrawler):
    """
    Constructor.
    """
    def __init__(self):
        TroilkattCrawler.__init__(self)
    
    """
    Read a remote file using various protocols. The file content is returned
    since it often requires some processing before being stored in the format
    used by our various tools and scripts
    
    @param src: URL to read
    @return: file content as list of string with one lien per string
    """
    def readFile(self, src):
        rf = urllib.urlopen(src)    
        lines = rf.readlines()
        rf.close()
        
        return lines

    """
    Main loop
    """
    def crawl(self, inputObjects):
        for d in META_FILES:            
            lines = self.readFile(d['source'])
            func = d['parser']
            dstPath = os.path.join(self.setTroilkattVariables(d['local directory']),
                                   d['output filename'])
            savePath = os.path.join(self.logDir, d['output filename'])
            func(lines, dstPath, savePath)
    

"""
Do a geo query

Command line arguments: %s tmp output log <args>, where
    tmp: tmp directory
    output: output directory
    log: logfile directory
        
The decription in usage() has additional details. 
"""
if __name__ == '__main__':
    c = MetaCrawler()
    c.mainRun()