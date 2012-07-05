'''
Create the files necessary to run the Spell search system
'''

"""
Create a script to downlaod all files from the filelist

@param filelist: list with datasets to download
@param outputFilename: name of script file
"""
def createDownloadScript(fileList, outputFilename):
    fp = open(fileList)                                                                                     
    lines = fp.readlines()                                                    
    fp.close()                                                                                                
                                                                                                              
    out = open(outputFilename, 'w')
                                                                                                      
    for l in lines:                                                                                           
        gseId = l.split('/')[-1].split('.')[0]                                                                                                                                                                                               
        cmd = 'hadoop fs -get %s %s.final.pcl' % (l.strip(), gseId)
        out.write(cmd + '\n')
        
    out.close()
        

"""
Parse .info file to create Pubmed references file.
@param filelist: list with dataset files to download, that is the datasets for which the pubmed 
 reference should be found.
@param infoFile: the .info file creatd when converting the SOFT file to PCL. This file contains the
 pubmed references for all datasets.
@param outputFile: filename of the pubmed filename list to create

@return: none
"""
def parsePubmed(filelist, infoFile, outputFile):
    fp = open(filelist)                                                                                     
    lines = fp.readlines()                                                    
    fp.close()                                                                                                
                                                                                                              
    gds = []                                                                                                  
    for l in lines:                                                                                           
        id = l.split('/')[-1]                                                                                 
        id = id.split('.')[0]                                                                                                                                                                                               
        gds.append(id)                                                                                        
                                                                                                              
    fp = open(infoFile)                                                                                   
    lines = fp.readlines()                                                                                    
    fp.close()                                                                                                
                                                                                                              
    id2pm = {}                                                                                                
    for l in lines:                                                                                           
        cols = l.split('\t')                                                                                  
        id2pm[cols[1]] = cols[8]                                                                              
                                                                                                              
    of = open(outputFile, 'w')                                                                              
    for id in gds:                                                                                            
        of.write('%s.pcl.info\t%s\n' % (id, id2pm[id]))                                                       
    of.close()                        

"""
Create a genelist file, that is a file where the format is:
        1<tab>gene-name-1<newline>
        2<tab>gene-name-2<newline>
        ...
        N<tab>gene-name-N<newline

@param geneNameFile: filename for a file with a list of systematic gene names for this organism.
@param geneListFile: filename for the output file.
"""
def createGeneListFile(geneNameFile, geneListFile):
    fp = open(geneNameFile)                                                                                     
    lines = fp.readlines()                                                    
    fp.close()
    
    out = open(geneListFile, 'w')
    
    id = 1 # Gene ID's starts from 1 (and not zero)
    lines.sort()
    for l in lines:
        out.write('%d\t%s\n' % (id, l.strip()))
        id += 1
        
    out.close()
    

if __name__ == '__main__':
    import sys
    import os, shutil
    
    assert(len(sys.argv) == 6), 'Usage: %s sink-name output-dir tmp-dir systematic-gene-name-file organism' % (sys.argv[0])
    
    # Specifies the organism
    sinkName = sys.argv[1]
    # Where the final output files are stored
    outputDir = sys.argv[2]
    # Directory for temporary files downloaded from Troilkatt
    tmpDir = sys.argv[3]
    # Systematic file names
    geneNameFile = sys.argv[4]
    # Organism compendium is created for (used for the naming)
    organism = sys.argv[5]
    
    #
    # 1. Create filelist with HDFS filenames, and a script to copy these from HDFS to a
    # local filesystem
    #
    print 'Get filelist from Troilkatt'
    downloadFilesListCmd = 'java -Xmx2048m -classpath $TROILKATT_HOME/bin:$HBASE_HOME/hbase-0.20.0.jar:$HADOOP_HOME/hadoop-0.20.0-ant.jar:$HADOOP_HOME/hadoop-0.20.0-core.jar:$HADOOP_HOME/hadoop-0.20.0-tools.jar:$HOME/lib/java/commons-logging.jar:$HOME/lib/java/commons-cli.jar:$HOME/lib/java/gnu-getopt.jar:$HBASE_CONF:$ZOOKEEPER_HOME/zookeeper-3.2.1.jar:$HOME/lib/java/log4j-1.2.jar:$HADOOP_CONF edu.princeton.function.troilkatt.clients.SinkViewer download %s %s -f all -c $TROILKATT_HOME/conf/troilkatt.xml' % (sinkName, tmpDir)
    print 'Execute: %s' % (downloadFilesListCmd)
    os.system(downloadFilesListCmd)
    fileList = os.path.join(outputDir, '%s.files' % (organism))
    downloadScript = os.path.join(outputDir, 'download.%s.files.sh' % (organism))
    print 'Save filelist as: %s' % (fileList) 
    shutil.move(os.path.join(tmpDir, os.path.join(sinkName, 'all')), fileList)
    print 'Create download script as: %s' % (fileList)
    createDownloadScript(fileList, downloadScript)        
    
    #
    # 2. Create a .info file containing the meta-information for the files in the filelist
    #
    print 'Download .info file from Troilkatt'
    downloadInfoCmd = 'java -Xmx2048m -classpath $TROILKATT_HOME/bin:$HBASE_HOME/hbase-0.20.0.jar:$HADOOP_HOME/hadoop-0.20.0-ant.jar:$HADOOP_HOME/hadoop-0.20.0-core.jar:$HADOOP_HOME/hadoop-0.20.0-tools.jar:$HOME/lib/java/commons-logging.jar:$HOME/lib/java/commons-cli.jar:$HOME/lib/java/gnu-getopt.jar:$HBASE_CONF:$ZOOKEEPER_HOME/zookeeper-3.2.1.jar:$HOME/lib/java/log4j-1.2.jar:$HADOOP_CONF edu.princeton.function.troilkatt.clients.GetSinkFiles geo-gsd-info %s -c $TROILKATT_HOME/conf/troilkatt.xml' % (tmpDir)
    print 'Execute: %s' % (downloadInfoCmd)
    os.system(downloadInfoCmd)
    infoFile = os.path.join(outputDir, '%s.info' % (organism))
    print 'Save .info file as: %s' % (infoFile)
    shutil.move(os.path.join(tmpDir, 'geo-gsd-info/spell.info'), infoFile)
    
    #
    # 3. Parse the .info file to create a list of pubmed references
    #
    pubmedFile = os.path.join(outputDir, '%s.pubmed' % (organism))
    print 'Create pubmed file: %s' % (pubmedFile)
    parsePubmed(fileList, infoFile, pubmedFile)
    
    
    #
    # 4. Copy the gene name mapping lists, and create a gene name list
    #
    geneListFile = os.path.join(outputDir, '%s.genes' % (organism))
    print 'Create gene-list: %s' % (geneListFile)
    createGeneListFile(geneNameFile, geneListFile)
    
    print 'Done: output is saved in: %s' % (outputDir)
    