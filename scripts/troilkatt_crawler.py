"""
Superclass for troilkatt crawler scripts. It provides the parsing of the troilkatt configuration file,
parsing of arguments, and a run function that can be used to start the crawler.
"""

import os.path
from troilkatt_script import TroilkattScript

class TroilkattCrawler(TroilkattScript):
    """
    arguments: file with gene-names
        
    @param: see description for super-class
    """
    def __init__(self):
        TroilkattScript.__init__(self)
        self.downloadDir = self.inputDir

    """
    Run a Python crawler
    """
    def mainRun(self):                      
        import pickle
           
        #
        # 1. Read in input objects
        #        
        self.logger.debug('Read input objects')
        inputObjects = {}
        if self.objectInputDir != None:
            objectFilenames = self.getAllFiles(self.objectInputDir, False)
            for f in objectFilenames:    
                self.logger.info('Read object: %s' % (f))
                fp = open(os.path.join(self.objectInputDir, f), 'r')
                inputObjects[f] = pickle.load(fp)
                fp.close()
            
        #
        # 2. Do the crawl
        #
        self.logger.debug('Do the crawl')
        outputObjects = self.crawl(inputObjects)

        #
        # 3. Save output objects
        #
        self.logger.debug('Save output objects')
        if outputObjects != None:
            for k in outputObjects:
                self.logger.info('Write object: %s' % (k))
                f = open(os.path.join(self.objectOutputDir, k), 'w')
                pickle.dump(outputObjects[k], f)
                f.close()
                
        self.logger.debug('Done')
        print 'Done'
                
    """
    Script specific crawl function. A sub-class should implement this function.
    
    @param inputObjects: dictionary with objects indexed by a script specific key (can be None).
    
    @return: dictionary with objects that should be saved in Hbase using the given keys (can be None).
    """
    def crawl(self, inputObjects):
        outputObjects = None
        raise Exception("Subclass should implement the crawl() function")
        return outputObjects
    
    """
    Unpack all files in a directory.
    
    @param dir: directory with files to unpack
    
    @return: none
    """
    def unpackAll(self, dir):
        self.logger.debug('Unpack all files in %s' % (dir))
        
        oldCwd = os.getcwd()
        try:
            os.chdir(dir)
        except Exception, e:
            self.logger.info('Could not change to %s (probably no geo record for this ID)' % (dir))
            self.logger.info('Excpetion: %s' % (e))
            return
                
        # Loop since some tarballs contain compressed files
        allUnpacked = 0
        unpacked = []
        while not allUnpacked:
            allUnpacked = 1
            
            files = os.listdir('.')
            for f in files:
                if f in unpacked:
                    # Ignore files that have already been unpacked
                    continue
                
                if self.endsWith(f, '.tar.gz') or self.endsWith(f, '.tgz'):
                    cmd = 'tar xvzf %s > %s 2> %s' % (f, 
                                                      os.path.join(self.logDir, 'untar.%s.output' % (f)),
                                                      os.path.join(self.logDir, 'untar.%s.error' % (f)))
                    if os.system(cmd) != 0:
                        self.logger.error('Command failed: %s' % (cmd))
                    allUnpacked = 0
                    unpacked.append(f)
                elif self.endsWith(f, '.tar'):
                    cmd = 'tar xvf %s > %s 2> %s' % (f, 
                                                     os.path.join(self.logDir, 'untar.%s.output' % (f)),
                                                     os.path.join(self.logDir, 'untar.%s.error' % (f)))
                    if os.system(cmd) != 0:
                        self.logger.error('Command failed: %s' % (cmd))                    
                    allUnpacked = 0
                    unpacked.append(f)
                elif self.endsWith(f, '.gz') or self.endsWith(f, '.Z'):
                    # keep file
                    cmd = 'gunzip -cf %s > %s 2> %s' % (f, f[:-3],
                                                        os.path.join(self.logDir, 'gunzip.%s.error' % (f)))
                    if os.system(cmd) != 0:
                        self.logger.error('Command failed: %s' % (cmd))
                    allUnpacked = 0
                    unpacked.append(f)
                elif self.endsWith(f, '.zip'):
                    # keep file
                    cmd = 'unzip -o %s > %s 2> %s' % (f,
                                                      os.path.join(self.logDir, 'unzip.%s.output' % (f)),
                                                      os.path.join(self.logDir, 'unzip.%s.error' % (f)))
                    if os.system(cmd) != 0:
                        self.logger.error('Command failed: %s' % (cmd))
                    allUnpacked = 0
                    unpacked.append(f)
                
        
        os.chdir(oldCwd)
        return unpacked
    
    """
    Delete all compressed files in directory
    
    @dir: directory with files to delete
    """
    def deleteCompressed(self, dir):
        files = os.listdir(dir)
        
        for f in files:
            fullName = os.path.join(dir, f)
            if os.path.isdir(fullName):
                self.deleteCompressed(fullName)
            if self.endsWith(f, '.tar.gz') or self.endsWith(f, '.tgz') or self.endsWith(f, '.tar') or self.endsWith(f, '.gz') or self.endsWith(f, '.Z') or self.endsWith(f, '.zip'):
                os.remove(os.path.join(dir, f))
                
    """
    Delete all files in directory
    
    @dir: directory with files to delete
    """
    def deleteAll(self, dir):
        files = os.listdir(dir)
        
        for f in files:
            fullName = os.path.join(dir, f)
            if os.path.isdir(fullName):
                self.deleteAll(fullName)
                os.rmdir(fullName)
            else:            
                os.remove(os.path.join(dir, f))
        
    
"""
Run a troilkatt crawler

Command line arguments: %s <options> download output log <args>, where
    <options>: -i object input directory, and -o object output directory
    download: download directory
    output: output directory
    log: logfile directory
    <args>: optional list of stage specific arguments
        
The decription in parseArgs() has additional details. 
"""
if __name__ == '__main__':
    c = TroilkattCrawler()
    c.mainRun()