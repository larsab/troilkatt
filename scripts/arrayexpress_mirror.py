import shutil
import os.path

from ftplib import FTP

from troilkatt_crawler import TroilkattCrawler

"""
Maintain a mirror of all by_series files in geo
"""
class ArrayExpressMirror(TroilkattCrawler):
    """
    arguments: 
     [0]: directory for storing unknown files. 
        
    @param: see description for super-class
    """
    def __init__(self):
        TroilkattCrawler.__init__(self)
        
        # E-mail address is often used as password or field in queries against NIH servers
        self.adminEmail = 'lbongo@princeton.edu'
                                
        self.ftp = FTP('ftp.ebi.ac.uk')        
        self.experimentDir = '/pub/databases/microarray/data/experiment/'     
                
        # Directory for unknown (unpacked) files
        self.unknownDir = self.args
        if not os.path.isdir(self.unknownDir):
            self.logger.debug('Create unknown directory: %s' % (self.unknownDir))
            os.makedirs(self.unknownDir)
                
        self.logger.debug('arrayexpress-mirror module initialized')
        
    
    """
    Query geo and retrieve new and updated files.
    
    @param inputObjects: dictionary with objects indexed by a script specific key (can be None).
    
    @return: dictionary with objects that should be saved in Hbase using the given keys (can be None).
    """
    def crawl(self, inputObjects):        
                
        #print 'WARNING: input objects not downloaded from server'
        #inputObjects = {}
        
        if 'experimentIDs' in inputObjects:            
            experimentIDs = inputObjects['experimentIDs']
        else:
            experimentIDs = []        
            
        #experimentIDs = self.cleanExperimentIDs(experimentIDs)
        experimentIDs = self.loadExperimentIDs()        
        
        if 'experimentFiles' in inputObjects:
            experimentFiles = inputObjects['experimentFiles']
        else:
            experimentFiles = {}            
        
        self.saveDirsFile(experimentIDs, 'previous')    
        self.file2ExperimentID = None
                
        self.ftp.login('anonymous', self.adminEmail)
        
        subDirs = self.getSubdirs()        
        newDirs = self.getNewdirs(subDirs, experimentIDs)        
        
        downloaded = [] # list of downloaded experiment id's        
        errors = []
        for d in newDirs:
            eid = self.getExperimentID(d)

            if self.outputContains(eid):
                downloaded.append(eid)
                self.logger.info('Experiment already downloaded: %s' % (eid))
                continue
            
            self.deleteAll(self.downloadDir)
            if not self.downloadFiles(d): # the files could not be downloaded
                errors.append(eid)
                continue
             
            if eid not in experimentIDs:
                experimentIDs.append(eid)
                
            downloaded.append(eid)            
            if self.unpackAll(self.downloadDir) == None: # Failed
                self.deleteAll(self.downloadDir)
            else:            
                self.deleteCompressed(self.downloadDir)
                self.verifyFilenames(self.downloadDir)
                experimentFiles[eid] = self.move2Output()                                             

        try:
            self.ftp.quit()
        except Exception, e:
            self.logger.warning('FTP quit failed: %s' % (e))            
        
        self.saveDirsFile(downloaded, 'downloaded')
        self.saveDirsFile(errors, 'errors')
        
        # outputObjects
        return {'experimentIDs': experimentIDs,
                'experimentFiles': experimentFiles}
    
    """
    Save a list of subdirectories in a file (one directory per line)
    
    @param list: list of directories
    @param filename:
    """
    def saveDirsFile(self, list, filename):
        self.logger.debug('Save filelist in %s' % (filename))
        fp = open(os.path.join(self.logDir, filename), 'w')
                
        for f in list:
            fp.write('%s\n' % (f))
        fp.close()
    
    """
    Get list of all subdirectories in the SOFT/by_series NCBI Geo FTP server.
    
    @return list with by_series/ subdirectories
    """
    def getSubdirs(self):        
        #
        # Array express stores data in a two-level hierarchy. The first level is platform type
        # and the second has subdirectories for each experiment
        #
        subdirs = []
        
        # Get list of platforms
        self.logger.info('Retrieve experiment directory list from FTP server')
        platformDirs = self.ftp.nlst(self.experimentDir)
        # Note that the platformDirs are absolute pathnames
        
        for p in platformDirs:
            # Get file list
            self.logger.info('Retrieve epxeriment directory list from FTP server: ' + os.path.basename(p))
            subdirs = subdirs + self.ftp.nlst(p)                    
        
        subdirs.sort()        
        self.saveDirsFile(subdirs, 'all')
        
        return subdirs
    
    """
    Compare the current subdirs with the previously downloaded directories to create a list of 
    files to download
    
    @param downloadedList: subdirectory list retrieved from the FTP server
    
    @return list of subdirectories in downloadedList that are not in self.experimentIDs    
    """
    def getNewdirs(self, downloadedList, experimentIDs):
        newList = []
        for s in downloadedList:
            eid = self.getExperimentID(s)
            if eid == None:
                continue
            
            if eid not in experimentIDs:
                newList.append(s)
                
        self.saveDirsFile(newList, 'new')
        
        return newList[0:10]
    
    """
    Download all files in a subdirectory. The files are saved in the downloadDirectory
    
    @param subdir: path to a ArrayExpress experiment directory
    
    @return true on success, false on failure
    """
    def downloadFiles(self, subdir):
        remotePath = subdir
        
        downloaded = []
        try:  
            self.logger.debug('Download all files in: %s' % (remotePath))
            files = self.ftp.nlst(remotePath)
            for f in files:
                self.logger.debug('FTP retrieve: %s' % (f))
                basename = os.path.basename(f)
                dstName = os.path.join(self.downloadDir, basename)
                self.ftp.retrbinary('RETR %s' % (f), open(dstName, 'wb').write, 32*1024*1024)
                downloaded.append(dstName)                       
        except Exception, e:
            self.logger.warning("Could not download files in: %s" % (subdir))
            self.logger.warning(e)
            
            # Delete all downloaded files
            for d in downloaded:
                os.remove(d)
            
            return False                    
            
        return True

    """
    Move all files from download to output directory.
    
    @return: a list of files moved      
    """
    def move2Output(self):                        
        files = os.listdir(self.downloadDir)
        movedFiles = []
                
        for f in files:
            # Move all files to output            
            src = os.path.join(self.downloadDir, f)            
            dst = os.path.join(self.outputDir, f)                    
            shutil.move(src, dst)
            movedFiles.append(f)
            
        return movedFiles
            
    """
    Verify that filenames are valid HDFS filenames
    """
    def verifyFilenames(self, dir):
        files = os.listdir(dir)
        
        for f in files:
            absolutePath = os.path.join(dir, f)
            if os.path.isdir(absolutePath):
                self.verifyFilenames(absolutePath)
            
            if f.find(':') != -1:
                dst = absolutePath.replace(':', '')
                shutil.move(absolutePath, dst)
    
            
    """
    @return the experiment ID for an experiment subdir, or None if it is an invalid path
    """
    def getExperimentID(self, path):                
        expdir = os.path.basename(path)
        if (expdir.find('.') != -1):
            self.logger.warning('Incorrect filename: ' + path)
            return None
        else:
            return expdir # The experiment directory is the identificator
        
    """
    Unpack all files in a directory.
    
    @param dir: directory with files to unpack
    @param createSubdir: True if mageml and raw files should be moved to a subdirectory before being 
     unpacked. 
    
    @return: list of unpacket files on success, None on failure
    """
    def unpackAll(self, dir, createSubdir=True):
        self.logger.debug('Unpack all files in %s' % (dir))
        
        oldCwd = os.getcwd()
        try:
            os.chdir(dir)
        except Exception, e:
            self.logger.info('Could not change to %s' % (dir))
            self.logger.info('Excpetion: %s' % (e))
            return None
                
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
                
                if os.path.isdir(f):
                    self.unpackAll(os.path.join(dir, f), False)                    
                
                try:
                    if f.find('.raw') != -1 and createSubdir:
                        id = f.split('.')[0]
                        rawDir = os.path.join(dir, id + '.raw')                                        
                        if os.path.isdir(rawDir):
                            self.logger.warning("Ignoring file with same prefix already unpacked: " + f)
                        else:
                            os.mkdir(rawDir)
                            unpacked.append(id + '.raw')
                        shutil.move(f, os.path.join(rawDir, f))
                        self.unpackAll(rawDir, False)
                        continue
                    elif f.find('.mageml') != -1 and createSubdir:
                        id = f.split('.')[0]
                        rawDir = os.path.join(dir, id + '.mageml')
                        if os.path.isdir(rawDir):
                            self.logger.warning("Ignoring file with same prefix already unpacked: " + f)
                        else:
                            os.mkdir(rawDir)
                            unpacked.append(id + '.mageml')
                        shutil.move(f, os.path.join(rawDir, f))
                        self.unpackAll(rawDir, False)
                        continue
                    elif (f.find('.cel') != -1 or f.find('.CEL') != -1) and createSubdir:
                        id = f.split('.')[0]
                        rawDir = os.path.join(dir, id + '.cel')
                        if os.path.isdir(rawDir):
                            self.logger.warning("Ignoring file with same prefix already unpacked: " + f)
                        else:
                            os.mkdir(rawDir)
                            unpacked.append(id + '.cel')
                        shutil.move(f, os.path.join(rawDir, f))
                        self.unpackAll(rawDir, False)
                        continue
                    elif f.find('.processed') != -1 and createSubdir:
                        id = f.split('.')[0]
                        rawDir = os.path.join(dir, id + '.processed')
                        if os.path.isdir(rawDir):
                            self.logger.warning("Ignoring file with same prefix already unpacked: " + f)
                        else:
                            os.mkdir(rawDir)
                            unpacked.append(id + '.processed')
                        shutil.move(f, os.path.join(rawDir, f))
                        self.unpackAll(rawDir, False)
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
                except Exception, e:
                    self.logger.error('Could not unpack files in: %s' % (dir))
                    self.logger.error('Exception: %s' % (e))                    
                    return None
                    
            
        os.chdir(oldCwd)
        return unpacked
    
    """
    Check if output directory contains EID
    """
    def outputContains(self, eid):
        files = os.listdir(self.outputDir)
        idfFilename = eid + ".idf.txt"                        
        readmeFilename = eid + ".README.txt"
                
        for f in files:
            if f == idfFilename:
                return True
            elif f == readmeFilename:
                return True
            
        return False
    
    def cleanExperimentIDs(self, eids):
        f = open('/tmp/downloaded', 'r')
        lines = f.readlines()
        f.close()
        
        for l in lines:
            l = l.strip()
            eids.remove(l)
        
        return eids
    
    def loadExperimentIDs(self):
        f = open('/tmp/downloaded', 'r')
        lines = f.readlines()
        f.close()
        
        eids = []
        for l in lines:
            l = l.strip()
            eids.append(l)
        
        return eids
        
            
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
    c = ArrayExpressMirror()
    c.mainRun()
    print 'Done'