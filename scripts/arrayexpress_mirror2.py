'''
Array express mirror that does not uncompress the downloaded files.
'''

import shutil
import os.path

from ftplib import FTP

from troilkatt_crawler import TroilkattCrawler

"""
Maintain a mirror of all by_series files in geo
"""
class ArrayExpressMirror2(TroilkattCrawler):
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
            
            if not self.downloadFiles(d): # the files could not be downloaded
                errors.append(eid)
                continue
             
            if eid not in experimentIDs:
                experimentIDs.append(eid)
                
            downloaded.append(eid)            
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
            self.logger.info('Retrieve epxeriemtn directory list from FTP server: ' + os.path.basename(p))
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
            self.logger.debug('Downlaod all files in: %s' % (remotePath))
            files = self.ftp.nlst(remotePath)
            for f in files:
                self.logger.debug('FTP retrieve: %s' % (f))
                basename = os.path.basename(f)
                dstName = os.path.join(self.downloadDir, basename)
                self.ftp.retrbinary('RETR %s' % (f), open(dstName, 'wb').write, 32*1024*1024)
                downloaded.append(dstName)                       
        except Exception, e:
            self.logger.warning("Could not downaload files in: " + subdir)
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
    c = ArrayExpressMirror2()
    c.mainRun()
    print 'Done'