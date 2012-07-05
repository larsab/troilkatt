import shutil
import os.path

from ftplib import FTP

from troilkatt_crawler import TroilkattCrawler



"""
Maintain a mirror of all by_series files in geo
"""
class GeoMirror(TroilkattCrawler):
    """
    arguments: 
     [0]: directory for storing unknown files. 
        
    @param: see description for super-class
    """
    def __init__(self):
        TroilkattCrawler.__init__(self)
        
        # E-mail address is often used as password or field in queries against NIH servers
        self.adminEmail = 'lbongo@princeton.edu'
        
        # These are set in crawl()
        self.gselist = None                
        
        self.ftp = FTP('ftp.ncbi.nih.gov')        
        self.softDir = '/pub/geo/DATA/SOFT/by_series'
        #self.softDir = '/sdc/troilkatt/crawler/geo-yeast/download/SOFT/by_series'
        #self.softDir = '/nhome/larsab/troilkatt/geo-yeast/SOFT/by_series'        
                
        # Directory for unknown (unpacked) files
        self.unknownDir = self.args
        if not os.path.isdir(self.unknownDir):
            self.logger.debug('Create unknown directory: %s' % (self.unknownDir))
            os.makedirs(self.unknownDir)
        else:
            self.logger.debug('Unknown directory is: %s' % (self.unknownDir))
                
        self.logger.debug('geo-mirror module initialized')
        
    
    """
    Query geo and retrieve new and updated files.
    
    @param inputObjects: dictionary with objects indexed by a script specific key (can be None).
    
    @return: dictionary with objects that should be saved in Hbase using the given keys (can be None).
    """
    def crawl(self, inputObjects):
                
        if 'gselist' in inputObjects:            
            self.gselist = inputObjects['gselist']
        else:
            self.gselist = []
        #print 'DEBUG: Previously mirrored filelist is not downloaded' 
        
        if 0:
            print 'DEBUG: Read GSE list from file geo_mirror_gselist.txt'
            fp = open('/tmp/geo_mirror_gselist.txt')
            lines = fp.readlines()
            fp.close()
            self.gselist = []
            for l in lines:
                l = l.strip()
                if l not in self.gselist:
                    self.gselist.append(l)
            self.gselist.sort()
        self.saveDirsFile(self.gselist, "gselist")
                
        self.ftp.login('anonymous', self.adminEmail)
        
        subDirs = self.getSubdirs()        
        newDirs = self.getNewdirs(subDirs)                
        
        downloadedDirs = []
        for d in newDirs:
            if self.downloadFiles(d):
                downloadedDirs.append(d)
                try:
                    self.unpackAll(self.downloadDir)
                    self.move2Output()
                    self.deleteCompressed(self.downloadDir)
                    self.move2Log()
                    self.moveAll(self.downloadDir, self.unknownDir)
                except Exception, e:
                    self.deleteAll(self.downloadDir)
                    self.logger.warning("Could not unpack files downloaded from: %s" % (d))
                    self.logger.warning(e)
                    downloadedDirs.remove(d)
                        
        self.ftp.quit()
        
        self.saveDirsFile(downloadedDirs, 'downloaded')
        
        # Update downloaded subdirs list
        for gse in downloadedDirs:
            if gse not in self.gselist:
                self.gselist.append(gse)            
            
        # outputObjects
        return {'gselist': self.gselist}
    
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
        # Get file list
        self.logger.info('Retrieve directory list from FTP server (this may take some time)')
        subdirs = self.ftp.nlst(self.softDir)
        #subdirs = os.listdir(self.softDir)
        subdirs.sort()
        self.saveDirsFile(subdirs, 'all')
        
        return subdirs
    
    """
    Compare the current subdirs with the previously downloaded directories to create a list of 
    files to download
    
    @param downloadedList: subdirectory list retrieved from the FTP server
    
    @return list of subdirectories in downloadedList that are not in self.gselist    
    """
    def getNewdirs(self, downloadedList):
        newList = []
        for s in downloadedList:
            if s not in self.gselist:
                newList.append(s)
                
        self.saveDirsFile(newList, 'new')
        
        return newList
    
    """
    Download all files in a subdirectory. The files are saved in the downloadDirectory
    
    @param subdir: by_series/ subdirectory.
    
    @return true on success, false on failure
    """
    def downloadFiles(self, subdir):
        remotePath = subdir
        
        downloaded = []
        try:
            self.ftp.cwd(remotePath)          
            
            self.logger.debug('Downlaod all files in: %s' % (remotePath))
            
            files = self.ftp.nlst('.')
            #files = os.listdir('remotePath')
            for f in files:
                self.logger.debug('FTP retrieve: %s' % (f))
                dstName = os.path.join(self.downloadDir, f)
                self.ftp.retrbinary('RETR %s' % (f), open(dstName, 'wb').write, 32*1024*1024)
                #shutil.copy(os.path.join(remotePath, f), dstName)
                downloaded.append(f)
        except Exception, e:
            self.logger.warning("Could not download files in: %s" % (remotePath))
            self.logger.warning(e)
            
            # Delete all downloaded files
            for d in downloaded:
                os.remove(d)
            
            return False                    
            
        return True

    """
    Move all SOFT files from download to output directory.      
    """
    def move2Output(self):                        
        files = os.listdir(self.downloadDir)
            
        for f in files:
            if self.endsWith(f, '.soft'):
                src = os.path.join(self.downloadDir, f)
                dst = os.path.join(self.outputDir, f)                    
                shutil.move(src, dst)
                
    """
    Move all log files from download to log directory. 
    """
    def move2Log(self):
        # Do nothing
        pass
    
    """
    Move all remaining files to a directory
    
    @param srcDir
    @param dstDir
    """
    def moveAll(self, srcDir, dstDir):
        files = os.listdir(srcDir)
        for f in files:
            src = os.path.join(srcDir, f)
            dst = os.path.join(dstDir, f)
            self.logger.info('Move unknown file: %s to %s' % (src, dst))   
            shutil.move(src, dst)
            
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
    c = GeoMirror()
    c.mainRun()
    print 'Done'