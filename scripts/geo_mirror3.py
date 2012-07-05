import os.path

from ftplib import FTP

from geo_mirror import GeoMirror

"""
Maintain a mirror of all supplementary files in geo
"""
class GeoMirror3(GeoMirror):
    """
    @param: see description for super-class
    """
    def __init__(self):
        GeoMirror.__init__(self)               
        
        self.ftp = FTP('ftp.ncbi.nih.gov')        
        #self.softDir = '/sdc/troilkatt/input/spell'
        self.softDir = '/pub/geo/DATA/supplementary/series'        
                
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
        #self.gselist = [] 
        
        self.ftp.login('anonymous', self.adminEmail)
        
        allDirs = self.getAllRemoteDirs()        
        newDirs = self.getNewDirs(allDirs)        
        
        downloadedDirs = []
        for d in newDirs:
            if self.downloadFiles(d):
                downloadedDirs.append(d)                 
        self.ftp.quit()
        
        self.saveDirsFile(downloadedDirs, 'downloaded')
        
        # Update downloaded subdirs list
        for gse in downloadedDirs:
            if gse not in self.gselist:
                self.gselist.append(gse)            
            
        # outputObjects
        return {'gselist': self.gselist}
    
    """
    Get list of all subdirectories in the SOFT/by_series NCBI Geo FTP server.
    
    @return list with by_series/ subdirectories
    """
    def getAllRemoteDirs(self):        
        # Get file list
        self.logger.info('Retrieve directory list from FTP server (this may take some time)')
        files = self.ftp.nlst(self.softDir)
        #files = os.listdir(self.softDir)
        
        self.saveDirsFile(files, 'all')
        
        return files
    
    """
    Compare the current subdirs with the previously downloaded directories to create a list of 
    files to download
    
    @param downloadedList: subdirectory list retrieved from the FTP server
    
    @return list of subdirectories in downloadedList that are not in self.gselist    
    """
    def getNewDirs(self, downloadedList):
        newList = []
        for s in downloadedList:
            if s not in self.gselist:
                newList.append(s)
                
        self.saveDirsFile(newList, 'new')
        
        return newList
    
    """
    Download all files in a subdirectory. The files are saved in the downloadDirectory
    
    @param filename: GSD file to download
    
    @return true on success, false on failure
    """
    def downloadFiles(self, subdir):
        remotePath = subdir
        localPath = os.path.join(self.outputDir, os.path.basename(subdir))
        if not os.path.isdir(localPath):
            self.logger.debug('Create output subdirectory: %s' % (localPath))
            os.makedirs(localPath)  
        
        downloaded = []
        try:
            self.ftp.cwd(remotePath)          
            
            self.logger.debug('Downlaod all files in: %s into: %s' % (remotePath, localPath))
            
            files = self.ftp.nlst('.')
            #files = os.listdir('remotePath')
            for f in files:
                if f != 'filelist.txt':
                    continue
                
                self.logger.debug('FTP retrieve: %s' % (f))
                dstName = os.path.join(localPath, f)
                self.ftp.retrbinary('RETR %s' % (f), open(dstName, 'wb').write, 32*1024*1024)
                #shutil.copy(os.path.join(remotePath, f), dstName)
                downloaded.append(f)
        except Exception, e:
            self.logger.warning("Could not download files in: %s" % (remotePath))
            self.logger.warning(e)
            return False                    
            
        return True
    
            
"""
Run a troilkatt crawler

Command line arguments: %s <options> download output log <args>, where the required arguments are:
    1. -i <object input directory>
    2. -o <object output directory>
    3. download directory
    4. output directory
    5. logfile directory
    6. unknown file directory
        
The decription in parseArgs() has additional details. 
"""
if __name__ == '__main__':
    c = GeoMirror3()
    c.mainRun()
    print 'Done'